/** Bounded read-only discovery of deterministic Orca Whirlpool PDAs. */

import { createRequire } from "node:module";
import readline from "node:readline";

import { Connection, PublicKey, type AccountInfo } from "@solana/web3.js";
import type { WhirlpoolData } from "@orca-so/whirlpools-sdk";

import { closeProtocolOutput, emit } from "./protocol.js";

interface AssetInput {
  symbol: string;
  mint: string;
  decimals: number;
  cex_symbol: string;
}

interface PairInput {
  base: AssetInput;
  bridge: AssetInput;
}

interface DiscoveryInput {
  rpc_http_url: string;
  pairs: PairInput[];
  maximum_pools: number;
}

interface OrcaRuntime {
  ORCA_WHIRLPOOL_PROGRAM_ID: PublicKey;
  ORCA_WHIRLPOOLS_CONFIG: PublicKey;
  ORCA_SUPPORTED_TICK_SPACINGS: number[];
  PDAUtil: {
    getWhirlpool(
      programId: PublicKey,
      config: PublicKey,
      tokenA: PublicKey,
      tokenB: PublicKey,
      tickSpacing: number,
    ): { publicKey: PublicKey };
  };
  PoolUtil: {
    orderMints(a: PublicKey, b: PublicKey): [PublicKey, PublicKey];
    isInitializedWithAdaptiveFee(data: WhirlpoolData): boolean;
  };
  ParsableWhirlpool: {
    parse(address: PublicKey, account: AccountInfo<Buffer> | null): WhirlpoolData | null;
  };
}

const require = createRequire(import.meta.url);
const Orca = require("@orca-so/whirlpools-sdk") as OrcaRuntime;

function requiredString(value: unknown, field: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`${field} must be a non-empty string`);
  }
  return value.trim();
}

function parseAsset(value: unknown, field: string): AssetInput {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${field} must be an object`);
  }
  const item = value as Record<string, unknown>;
  const decimals = item.decimals;
  if (!Number.isInteger(decimals) || (decimals as number) < 0 || (decimals as number) > 18) {
    throw new Error(`${field}.decimals must be an integer in [0, 18]`);
  }
  return {
    symbol: requiredString(item.symbol, `${field}.symbol`).toUpperCase(),
    mint: new PublicKey(requiredString(item.mint, `${field}.mint`)).toBase58(),
    decimals: decimals as number,
    cex_symbol: requiredString(item.cex_symbol, `${field}.cex_symbol`).toUpperCase(),
  };
}

function parseInput(value: unknown): DiscoveryInput {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("discovery input must be an object");
  }
  const item = value as Record<string, unknown>;
  if (!Array.isArray(item.pairs) || item.pairs.length === 0 || item.pairs.length > 32) {
    throw new Error("pairs must contain 1..32 entries");
  }
  const maximumPools = item.maximum_pools;
  if (!Number.isInteger(maximumPools) || (maximumPools as number) <= 0 || (maximumPools as number) > 32) {
    throw new Error("maximum_pools must be an integer in [1, 32]");
  }
  return {
    rpc_http_url: requiredString(item.rpc_http_url, "rpc_http_url"),
    pairs: item.pairs.map((value, index) => {
      if (typeof value !== "object" || value === null || Array.isArray(value)) {
        throw new Error(`pairs[${index}] must be an object`);
      }
      const pair = value as Record<string, unknown>;
      const base = parseAsset(pair.base, `pairs[${index}].base`);
      const bridge = parseAsset(pair.bridge, `pairs[${index}].bridge`);
      if (base.mint === bridge.mint) throw new Error(`pairs[${index}] repeats one mint`);
      return { base, bridge };
    }),
    maximum_pools: maximumPools as number,
  };
}

async function discover(input: DiscoveryInput): Promise<Record<string, unknown>> {
  const connection = new Connection(input.rpc_http_url, "processed");
  const candidates = new Map<string, { pair: PairInput; tickSpacing: number; address: PublicKey }>();
  for (const pair of input.pairs) {
    const [tokenA, tokenB] = Orca.PoolUtil.orderMints(
      new PublicKey(pair.base.mint),
      new PublicKey(pair.bridge.mint),
    );
    for (const tickSpacing of Orca.ORCA_SUPPORTED_TICK_SPACINGS) {
      const address = Orca.PDAUtil.getWhirlpool(
        Orca.ORCA_WHIRLPOOL_PROGRAM_ID,
        Orca.ORCA_WHIRLPOOLS_CONFIG,
        tokenA,
        tokenB,
        tickSpacing,
      ).publicKey;
      candidates.set(address.toBase58(), { pair, tickSpacing, address });
    }
  }

  const entries = [...candidates.values()];
  const found: Array<{ pair: PairInput; address: PublicKey; data: WhirlpoolData }> = [];
  for (let offset = 0; offset < entries.length; offset += 100) {
    const batch = entries.slice(offset, offset + 100);
    const response = await connection.getMultipleAccountsInfoAndContext(
      batch.map((item) => item.address),
      "processed",
    );
    for (let index = 0; index < batch.length; index += 1) {
      const account = response.value[index];
      if (account === null) continue;
      const item = batch[index];
      const data = Orca.ParsableWhirlpool.parse(item.address, account);
      if (data === null || data.liquidity.isZero()) continue;
      found.push({ pair: item.pair, address: item.address, data });
    }
  }

  const selected: typeof found = [];
  for (const pair of input.pairs) {
    const forPair = found
      .filter((item) => {
        const mints = new Set([item.data.tokenMintA.toBase58(), item.data.tokenMintB.toBase58()]);
        return mints.has(pair.base.mint) && mints.has(pair.bridge.mint);
      })
      .filter((item) => !Orca.PoolUtil.isInitializedWithAdaptiveFee(item.data))
      .sort((left, right) => right.data.liquidity.cmp(left.data.liquidity));
    if (forPair.length > 0) selected.push(forPair[0]);
    if (selected.length >= input.maximum_pools) break;
  }

  return {
    type: "orca_discovery_result",
    candidate_addresses: entries.length,
    found_accounts: found.length,
    skipped_adaptive_fee_accounts: found.filter((item) =>
      Orca.PoolUtil.isInitializedWithAdaptiveFee(item.data)).length,
    pools: selected.map((item) => ({
      pool_id: item.address.toBase58(),
      label: `${item.pair.base.symbol}/${item.pair.bridge.symbol} Orca Whirlpool`,
      base: item.pair.base,
      bridge: item.pair.bridge,
      token_a_mint: item.data.tokenMintA.toBase58(),
      token_b_mint: item.data.tokenMintB.toBase58(),
      tick_spacing: item.data.tickSpacing,
      fee_rate_millionths: item.data.feeRate,
      protocol_fee_rate: item.data.protocolFeeRate,
      liquidity_raw: item.data.liquidity.toString(10),
      adaptive_fee_enabled: false,
    })),
    wallet_or_private_key_used: false,
    transactions_submitted: false,
  };
}

const lines = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
try {
  for await (const line of lines) {
    if (line.trim().length === 0) continue;
    const result = await discover(parseInput(JSON.parse(line)));
    emit(result);
    await closeProtocolOutput();
    break;
  }
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  emit({ type: "orca_discovery_error", error: message.slice(0, 512) });
  await closeProtocolOutput().catch(() => undefined);
  process.exitCode = 1;
}

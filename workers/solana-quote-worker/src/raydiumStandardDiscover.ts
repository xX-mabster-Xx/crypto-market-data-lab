/** Bounded read-only identification of Raydium standard-pool account layouts. */

import readline from "node:readline";

import { Connection, PublicKey } from "@solana/web3.js";
import {
  AMM_V4,
  CREATE_CPMM_POOL_PROGRAM,
  CpmmPoolInfoLayout,
  liquidityStateV4Layout,
} from "@raydium-io/raydium-sdk-v2";

interface PoolInput {
  pool_id: string;
  label: string;
}

interface DiscoveryInput {
  rpc_http_url: string;
  pools: PoolInput[];
}

function requiredString(value: unknown, field: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`${field} must be a non-empty string`);
  }
  return value.trim();
}

function parseInput(value: unknown): DiscoveryInput {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("discovery input must be an object");
  }
  const item = value as Record<string, unknown>;
  if (!Array.isArray(item.pools) || item.pools.length === 0 || item.pools.length > 64) {
    throw new Error("pools must contain 1..64 entries");
  }
  const pools = item.pools.map((entry, index) => {
    if (typeof entry !== "object" || entry === null || Array.isArray(entry)) {
      throw new Error(`pools[${index}] must be an object`);
    }
    const pool = entry as Record<string, unknown>;
    return {
      pool_id: new PublicKey(requiredString(pool.pool_id, `pools[${index}].pool_id`)).toBase58(),
      label: requiredString(pool.label, `pools[${index}].label`),
    };
  });
  if (new Set(pools.map((pool) => pool.pool_id)).size !== pools.length) {
    throw new Error("pool IDs must be unique");
  }
  return {
    rpc_http_url: requiredString(item.rpc_http_url, "rpc_http_url"),
    pools,
  };
}

async function discover(input: DiscoveryInput): Promise<Record<string, unknown>> {
  const connection = new Connection(input.rpc_http_url, "processed");
  const response = await connection.getMultipleAccountsInfoAndContext(
    input.pools.map((pool) => new PublicKey(pool.pool_id)),
    "processed",
  );
  const pools = input.pools.map((descriptor, index) => {
    const account = response.value[index];
    if (account === null) {
      return { ...descriptor, status: "missing", protocol: "unknown" };
    }
    const owner = account.owner.toBase58();
    if (account.owner.equals(CREATE_CPMM_POOL_PROGRAM)) {
      const state = CpmmPoolInfoLayout.decode(account.data);
      return {
        ...descriptor,
        status: "ok",
        protocol: "raydium_cpmm",
        owner,
        token_a_mint: state.mintA.toBase58(),
        token_b_mint: state.mintB.toBase58(),
        token_a_decimals: state.mintDecimalA,
        token_b_decimals: state.mintDecimalB,
        vault_a: state.vaultA.toBase58(),
        vault_b: state.vaultB.toBase58(),
        config_id: state.configId.toBase58(),
      };
    }
    if (account.owner.equals(AMM_V4)) {
      const state = liquidityStateV4Layout.decode(account.data);
      return {
        ...descriptor,
        status: "ok",
        protocol: "raydium_amm_v4",
        owner,
        token_a_mint: state.baseMint.toBase58(),
        token_b_mint: state.quoteMint.toBase58(),
        token_a_decimals: state.baseDecimal.toNumber(),
        token_b_decimals: state.quoteDecimal.toNumber(),
        vault_a: state.baseVault.toBase58(),
        vault_b: state.quoteVault.toBase58(),
      };
    }
    return { ...descriptor, status: "unsupported_owner", protocol: "unknown", owner };
  });
  return {
    type: "raydium_standard_discovery_result",
    slot: response.context.slot,
    pools,
    wallet_or_private_key_used: false,
    transactions_submitted: false,
  };
}

const lines = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
try {
  for await (const line of lines) {
    if (line.trim().length === 0) continue;
    const result = await discover(parseInput(JSON.parse(line)));
    process.stdout.write(`${JSON.stringify(result)}\n`);
    break;
  }
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  process.stdout.write(`${JSON.stringify({
    type: "raydium_standard_discovery_error",
    error: message.slice(0, 512),
  })}\n`);
  process.exitCode = 1;
}

/** Read-only local exact-input Meteora DLMM quote engine. */

import { createRequire } from "node:module";

import BN from "bn.js";
import type { BinArrayAccount, SwapQuote } from "@meteora-ag/dlmm";
import {
  Connection,
  type AccountInfo,
  type Context,
  PublicKey,
} from "@solana/web3.js";

import type { ConfigureMessage, PoolDescriptor, QuoteRequestMessage } from "./protocol.js";
import { sharedRpcFetchMiddleware } from "./rpcPacer.js";

const DEFAULT_BIN_CACHE_MAX_AGE_MS = 300_000;

export interface MeteoraPoolStateNotice {
  pool_id: string;
  label: string;
  slot: number;
  active_id: number;
  token_a_mint: string;
  token_b_mint: string;
  token_a_decimals: number;
  token_b_decimals: number;
  bin_step: number;
  bin_cache_age_ms: number | null;
}

export interface MeteoraQuoteResult {
  request_id: string;
  pool_id: string;
  label: string;
  status: "ok" | "stale_state" | "quote_unavailable";
  state_slot: number;
  input_mint: string;
  output_mint: string;
  input_amount_raw: string;
  output_amount_raw?: string;
  consumed_input_amount_raw?: string;
  pool_fee_raw?: string;
  protocol_fee_raw?: string;
  pool_fee_on_input?: boolean;
  price_impact_pct?: string;
  all_trade?: boolean;
  bin_cache_age_ms?: number;
  error?: string;
}

export interface MeteoraEngineCallbacks {
  onPoolState: (notice: MeteoraPoolStateNotice) => void;
}

interface AttachedPool {
  descriptor: PoolDescriptor;
  address: PublicKey;
  dlmm: DlmmClient;
  slot: number;
  receivedAtMs: number;
  poolSubscriptionId: number;
  binArrays: Map<string, BinArrayAccount>;
  binSubscriptionIds: Map<string, number>;
  binCacheAtMs: number;
  binRefresh?: Promise<void>;
}

interface DlmmClient {
  program: Parameters<DecodeAccount>[0];
  lbPair: {
    activeId: number;
    binStep: number;
    [key: string]: unknown;
  };
  tokenX: { mint: { address: PublicKey; decimals: number } };
  tokenY: { mint: { address: PublicKey; decimals: number } };
  getBinArrayForSwap(swapForY: boolean, count?: number): Promise<BinArrayAccount[]>;
  swapQuote(
    inAmount: BN,
    swapForY: boolean,
    allowedSlippage: BN,
    binArrays: BinArrayAccount[],
    isPartialFill?: boolean,
  ): SwapQuote;
}

interface DlmmFactory {
  create(connection: Connection, pool: PublicKey): Promise<DlmmClient>;
}

type DecodeAccount = typeof import("@meteora-ag/dlmm").decodeAccount;
type MeteoraRuntime = DlmmFactory & { decodeAccount: DecodeAccount };

// Meteora's published ESM entry currently contains an Anchor directory
// import rejected by recent Node versions.  The package's documented
// `require` export is equivalent and works in this otherwise-ESM worker.
const require = createRequire(import.meta.url);
const MeteoraSdk = require("@meteora-ag/dlmm") as MeteoraRuntime;
const DLMM: DlmmFactory = MeteoraSdk;

/**
 * Keeps the LB-pair and nearby bin-array accounts in RAM.  The SDK is used
 * only for account decoding and quote math; no wallet or transaction method
 * is exposed by this class.
 */
export class MeteoraDlmmQuoteEngine {
  private connection: Connection | null = null;
  private pools = new Map<string, AttachedPool>();
  private binCacheMaxAgeMs = DEFAULT_BIN_CACHE_MAX_AGE_MS;

  public constructor(private readonly callbacks: MeteoraEngineCallbacks) {}

  public async open(config: ConfigureMessage): Promise<void> {
    if (this.connection !== null) throw new Error("Meteora quote engine is already open");
    this.binCacheMaxAgeMs = config.tick_cache_max_age_ms ?? DEFAULT_BIN_CACHE_MAX_AGE_MS;
    this.connection = new Connection(config.rpc_http_url, {
      commitment: "processed",
      wsEndpoint: config.rpc_ws_url,
      fetchMiddleware: sharedRpcFetchMiddleware,
      confirmTransactionInitialTimeout: 10_000,
    });
    // Deliberately serial to stay below a free RPC's startup burst limit.
    for (const descriptor of config.meteora_dlmm_pools) {
      await this.attachPool(descriptor);
    }
  }

  public async close(): Promise<void> {
    const connection = this.connection;
    if (connection !== null) {
      const subscriptions = [...this.pools.values()].flatMap((pool) => [
        pool.poolSubscriptionId,
        ...pool.binSubscriptionIds.values(),
      ]);
      await Promise.all(
        subscriptions
          .filter((id) => id >= 0)
          .map(async (id) => connection.removeAccountChangeListener(id)),
      );
    }
    this.pools.clear();
    this.connection = null;
  }

  /** Reconcile LB-pair state in one batch; bin arrays stay demand-refreshed. */
  public async refreshAllPoolStates(): Promise<void> {
    const connection = this.requireConnection();
    const pools = [...this.pools.values()];
    if (pools.length === 0) return;
    const snapshot = await connection.getMultipleAccountsInfoAndContext(
      pools.map((pool) => pool.address),
      "processed",
    );
    for (let index = 0; index < pools.length; index += 1) {
      const pool = pools[index];
      const account = snapshot.value[index];
      if (pool === undefined || account === null || account === undefined) {
        throw new Error("Meteora refresh returned a missing LB-pair account");
      }
      if (snapshot.context.slot < pool.slot) continue;
      pool.dlmm.lbPair = MeteoraSdk.decodeAccount(
        pool.dlmm.program,
        "lbPair",
        account.data,
      ) as DlmmClient["lbPair"];
      pool.slot = snapshot.context.slot;
      pool.receivedAtMs = Date.now();
      this.emitPoolState(pool);
    }
  }

  public async quote(request: QuoteRequestMessage): Promise<MeteoraQuoteResult> {
    const pool = this.pools.get(request.pool_id);
    if (pool === undefined) return this.unavailable(request, "pool is not configured in this worker");
    if (request.minimum_state_slot !== undefined && pool.slot < request.minimum_state_slot) {
      return {
        ...this.unavailable(request, "worker state is older than required minimum slot"),
        status: "stale_state",
        state_slot: pool.slot,
      };
    }
    const tokenX = pool.dlmm.tokenX.mint.address.toBase58();
    const tokenY = pool.dlmm.tokenY.mint.address.toBase58();
    const swapForY = request.input_mint === tokenX && request.output_mint === tokenY;
    const swapForX = request.input_mint === tokenY && request.output_mint === tokenX;
    if (!swapForY && !swapForX) {
      return this.unavailable(request, "input/output mint pair does not match configured pool");
    }
    try {
      await this.ensureBinCache(pool, false);
      const input = new BN(request.input_amount_raw, 10);
      const result = pool.dlmm.swapQuote(
        input,
        swapForY,
        new BN(0),
        [...pool.binArrays.values()],
        false,
      );
      return {
        request_id: request.request_id,
        pool_id: pool.descriptor.pool_id,
        label: pool.descriptor.label,
        status: "ok",
        state_slot: pool.slot,
        input_mint: request.input_mint,
        output_mint: request.output_mint,
        input_amount_raw: request.input_amount_raw,
        output_amount_raw: result.outAmount.toString(10),
        consumed_input_amount_raw: result.consumedInAmount.toString(10),
        pool_fee_raw: result.fee.toString(10),
        protocol_fee_raw: result.protocolFee.toString(10),
        pool_fee_on_input: result.feeOnInput,
        price_impact_pct: result.priceImpact.toString(),
        all_trade: result.consumedInAmount.eq(input),
        bin_cache_age_ms: Math.max(0, Date.now() - pool.binCacheAtMs),
      };
    } catch (error) {
      void this.ensureBinCache(pool, true).catch(() => undefined);
      return this.unavailable(request, error instanceof Error ? error.message : String(error));
    }
  }

  private async attachPool(descriptor: PoolDescriptor): Promise<void> {
    const connection = this.requireConnection();
    const publicKey = new PublicKey(descriptor.pool_id);
    const dlmm = await DLMM.create(connection, publicKey);
    const account = await connection.getAccountInfoAndContext(publicKey, "processed");
    if (account.value === null) throw new Error(`Meteora pool account ${descriptor.pool_id} was not found`);
    dlmm.lbPair = MeteoraSdk.decodeAccount(
      dlmm.program,
      "lbPair",
      account.value.data,
    ) as DlmmClient["lbPair"];
    const pool: AttachedPool = {
      descriptor,
      address: publicKey,
      dlmm,
      slot: account.context.slot,
      receivedAtMs: Date.now(),
      poolSubscriptionId: -1,
      binArrays: new Map(),
      binSubscriptionIds: new Map(),
      binCacheAtMs: 0,
    };
    pool.poolSubscriptionId = connection.onAccountChange(
      publicKey,
      (updated, context) => this.updatePair(pool, updated, context),
      "processed",
    );
    this.pools.set(descriptor.pool_id, pool);
    await this.ensureBinCache(pool, true);
    this.emitPoolState(pool);
  }

  private updatePair(pool: AttachedPool, account: AccountInfo<Buffer>, context: Context): void {
    if (context.slot < pool.slot) return;
    pool.dlmm.lbPair = MeteoraSdk.decodeAccount(
      pool.dlmm.program,
      "lbPair",
      account.data,
    ) as DlmmClient["lbPair"];
    pool.slot = context.slot;
    pool.receivedAtMs = Date.now();
    this.emitPoolState(pool);
  }

  private updateBin(
    pool: AttachedPool,
    publicKey: PublicKey,
    account: AccountInfo<Buffer>,
    context: Context,
  ): void {
    pool.binArrays.set(publicKey.toBase58(), {
      publicKey,
      account: MeteoraSdk.decodeAccount(pool.dlmm.program, "binArray", account.data),
    });
    pool.slot = Math.max(pool.slot, context.slot);
    pool.receivedAtMs = Date.now();
    pool.binCacheAtMs = Date.now();
    this.emitPoolState(pool);
  }

  private async ensureBinCache(pool: AttachedPool, force: boolean): Promise<void> {
    if (!force && pool.binArrays.size > 0 && Date.now() - pool.binCacheAtMs <= this.binCacheMaxAgeMs) {
      return;
    }
    if (pool.binRefresh !== undefined) return pool.binRefresh;
    pool.binRefresh = this.refreshBins(pool);
    try {
      await pool.binRefresh;
    } finally {
      pool.binRefresh = undefined;
    }
  }

  private async refreshBins(pool: AttachedPool): Promise<void> {
    const connection = this.requireConnection();
    const [forY, forX] = await Promise.all([
      pool.dlmm.getBinArrayForSwap(true, 4),
      pool.dlmm.getBinArrayForSwap(false, 4),
    ]);
    const current = new Map<string, BinArrayAccount>();
    for (const item of [...forY, ...forX]) current.set(item.publicKey.toBase58(), item);
    if (current.size === 0) throw new Error("Meteora returned no nearby bin arrays");

    for (const [address, subscriptionId] of pool.binSubscriptionIds) {
      if (!current.has(address)) {
        await connection.removeAccountChangeListener(subscriptionId);
        pool.binSubscriptionIds.delete(address);
      }
    }
    for (const [address, item] of current) {
      if (!pool.binSubscriptionIds.has(address)) {
        const subscriptionId = connection.onAccountChange(
          item.publicKey,
          (updated, context) => this.updateBin(pool, item.publicKey, updated, context),
          "processed",
        );
        pool.binSubscriptionIds.set(address, subscriptionId);
      }
    }
    pool.binArrays = current;
    pool.binCacheAtMs = Date.now();
  }

  private emitPoolState(pool: AttachedPool): void {
    this.callbacks.onPoolState({
      pool_id: pool.descriptor.pool_id,
      label: pool.descriptor.label,
      slot: pool.slot,
      active_id: pool.dlmm.lbPair.activeId,
      token_a_mint: pool.dlmm.tokenX.mint.address.toBase58(),
      token_b_mint: pool.dlmm.tokenY.mint.address.toBase58(),
      token_a_decimals: pool.dlmm.tokenX.mint.decimals,
      token_b_decimals: pool.dlmm.tokenY.mint.decimals,
      bin_step: pool.dlmm.lbPair.binStep,
      bin_cache_age_ms: pool.binCacheAtMs === 0 ? null : Math.max(0, Date.now() - pool.binCacheAtMs),
    });
  }

  private unavailable(request: QuoteRequestMessage, error: string): MeteoraQuoteResult {
    const pool = this.pools.get(request.pool_id);
    return {
      request_id: request.request_id,
      pool_id: request.pool_id,
      label: pool?.descriptor.label ?? request.pool_id,
      status: "quote_unavailable",
      state_slot: pool?.slot ?? 0,
      input_mint: request.input_mint,
      output_mint: request.output_mint,
      input_amount_raw: request.input_amount_raw,
      error: error.slice(0, 512),
    };
  }

  private requireConnection(): Connection {
    if (this.connection === null) throw new Error("Meteora quote engine is not open");
    return this.connection;
  }
}

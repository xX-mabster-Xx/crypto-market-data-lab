/** Read-only local exact-input Raydium CLMM quote engine. */

import BN from "bn.js";
import {
  Connection,
  type AccountInfo,
  type Context,
  type EpochInfo,
  PublicKey,
} from "@solana/web3.js";
import {
  PoolInfoLayout,
  PoolUtils,
  TickArrayLayout,
  TickUtil,
  type ApiV3PoolInfoConcentratedItem,
  type ComputeClmmPoolInfo,
  type ReturnTypeFetchMultiplePoolTickArrays,
} from "@raydium-io/raydium-sdk-v2";

import type { ConfigureMessage, PoolDescriptor, QuoteRequestMessage } from "./protocol.js";
import { sharedRpcFetchMiddleware } from "./rpcPacer.js";

const RAYDIUM_POOL_BY_ID_ENDPOINT = "https://api-v3.raydium.io/pools/info/ids";
const DEFAULT_TICK_CACHE_MAX_AGE_MS = 300_000;
const EPOCH_REFRESH_MS = 60_000;
const BLOCK_TIME_REFRESH_MS = 5_000;

export interface PoolStateNotice {
  pool_id: string;
  label: string;
  slot: number;
  tick_current: number;
  token_a_mint: string;
  token_b_mint: string;
  token_a_decimals: number;
  token_b_decimals: number;
  sqrt_price_x64: string;
  tick_cache_age_ms: number | null;
}

export interface QuoteResult {
  request_id: string;
  pool_id: string;
  label: string;
  status: "ok" | "stale_state" | "quote_unavailable";
  state_slot: number;
  input_mint: string;
  output_mint: string;
  input_amount_raw: string;
  output_amount_raw?: string;
  pool_fee_raw?: string;
  price_impact_pct?: string;
  all_trade?: boolean;
  tick_cache_age_ms?: number;
  chain_time_age_ms?: number;
  error?: string;
}

export interface RaydiumEngineCallbacks {
  onPoolState: (notice: PoolStateNotice) => void;
}

type PoolMetadata = Pick<
  ApiV3PoolInfoConcentratedItem,
  "id" | "programId" | "mintA" | "mintB" | "config" | "price"
>;

type TickCache = ReturnTypeFetchMultiplePoolTickArrays[string];
type DecodedPool = ReturnType<typeof PoolInfoLayout.decode>;

interface AttachedPool {
  descriptor: PoolDescriptor;
  publicKey: PublicKey;
  metadata: PoolMetadata;
  compute: ComputeClmmPoolInfo;
  tickCache: TickCache;
  tickCacheAtMs: number;
  currentTickArray: number;
  slot: number;
  receivedAtMs: number;
  subscriptionId: number;
  tickSubscriptions: Map<string, { id: number; slot: number }>;
  tickRefresh?: Promise<void>;
  updateChain?: Promise<void>;
}

interface RaydiumEnvelope {
  success?: unknown;
  data?: unknown;
  msg?: unknown;
}

function currentTickArray(tick: number, tickSpacing: number): number {
  // Raydium CLMM has 60 ticks per array.  Keeping this expression local avoids
  // a network read merely to decide whether the cached traversal data is stale.
  return Math.floor(tick / (tickSpacing * 60));
}

function finiteTimestamp(value: number | null): number | null {
  return value !== null && Number.isFinite(value) && value > 0 ? value : null;
}

function isPoolMetadata(value: unknown, expectedPoolId: string): value is PoolMetadata {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  const item = value as Record<string, unknown>;
  const mintA = item.mintA;
  const mintB = item.mintB;
  const config = item.config;
  return (
    item.id === expectedPoolId &&
    item.type === "Concentrated" &&
    typeof item.programId === "string" &&
    mintA !== null && typeof mintA === "object" &&
    mintB !== null && typeof mintB === "object" &&
    config !== null && typeof config === "object" &&
    typeof item.price === "number"
  );
}

async function fetchPoolMetadata(poolId: string): Promise<PoolMetadata> {
  const url = `${RAYDIUM_POOL_BY_ID_ENDPOINT}?ids=${encodeURIComponent(poolId)}`;
  const response = await fetch(url, { headers: { Accept: "application/json" } });
  if (!response.ok) {
    throw new Error(`Raydium pool metadata HTTP ${response.status}`);
  }
  const envelope = await response.json() as RaydiumEnvelope;
  if (envelope.success !== true) {
    throw new Error(`Raydium pool metadata failure: ${String(envelope.msg ?? "unknown")}`);
  }
  const payload = Array.isArray(envelope.data) ? envelope.data[0] : envelope.data;
  if (!isPoolMetadata(payload, poolId)) {
    throw new Error("Raydium metadata is not a concentrated pool matching requested ID");
  }
  return payload;
}

/**
 * Holds static API metadata, fresh on-chain PoolState, and a bounded tick
 * cache.  It never constructs, signs, simulates, or sends a transaction.
 */
export class RaydiumClmmQuoteEngine {
  private connection: Connection | null = null;
  private pools = new Map<string, AttachedPool>();
  private tickCacheMaxAgeMs = DEFAULT_TICK_CACHE_MAX_AGE_MS;
  private epochInfo: EpochInfo | null = null;
  private epochFetchedAtMs = 0;
  private epochRefresh: Promise<void> | null = null;
  private blockTimestamp: number | null = null;
  private blockTimestampFetchedAtMs = 0;
  private blockTimestampRefresh: Promise<void> | null = null;

  public constructor(private readonly callbacks: RaydiumEngineCallbacks) {}

  public async open(config: ConfigureMessage): Promise<void> {
    if (this.connection !== null) throw new Error("Raydium quote engine is already open");
    this.tickCacheMaxAgeMs = config.tick_cache_max_age_ms ?? DEFAULT_TICK_CACHE_MAX_AGE_MS;
    this.connection = new Connection(config.rpc_http_url, {
      commitment: "processed",
      wsEndpoint: config.rpc_ws_url,
      fetchMiddleware: sharedRpcFetchMiddleware,
      confirmTransactionInitialTimeout: 10_000,
    });
    // Deliberately initialize serially.  This holds startup below a free RPC's
    // burst ceiling; later pool batches can be added after measuring it.
    for (const descriptor of config.raydium_clmm_pools) {
      await this.attachPool(descriptor);
    }
  }

  public async close(): Promise<void> {
    const connection = this.connection;
    if (connection !== null) {
      await Promise.all(
        [...this.pools.values()].map(async (pool) => {
          await connection.removeAccountChangeListener(pool.subscriptionId);
          await Promise.all([...pool.tickSubscriptions.values()].map(
            (subscription) => connection.removeAccountChangeListener(subscription.id),
          ));
        }),
      );
    }
    this.pools.clear();
    this.epochRefresh = null;
    this.blockTimestampRefresh = null;
    this.connection = null;
  }

  /** Reconcile every subscribed pool against one current RPC snapshot. */
  public async refreshAllPoolStates(): Promise<void> {
    const connection = this.requireConnection();
    const pools = [...this.pools.values()];
    if (pools.length === 0) return;
    const snapshot = await connection.getMultipleAccountsInfoAndContext(
      pools.map((pool) => pool.publicKey),
      "processed",
    );
    for (let index = 0; index < pools.length; index += 1) {
      const pool = pools[index];
      const account = snapshot.value[index];
      if (pool === undefined || account === null || account === undefined) {
        throw new Error("Raydium CLMM refresh returned a missing pool account");
      }
      if (snapshot.context.slot < pool.slot) continue;
      pool.compute = this.computeFromDecoded(pool, PoolInfoLayout.decode(account.data));
      pool.slot = snapshot.context.slot;
      pool.receivedAtMs = Date.now();
      const nextArray = currentTickArray(pool.compute.tickCurrent, pool.compute.tickSpacing);
      if (nextArray !== pool.currentTickArray) {
        pool.currentTickArray = nextArray;
        await this.ensureTickCache(pool, true);
      }
      this.emitPoolState(pool);
    }
  }

  public async quote(request: QuoteRequestMessage): Promise<QuoteResult> {
    const pool = this.pools.get(request.pool_id);
    if (pool === undefined) {
      return this.unavailable(request, "pool is not configured in this worker");
    }
    if (request.minimum_state_slot !== undefined && pool.slot < request.minimum_state_slot) {
      return {
        ...this.unavailable(request, "worker state is older than required minimum slot"),
        status: "stale_state",
        state_slot: pool.slot,
      };
    }
    const expectedOutput = request.input_mint === pool.compute.mintA.address
      ? pool.compute.mintB.address
      : request.input_mint === pool.compute.mintB.address
        ? pool.compute.mintA.address
        : null;
    if (expectedOutput === null || expectedOutput !== request.output_mint) {
      return this.unavailable(request, "input/output mint pair does not match configured pool");
    }
    try {
      await this.ensureTickCache(pool, false);
      const { epochInfo, blockTimestamp, chainTimeAgeMs } = await this.chainContext(pool.slot);
      const tokenOut = request.output_mint === pool.compute.mintA.address
        ? pool.compute.mintA
        : pool.compute.mintB;
      const result = PoolUtils.computeAmountOutFormat({
        poolInfo: pool.compute,
        tickarrayBitmapExtension: pool.compute.exBitmapInfo,
        tickArrayCache: pool.tickCache,
        amountIn: new BN(request.input_amount_raw, 10),
        tokenOut,
        slippage: 0,
        epochInfo,
        blockTimestamp,
        catchLiquidityInsufficient: true,
      });
      return {
        request_id: request.request_id,
        pool_id: pool.descriptor.pool_id,
        label: pool.descriptor.label,
        status: "ok",
        state_slot: pool.slot,
        input_mint: request.input_mint,
        output_mint: request.output_mint,
        input_amount_raw: request.input_amount_raw,
        output_amount_raw: result.amountOut.amount.raw.toString(10),
        pool_fee_raw: result.fee.raw.toString(10),
        price_impact_pct: result.priceImpact.toFixed(8),
        all_trade: result.allTrade,
        tick_cache_age_ms: Math.max(0, Date.now() - pool.tickCacheAtMs),
        chain_time_age_ms: chainTimeAgeMs,
      };
    } catch (error) {
      // One stale tick cache should not look like a valid price.  Refresh the
      // on-chain data once only for the *next* request; do not issue a hidden
      // quote-API fallback or retry a quote with unknown state.
      void this.refreshFullCompute(pool).catch(() => undefined);
      return this.unavailable(request, error instanceof Error ? error.message : String(error));
    }
  }

  private async attachPool(descriptor: PoolDescriptor): Promise<void> {
    const connection = this.requireConnection();
    const publicKey = new PublicKey(descriptor.pool_id);
    const metadata = await fetchPoolMetadata(descriptor.pool_id);
    const account = await connection.getAccountInfoAndContext(publicKey, "processed");
    if (account.value === null) throw new Error(`Raydium pool account ${descriptor.pool_id} was not found`);
    const decoded = PoolInfoLayout.decode(account.value.data);
    const compute = await PoolUtils.fetchComputeClmmInfo({
      connection,
      poolInfo: metadata,
      rpcData: decoded,
    });
    const tickResult = await PoolUtils.fetchMultiplePoolTickArrays({
      connection,
      poolKeys: [compute],
    });
    const tickCache = tickResult[descriptor.pool_id];
    if (tickCache === undefined || Object.keys(tickCache).length === 0) {
      throw new Error(`Raydium CLMM ${descriptor.pool_id} returned no current tick arrays`);
    }
    const pool: AttachedPool = {
      descriptor,
      publicKey,
      metadata,
      compute,
      tickCache,
      tickCacheAtMs: Date.now(),
      currentTickArray: currentTickArray(compute.tickCurrent, compute.tickSpacing),
      slot: account.context.slot,
      receivedAtMs: Date.now(),
      subscriptionId: -1,
      tickSubscriptions: new Map(),
    };
    pool.subscriptionId = connection.onAccountChange(
      publicKey,
      (updated, context) => {
        // A transient tick-array RPC failure affects this pool's next quote,
        // not the integrity of every protocol hosted by the worker.
        void this.enqueueUpdate(pool, updated, context).catch(() => undefined);
      },
      "processed",
    );
    this.pools.set(descriptor.pool_id, pool);
    await this.installTickCache(pool, tickCache);
    this.emitPoolState(pool);
  }

  private async enqueueUpdate(pool: AttachedPool, account: AccountInfo<Buffer>, context: Context): Promise<void> {
    const previous = pool.updateChain ?? Promise.resolve();
    pool.updateChain = previous
      .catch(() => undefined)
      .then(async () => {
        if (context.slot < pool.slot) return;
        const decoded = PoolInfoLayout.decode(account.data);
        pool.compute = this.computeFromDecoded(pool, decoded);
        pool.slot = context.slot;
        pool.receivedAtMs = Date.now();
        const nextArray = currentTickArray(pool.compute.tickCurrent, pool.compute.tickSpacing);
        if (nextArray !== pool.currentTickArray) {
          pool.currentTickArray = nextArray;
          await this.ensureTickCache(pool, true);
        }
        this.emitPoolState(pool);
      });
    await pool.updateChain;
  }

  private computeFromDecoded(pool: AttachedPool, decoded: DecodedPool): ComputeClmmPoolInfo {
    return {
      ...decoded,
      accInfo: decoded,
      id: pool.publicKey,
      version: 6,
      mintA: pool.metadata.mintA,
      mintB: pool.metadata.mintB,
      ammConfig: pool.compute.ammConfig,
      programId: pool.compute.programId,
      currentPrice: TickUtil.sqrtPriceX64ToPrice(
        decoded.sqrtPriceX64,
        pool.metadata.mintA.decimals,
        pool.metadata.mintB.decimals,
      ),
      exBitmapAccount: pool.compute.exBitmapAccount,
      exBitmapInfo: pool.compute.exBitmapInfo,
      startTime: decoded.startTime.toNumber(),
      rewardInfos: decoded.rewardInfos,
    };
  }

  private async ensureTickCache(pool: AttachedPool, force: boolean): Promise<void> {
    const ageMs = Date.now() - pool.tickCacheAtMs;
    if (!force && ageMs <= this.tickCacheMaxAgeMs) return;
    if (pool.tickRefresh !== undefined) return pool.tickRefresh;
    const connection = this.requireConnection();
    pool.tickRefresh = (async () => {
      const refreshed = await PoolUtils.fetchMultiplePoolTickArrays({
        connection,
        poolKeys: [pool.compute],
      });
      const cache = refreshed[pool.descriptor.pool_id];
      if (cache === undefined || Object.keys(cache).length === 0) {
        throw new Error("Raydium tick-array refresh returned no active arrays");
      }
      await this.installTickCache(pool, cache);
    })();
    try {
      await pool.tickRefresh;
    } finally {
      pool.tickRefresh = undefined;
    }
  }

  /** Subscribe traversal arrays too: pool notifications alone miss LP changes. */
  private async installTickCache(pool: AttachedPool, cache: TickCache): Promise<void> {
    const connection = this.requireConnection();
    const arrays = Object.values(cache);
    const wanted = new Set(arrays.map((array) => array.address.toBase58()));
    for (const array of arrays) {
      const address = array.address;
      const key = address.toBase58();
      if (pool.tickSubscriptions.has(key)) continue;
      const subscription = { id: -1, slot: 0 };
      subscription.id = connection.onAccountChange(address, (account, context) => {
        if (context.slot < subscription.slot) return;
        try {
          const decoded = TickArrayLayout.decode(account.data);
          if (!decoded.poolId.equals(pool.publicKey)) return;
          subscription.slot = context.slot;
          pool.tickCache[String(decoded.startTickIndex)] = { ...decoded, address };
          // Do not publish a fresh pool event from a different account. The
          // next CEX/pool-triggered quote already consumes this updated cache.
        } catch {
          // Fail closed until the next full refresh if an account is invalid.
          pool.tickCacheAtMs = 0;
        }
      }, "processed");
      pool.tickSubscriptions.set(key, subscription);
    }
    // Reconcile after subscribing; SDK discovery has no snapshot context.
    // Per-account slots prevent an HTTP response overwriting newer WS data.
    const snapshot = await connection.getMultipleAccountsInfoAndContext(
      arrays.map((array) => array.address), "processed",
    );
    for (let index = 0; index < arrays.length; index += 1) {
      const array = arrays[index]!;
      const account = snapshot.value[index];
      const subscription = pool.tickSubscriptions.get(array.address.toBase58())!;
      if (account == null) throw new Error("Raydium tick account disappeared");
      if (snapshot.context.slot < subscription.slot) continue;
      const decoded = TickArrayLayout.decode(account.data);
      if (!decoded.poolId.equals(pool.publicKey)) throw new Error("Raydium tick pool mismatch");
      pool.tickCache[String(decoded.startTickIndex)] = { ...decoded, address: array.address };
      subscription.slot = snapshot.context.slot;
    }
    for (const [key, subscription] of pool.tickSubscriptions) {
      if (wanted.has(key)) continue;
      await connection.removeAccountChangeListener(subscription.id);
      pool.tickSubscriptions.delete(key);
    }
    for (const [index, array] of Object.entries(pool.tickCache)) {
      if (!wanted.has(array.address.toBase58())) delete pool.tickCache[index];
    }
    pool.tickCacheAtMs = Date.now();
  }

  private async refreshFullCompute(pool: AttachedPool): Promise<void> {
    const connection = this.requireConnection();
    const account = await connection.getAccountInfoAndContext(pool.publicKey, "processed");
    if (account.value === null) throw new Error("Raydium pool account disappeared during refresh");
    const compute = await PoolUtils.fetchComputeClmmInfo({
      connection,
      poolInfo: pool.metadata,
      rpcData: PoolInfoLayout.decode(account.value.data),
    });
    if (account.context.slot < pool.slot) return;
    pool.compute = compute;
    pool.slot = account.context.slot;
    pool.receivedAtMs = Date.now();
    pool.currentTickArray = currentTickArray(pool.compute.tickCurrent, pool.compute.tickSpacing);
    await this.ensureTickCache(pool, true);
    this.emitPoolState(pool);
  }

  private async chainContext(slot: number): Promise<{ epochInfo: EpochInfo; blockTimestamp: number; chainTimeAgeMs: number }> {
    const connection = this.requireConnection();
    const now = Date.now();
    if (this.epochInfo === null || now - this.epochFetchedAtMs > EPOCH_REFRESH_MS) {
      if (this.epochRefresh === null) {
        this.epochRefresh = connection.getEpochInfo("processed").then((epochInfo) => {
          this.epochInfo = epochInfo;
          this.epochFetchedAtMs = Date.now();
        });
      }
      const refresh = this.epochRefresh;
      try {
        await refresh;
      } finally {
        if (this.epochRefresh === refresh) this.epochRefresh = null;
      }
    }
    if (this.blockTimestamp === null || now - this.blockTimestampFetchedAtMs > BLOCK_TIME_REFRESH_MS) {
      if (this.blockTimestampRefresh === null) {
        this.blockTimestampRefresh = connection.getBlockTime(slot).then((value) => {
          const chainTimestamp = finiteTimestamp(value);
          if (chainTimestamp === null) {
            throw new Error("Solana RPC returned no block timestamp for current pool slot");
          }
          this.blockTimestamp = chainTimestamp;
          this.blockTimestampFetchedAtMs = Date.now();
        });
      }
      const refresh = this.blockTimestampRefresh;
      try {
        await refresh;
      } finally {
        if (this.blockTimestampRefresh === refresh) this.blockTimestampRefresh = null;
      }
    }
    const epochInfo = this.epochInfo;
    const blockTimestamp = this.blockTimestamp;
    if (epochInfo === null || blockTimestamp === null) {
      throw new Error("Solana chain context refresh completed without usable state");
    }
    return {
      epochInfo,
      blockTimestamp,
      chainTimeAgeMs: Math.max(0, Date.now() - this.blockTimestampFetchedAtMs),
    };
  }

  private emitPoolState(pool: AttachedPool): void {
    this.callbacks.onPoolState({
      pool_id: pool.descriptor.pool_id,
      label: pool.descriptor.label,
      slot: pool.slot,
      tick_current: pool.compute.tickCurrent,
      token_a_mint: pool.compute.mintA.address,
      token_b_mint: pool.compute.mintB.address,
      token_a_decimals: pool.compute.mintA.decimals,
      token_b_decimals: pool.compute.mintB.decimals,
      sqrt_price_x64: pool.compute.sqrtPriceX64.toString(10),
      tick_cache_age_ms: pool.tickCacheAtMs === 0 ? null : Math.max(0, Date.now() - pool.tickCacheAtMs),
    });
  }

  private unavailable(request: QuoteRequestMessage, error: string): QuoteResult {
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
    if (this.connection === null) throw new Error("Raydium quote engine is not open");
    return this.connection;
  }
}

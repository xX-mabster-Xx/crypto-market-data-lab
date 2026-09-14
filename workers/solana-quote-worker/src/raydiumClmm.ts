/** Read-only local exact-input Raydium CLMM quote engine. */

import BN from "bn.js";
import { performance } from "node:perf_hooks";
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
import {
  coreRefreshDue,
  DebouncedStateEmitter,
  LatestOnlyMailbox,
  PoolSlotProvenance,
  type RunRpcJob,
} from "./engineRuntime.js";
import { scheduleRpc, sharedRpcFetch } from "./rpcPacer.js";

const RAYDIUM_POOL_BY_ID_ENDPOINT = "https://api-v3.raydium.io/pools/info/ids";
const DEFAULT_TICK_CACHE_MAX_AGE_MS = 300_000;
const EPOCH_REFRESH_MS = 60_000;
const BLOCK_TIME_REFRESH_MS = 5_000;
const DEFAULT_POOL_STATE_EMIT_MIN_INTERVAL_MS = 100;
const DEFAULT_CORE_REFRESH_AFTER_MS = 15_000;
const DEFAULT_REFRESH_STAGGER_WINDOW_MS = 5_000;

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
  core_state_slot: number;
  dependency_slot_min: number | null;
  dependency_slot_max: number | null;
  dependency_generation: number;
}

export interface QuoteResult {
  request_id: string;
  pool_id: string;
  label: string;
  status: "ok" | "stale_state" | "quote_unavailable";
  state_slot: number;
  core_state_slot: number;
  dependency_slot_min: number | null;
  dependency_slot_max: number | null;
  dependency_generation: number;
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
  provenance: PoolSlotProvenance;
  subscriptionId: number;
  tickSubscriptions: Map<string, number>;
  tickRefresh?: Promise<void>;
  coreMailbox: LatestOnlyMailbox<{ account: AccountInfo<Buffer>; context: Context }>;
  stateEmitter: DebouncedStateEmitter;
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
  private poolStateEmitMinIntervalMs = DEFAULT_POOL_STATE_EMIT_MIN_INTERVAL_MS;
  private coreRefreshAfterMs = DEFAULT_CORE_REFRESH_AFTER_MS;
  private refreshStaggerWindowMs = DEFAULT_REFRESH_STAGGER_WINDOW_MS;
  private closed = false;
  private refreshesStarted = 0;
  private refreshesCompleted = 0;
  private refreshesUnchanged = 0;

  public constructor(
    private readonly callbacks: RaydiumEngineCallbacks,
    private readonly runRpcJob: RunRpcJob = scheduleRpc,
  ) {}

  public async open(config: ConfigureMessage): Promise<void> {
    if (this.connection !== null) throw new Error("Raydium quote engine is already open");
    this.closed = false;
    this.tickCacheMaxAgeMs = config.tick_cache_max_age_ms ?? DEFAULT_TICK_CACHE_MAX_AGE_MS;
    this.poolStateEmitMinIntervalMs = config.pool_state_emit_min_interval_ms
      ?? DEFAULT_POOL_STATE_EMIT_MIN_INTERVAL_MS;
    this.coreRefreshAfterMs = config.core_refresh_after_ms
      ?? config.state_snapshot_refresh_interval_ms
      ?? DEFAULT_CORE_REFRESH_AFTER_MS;
    this.refreshStaggerWindowMs = Math.min(
      config.refresh_stagger_window_ms ?? DEFAULT_REFRESH_STAGGER_WINDOW_MS,
      this.coreRefreshAfterMs,
    );
    this.connection = new Connection(config.rpc_http_url, {
      commitment: "processed",
      wsEndpoint: config.rpc_ws_url,
      fetch: sharedRpcFetch,
      confirmTransactionInitialTimeout: 10_000,
    });
    // Deliberately initialize serially.  This holds startup below a free RPC's
    // burst ceiling; later pool batches can be added after measuring it.
    for (const descriptor of config.raydium_clmm_pools) {
      await this.attachPool(descriptor);
    }
  }

  public async close(): Promise<void> {
    this.closed = true;
    const connection = this.connection;
    if (connection !== null) {
      for (const pool of this.pools.values()) pool.stateEmitter.dispose();
      await Promise.all(
        [...this.pools.values()].map(async (pool) => {
          await connection.removeAccountChangeListener(pool.subscriptionId);
          await Promise.all([...pool.tickSubscriptions.values()].map(
            (subscriptionId) => connection.removeAccountChangeListener(subscriptionId),
          ));
          await pool.coreMailbox.dispose();
        }),
      );
    }
    this.pools.clear();
    this.epochRefresh = null;
    this.blockTimestampRefresh = null;
    this.connection = null;
  }

  /** Compatibility hook: maintenance is stale-driven and starts at most one pool refresh. */
  public async refreshAllPoolStates(): Promise<void> {
    await this.maintainStalePools(performance.now());
  }

  public async maintainStalePools(nowMs = performance.now()): Promise<void> {
    for (const pool of this.pools.values()) {
      if (pool.provenance.refreshInFlight || !coreRefreshDue(
        pool.provenance,
        `raydium-clmm:${pool.descriptor.pool_id}`,
        nowMs,
        this.coreRefreshAfterMs,
        this.refreshStaggerWindowMs,
      )) continue;
      await this.scheduleCoreRefresh(pool);
      return;
    }
  }

  public async quote(request: QuoteRequestMessage): Promise<QuoteResult> {
    const pool = this.pools.get(request.pool_id);
    if (pool === undefined) {
      return this.unavailable(request, "pool is not configured in this worker");
    }
    if (request.minimum_state_slot !== undefined
      && pool.provenance.coreStateSlot < request.minimum_state_slot) {
      return {
        ...this.unavailable(request, "worker state is older than required minimum slot"),
        status: "stale_state",
        state_slot: pool.provenance.coreStateSlot,
        ...pool.provenance.fields(),
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
      const { epochInfo, blockTimestamp, chainTimeAgeMs } = await this.chainContext(
        pool.provenance.coreStateSlot,
      );
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
        state_slot: pool.provenance.coreStateSlot,
        ...pool.provenance.fields(),
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
      void this.scheduleCoreRefresh(pool).catch(() => undefined);
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
    const provenance = new PoolSlotProvenance(account.context.slot);
    let pool!: AttachedPool;
    pool = {
      descriptor,
      publicKey,
      metadata,
      compute,
      tickCache,
      tickCacheAtMs: Date.now(),
      currentTickArray: currentTickArray(compute.tickCurrent, compute.tickSpacing),
      provenance,
      subscriptionId: -1,
      tickSubscriptions: new Map(),
      coreMailbox: new LatestOnlyMailbox(
        account.context.slot,
        async ({ account: updated, context }) => this.processCoreUpdate(pool, updated, context),
      ),
      stateEmitter: new DebouncedStateEmitter(
        () => this.emitPoolStateNow(pool),
        this.poolStateEmitMinIntervalMs,
      ),
    };
    pool.subscriptionId = connection.onAccountChange(
      publicKey,
      (updated, context) => {
        // A transient tick-array RPC failure affects this pool's next quote,
        // not the integrity of every protocol hosted by the worker.
        pool.coreMailbox.submit({ account: updated, context }, context.slot);
      },
      "processed",
    );
    this.pools.set(descriptor.pool_id, pool);
    await this.installTickCache(pool, tickCache);
    this.requestPoolState(pool, "initial", "immediate");
  }

  private async processCoreUpdate(
    pool: AttachedPool,
    account: AccountInfo<Buffer>,
    context: Context,
  ): Promise<boolean> {
    if (this.closed || context.slot <= pool.provenance.coreStateSlot) return false;
    const previousCompute = pool.compute;
    const previousArray = pool.currentTickArray;
    const compute = this.computeFromDecoded(pool, PoolInfoLayout.decode(account.data));
    const nextArray = currentTickArray(compute.tickCurrent, compute.tickSpacing);
    pool.compute = compute;
    pool.currentTickArray = nextArray;
    try {
      if (nextArray !== previousArray) await this.ensureTickCache(pool, true);
    } catch (error) {
      pool.compute = previousCompute;
      pool.currentTickArray = previousArray;
      throw error;
    }
    if (!pool.provenance.acceptCore(context.slot)) return false;
    this.requestPoolState(pool, `core:${context.slot}`, "immediate");
    return true;
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
    const hadSubscriptions = pool.tickSubscriptions.size > 0;
    const generationBefore = pool.provenance.dependencyGeneration;
    const arrays = Object.values(cache);
    const wanted = new Set(arrays.map((array) => array.address.toBase58()));
    for (const array of arrays) {
      const address = array.address;
      const key = address.toBase58();
      if (pool.tickSubscriptions.has(key)) continue;
      const subscriptionId = connection.onAccountChange(address, (account, context) => {
        try {
          const decoded = TickArrayLayout.decode(account.data);
          if (!decoded.poolId.equals(pool.publicKey)) return;
          if (!pool.provenance.acceptDependency(key, context.slot)) return;
          pool.tickCache[String(decoded.startTickIndex)] = { ...decoded, address };
          pool.tickCacheAtMs = Date.now();
          this.requestPoolState(
            pool,
            `dependency:${pool.provenance.dependencyGeneration}`,
            "debounced",
          );
        } catch {
          // Fail closed until the next full refresh if an account is invalid.
          pool.tickCacheAtMs = 0;
        }
      }, "processed");
      pool.tickSubscriptions.set(key, subscriptionId);
    }
    // Reconcile after subscribing; SDK discovery has no snapshot context.
    // Per-account slots prevent an HTTP response overwriting newer WS data.
    const snapshot = await connection.getMultipleAccountsInfoAndContext(
      arrays.map((array) => array.address), "processed",
    );
    for (let index = 0; index < arrays.length; index += 1) {
      const array = arrays[index]!;
      const account = snapshot.value[index];
      const key = array.address.toBase58();
      if (account == null) throw new Error("Raydium tick account disappeared");
      const decoded = TickArrayLayout.decode(account.data);
      if (!decoded.poolId.equals(pool.publicKey)) throw new Error("Raydium tick pool mismatch");
      if (!pool.provenance.acceptDependency(key, snapshot.context.slot)) continue;
      pool.tickCache[String(decoded.startTickIndex)] = { ...decoded, address: array.address };
    }
    for (const [key, subscriptionId] of pool.tickSubscriptions) {
      if (wanted.has(key)) continue;
      await connection.removeAccountChangeListener(subscriptionId);
      pool.tickSubscriptions.delete(key);
      pool.provenance.forgetDependency(key);
    }
    for (const [index, array] of Object.entries(pool.tickCache)) {
      if (!wanted.has(array.address.toBase58())) delete pool.tickCache[index];
    }
    pool.tickCacheAtMs = Date.now();
    pool.provenance.noteDependencyRefresh();
    if (hadSubscriptions && pool.provenance.dependencyGeneration !== generationBefore) {
      this.requestPoolState(
        pool,
        `dependency:${pool.provenance.dependencyGeneration}`,
        "debounced",
      );
    }
  }

  private async scheduleCoreRefresh(pool: AttachedPool): Promise<void> {
    if (this.closed || pool.provenance.refreshInFlight) return;
    pool.provenance.refreshInFlight = true;
    pool.provenance.refreshGeneration += 1;
    this.refreshesStarted += 1;
    try {
      await this.refreshCore(pool);
      this.refreshesCompleted += 1;
    } finally {
      pool.provenance.refreshInFlight = false;
    }
  }

  private async refreshCore(pool: AttachedPool): Promise<void> {
    const connection = this.requireConnection();
    const account = await this.runRpcJob(
      {
        priority: "refresh",
        coalesceKey: `refresh:raydium-clmm:${pool.descriptor.pool_id}:core`,
        description: `refresh Raydium CLMM core ${pool.descriptor.pool_id}`,
      },
      async () => connection.getAccountInfoAndContext(pool.publicKey, "processed"),
    );
    const accountValue = account.value;
    if (accountValue === null) throw new Error("Raydium pool account disappeared during refresh");
    if (account.context.slot < pool.provenance.coreStateSlot) return;
    const compute = await this.runRpcJob(
      {
        priority: "refresh",
        description: `refresh Raydium CLMM derived accounts ${pool.descriptor.pool_id}`,
      },
      async () => PoolUtils.fetchComputeClmmInfo({
        connection,
        poolInfo: pool.metadata,
        rpcData: PoolInfoLayout.decode(accountValue.data),
      }),
    );
    const changed = this.coreFingerprint(pool.compute) !== this.coreFingerprint(compute);
    pool.compute = compute;
    if (account.context.slot > pool.provenance.coreStateSlot) {
      pool.provenance.acceptCore(account.context.slot);
    }
    pool.provenance.noteRpcRefresh();
    pool.currentTickArray = currentTickArray(pool.compute.tickCurrent, pool.compute.tickSpacing);
    await this.ensureTickCache(pool, true);
    if (changed) {
      this.requestPoolState(pool, `refresh:${account.context.slot}`, "immediate");
    } else {
      this.refreshesUnchanged += 1;
    }
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

  private coreFingerprint(compute: ComputeClmmPoolInfo): string {
    return `${compute.tickCurrent}:${compute.sqrtPriceX64.toString(10)}`;
  }

  private requestPoolState(
    pool: AttachedPool,
    fingerprint: string,
    mode: "immediate" | "debounced",
  ): void {
    pool.stateEmitter.request(fingerprint, mode);
  }

  private emitPoolStateNow(pool: AttachedPool): void {
    if (this.closed) return;
    this.callbacks.onPoolState({
      pool_id: pool.descriptor.pool_id,
      label: pool.descriptor.label,
      slot: pool.provenance.coreStateSlot,
      tick_current: pool.compute.tickCurrent,
      token_a_mint: pool.compute.mintA.address,
      token_b_mint: pool.compute.mintB.address,
      token_a_decimals: pool.compute.mintA.decimals,
      token_b_decimals: pool.compute.mintB.decimals,
      sqrt_price_x64: pool.compute.sqrtPriceX64.toString(10),
      tick_cache_age_ms: pool.tickCacheAtMs === 0 ? null : Math.max(0, Date.now() - pool.tickCacheAtMs),
      ...pool.provenance.fields(),
    });
  }

  public runtimeStats(): Record<string, number> {
    let coreUpdatesCoalesced = 0;
    let coreUpdateErrors = 0;
    let corePending = 0;
    let dependencyEmitsCoalesced = 0;
    let externalEmits = 0;
    let refreshInFlight = 0;
    for (const pool of this.pools.values()) {
      const mailbox = pool.coreMailbox.stats();
      const emitter = pool.stateEmitter.stats();
      coreUpdatesCoalesced += mailbox.coalesced_total;
      coreUpdateErrors += mailbox.errors_total;
      corePending += mailbox.pending;
      dependencyEmitsCoalesced += emitter.coalesced_total;
      externalEmits += emitter.external_emits_total;
      refreshInFlight += pool.provenance.refreshInFlight ? 1 : 0;
    }
    return {
      pool_count: this.pools.size,
      core_updates_pending: corePending,
      core_updates_coalesced_total: coreUpdatesCoalesced,
      core_update_errors_total: coreUpdateErrors,
      dependency_emits_coalesced_total: dependencyEmitsCoalesced,
      external_pool_state_emits_total: externalEmits,
      refresh_inflight: refreshInFlight,
      refreshes_started_total: this.refreshesStarted,
      refreshes_completed_total: this.refreshesCompleted,
      refreshes_unchanged_total: this.refreshesUnchanged,
    };
  }

  private unavailable(request: QuoteRequestMessage, error: string): QuoteResult {
    const pool = this.pools.get(request.pool_id);
    return {
      request_id: request.request_id,
      pool_id: request.pool_id,
      label: pool?.descriptor.label ?? request.pool_id,
      status: "quote_unavailable",
      state_slot: pool?.provenance.coreStateSlot ?? 0,
      ...(pool === undefined
        ? {
          core_state_slot: 0,
          dependency_slot_min: null,
          dependency_slot_max: null,
          dependency_generation: 0,
        }
        : pool.provenance.fields()),
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

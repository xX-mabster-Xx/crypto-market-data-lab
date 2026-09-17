/** Read-only local exact-input Orca Whirlpool quote engine. */

import { createRequire } from "node:module";
import { performance } from "node:perf_hooks";

import BN from "bn.js";
import type {
  TickArray,
  TokenExtensionContext,
  WhirlpoolAccountFetcherInterface,
  WhirlpoolData,
} from "@orca-so/whirlpools-sdk";
import {
  Connection,
  type AccountInfo,
  type Context,
  PublicKey,
} from "@solana/web3.js";

import type { ConfigureMessage, PoolDescriptor, QuoteRequestMessage } from "./protocol.js";
import {
  coreRefreshDue,
  coreRefreshOverdueMs,
  DebouncedStateEmitter,
  FairMaintenanceCursor,
  PoolSlotProvenance,
  type RunRpcJob,
} from "./engineRuntime.js";
import { scheduleRpc, sharedRpcFetch } from "./rpcPacer.js";

const DEFAULT_TICK_CACHE_MAX_AGE_MS = 300_000;
const DEFAULT_POOL_STATE_EMIT_MIN_INTERVAL_MS = 100;
const DEFAULT_CORE_REFRESH_AFTER_MS = 15_000;
const DEFAULT_REFRESH_STAGGER_WINDOW_MS = 5_000;

interface OrcaRuntime {
  ORCA_WHIRLPOOL_PROGRAM_ID: PublicKey;
  IGNORE_CACHE: { maxAge: number };
  ParsableTickArray: {
    parse(address: PublicKey, account: AccountInfo<Buffer> | null): TickArray["data"];
  };
  ParsableWhirlpool: {
    parse(address: PublicKey, account: AccountInfo<Buffer> | null): WhirlpoolData | null;
  };
  PoolUtil: {
    isInitializedWithAdaptiveFee(data: WhirlpoolData): boolean;
  };
  SwapUtils: {
    getDefaultSqrtPriceLimit(aToB: boolean): BN;
    getDefaultOtherAmountThreshold(amountSpecifiedIsInput: boolean): BN;
    getTickArrays(
      tickCurrentIndex: number,
      tickSpacing: number,
      aToB: boolean,
      programId: PublicKey,
      pool: PublicKey,
      fetcher: WhirlpoolAccountFetcherInterface,
      options: { maxAge: number },
    ): Promise<TickArray[]>;
  };
  TokenExtensionUtil: {
    buildTokenExtensionContext(
      fetcher: WhirlpoolAccountFetcherInterface,
      data: WhirlpoolData,
      options: { maxAge: number },
    ): Promise<TokenExtensionContext>;
  };
  buildDefaultAccountFetcher(connection: Connection): WhirlpoolAccountFetcherInterface;
  swapQuoteWithParams(
    params: {
      whirlpoolData: WhirlpoolData;
      tokenAmount: BN;
      otherAmountThreshold: BN;
      sqrtPriceLimit: BN;
      aToB: boolean;
      amountSpecifiedIsInput: boolean;
      tickArrays: TickArray[];
      oracleData: null;
      tokenExtensionCtx: TokenExtensionContext;
    },
    slippage: unknown,
  ): {
    estimatedAmountIn: BN;
    estimatedAmountOut: BN;
    estimatedFeeAmount: BN;
    transferFee: {
      deductingFromEstimatedAmountIn: BN;
      deductedFromEstimatedAmountOut: BN;
    };
  };
}

interface CommonRuntime {
  Percentage: {
    fromFraction(numerator: number, denominator: number): unknown;
  };
}

const require = createRequire(import.meta.url);
const Orca = require("@orca-so/whirlpools-sdk") as OrcaRuntime;
const Common = require("@orca-so/common-sdk") as CommonRuntime;

export interface OrcaPoolStateNotice {
  pool_id: string;
  label: string;
  slot: number;
  tick_current: number;
  sqrt_price_x64: string;
  token_a_mint: string;
  token_b_mint: string;
  token_a_decimals: number;
  token_b_decimals: number;
  tick_spacing: number;
  fee_rate_millionths: number;
  tick_cache_age_ms: number | null;
  core_state_slot: number;
  dependency_slot_min: number | null;
  dependency_slot_max: number | null;
  dependency_generation: number;
}

export interface OrcaQuoteResult {
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
  consumed_input_amount_raw?: string;
  pool_fee_raw?: string;
  transfer_fee_input_raw?: string;
  transfer_fee_output_raw?: string;
  pool_fee_on_input?: boolean;
  all_trade?: boolean;
  tick_cache_age_ms?: number;
  error?: string;
}

export interface OrcaEngineCallbacks {
  onPoolState: (notice: OrcaPoolStateNotice) => void;
}

interface AttachedPool {
  descriptor: PoolDescriptor;
  address: PublicKey;
  data: WhirlpoolData;
  tokenExtensionContext: TokenExtensionContext;
  provenance: PoolSlotProvenance;
  coreAccountData: Buffer;
  poolSubscriptionId: number;
  tickSubscriptionIds: Map<string, number>;
  retiredTickSubscriptionIds: Set<number>;
  tickArraysAToB: TickArray[];
  tickArraysBToA: TickArray[];
  tickAccountData: Map<string, Buffer>;
  tickCacheAtMs: number;
  tickRefresh?: Promise<void>;
  stateEmitter: DebouncedStateEmitter;
}

export interface OrcaWhirlpoolSimulationState {
  pool_id: string;
  label: string;
  slot: number;
  core_state_slot: number;
  dependency_slot_min: number | null;
  dependency_slot_max: number | null;
  dependency_generation: number;
  token_a_mint: string;
  token_b_mint: string;
  token_a_decimals: number;
  token_b_decimals: number;
  sqrt_price_x64: string;
  liquidity_raw: string;
  tick_current_index: number;
  tick_spacing: number;
  fee_rate: number;
  protocol_fee_rate: number;
  fee_growth_global_a: string;
  fee_growth_global_b: string;
  protocol_fee_owed_a: string;
  protocol_fee_owed_b: string;
  tick_arrays: readonly {
    start_tick_index: number;
    ticks: readonly { initialized: boolean; liquidity_net_raw: string; liquidity_gross_raw: string }[];
  }[];
}

export class OrcaWhirlpoolQuoteEngine {
  private connection: Connection | null = null;
  private fetcher: WhirlpoolAccountFetcherInterface | null = null;
  private pools = new Map<string, AttachedPool>();
  private tickCacheMaxAgeMs = DEFAULT_TICK_CACHE_MAX_AGE_MS;
  private poolStateEmitMinIntervalMs = DEFAULT_POOL_STATE_EMIT_MIN_INTERVAL_MS;
  private coreRefreshAfterMs = DEFAULT_CORE_REFRESH_AFTER_MS;
  private refreshStaggerWindowMs = DEFAULT_REFRESH_STAGGER_WINDOW_MS;
  private closed = false;
  private refreshesStarted = 0;
  private refreshesCompleted = 0;
  private refreshesUnchanged = 0;
  private readonly maintenanceCursor = new FairMaintenanceCursor();
  private maintenanceDuePoolCount = 0;
  private maintenanceMaximumOverdueMs = 0;

  public constructor(
    private readonly callbacks: OrcaEngineCallbacks,
    private readonly runRpcJob: RunRpcJob = scheduleRpc,
  ) {}

  public async open(config: ConfigureMessage): Promise<void> {
    if (this.connection !== null) throw new Error("Orca quote engine is already open");
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
    this.fetcher = Orca.buildDefaultAccountFetcher(this.connection);
    for (const descriptor of config.orca_whirlpool_pools) {
      await this.attachPool(descriptor);
    }
  }

  public async close(): Promise<void> {
    this.closed = true;
    const connection = this.connection;
    if (connection !== null) {
      for (const pool of this.pools.values()) pool.stateEmitter.dispose();
      const subscriptions = [...this.pools.values()].flatMap((pool) => [
        pool.poolSubscriptionId,
        ...pool.tickSubscriptionIds.values(),
        ...pool.retiredTickSubscriptionIds,
      ]);
      await Promise.all(
        subscriptions
          .filter((id) => id >= 0)
          .map(async (id) => connection.removeAccountChangeListener(id)),
      );
    }
    this.pools.clear();
    this.fetcher = null;
    this.connection = null;
  }

  /** Compatibility hook: maintenance is stale-driven and starts at most one refresh. */
  public async refreshAllPoolStates(): Promise<void> {
    await this.maintainStalePools(performance.now());
  }

  public async maintainStalePools(nowMs = performance.now()): Promise<void> {
    const candidates = [...this.pools.values()];
    const overdue = candidates.map((candidate) => coreRefreshOverdueMs(
      candidate.provenance,
      `orca-whirlpool:${candidate.descriptor.pool_id}`,
      nowMs,
      this.coreRefreshAfterMs,
      this.refreshStaggerWindowMs,
    ));
    this.maintenanceDuePoolCount = overdue.filter((value) => value >= 0).length;
    this.maintenanceMaximumOverdueMs = overdue.reduce((maximum, value) => Math.max(maximum, value), 0);
    const pool = this.maintenanceCursor.select(
      candidates,
      (candidate) => candidate.descriptor.pool_id,
      (candidate) => !candidate.provenance.refreshInFlight && coreRefreshDue(
        candidate.provenance,
        `orca-whirlpool:${candidate.descriptor.pool_id}`,
        nowMs,
        this.coreRefreshAfterMs,
        this.refreshStaggerWindowMs,
      ),
    );
    if (pool !== undefined) await this.scheduleCoreRefresh(pool);
  }

  public async quote(request: QuoteRequestMessage): Promise<OrcaQuoteResult> {
    const pool = this.pools.get(request.pool_id);
    if (pool === undefined) return this.unavailable(request, "pool is not configured in this worker");
    if (request.minimum_state_slot !== undefined
      && pool.provenance.coreStateSlot < request.minimum_state_slot) {
      return {
        ...this.unavailable(request, "worker state is older than required minimum slot"),
        status: "stale_state",
        state_slot: pool.provenance.coreStateSlot,
        ...pool.provenance.fields(),
      };
    }
    const mintA = pool.data.tokenMintA.toBase58();
    const mintB = pool.data.tokenMintB.toBase58();
    const aToB = request.input_mint === mintA && request.output_mint === mintB;
    const bToA = request.input_mint === mintB && request.output_mint === mintA;
    if (!aToB && !bToA) {
      return this.unavailable(request, "input/output mint pair does not match configured pool");
    }
    try {
      await this.ensureTickCache(pool, false);
      const input = new BN(request.input_amount_raw, 10);
      const result = Orca.swapQuoteWithParams(
        {
          whirlpoolData: pool.data,
          tokenAmount: input,
          otherAmountThreshold: Orca.SwapUtils.getDefaultOtherAmountThreshold(true),
          sqrtPriceLimit: Orca.SwapUtils.getDefaultSqrtPriceLimit(aToB),
          aToB,
          amountSpecifiedIsInput: true,
          tickArrays: aToB ? pool.tickArraysAToB : pool.tickArraysBToA,
          oracleData: null,
          tokenExtensionCtx: pool.tokenExtensionContext,
        },
        Common.Percentage.fromFraction(0, 10_000),
      );
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
        output_amount_raw: result.estimatedAmountOut.toString(10),
        consumed_input_amount_raw: result.estimatedAmountIn.toString(10),
        pool_fee_raw: result.estimatedFeeAmount.toString(10),
        transfer_fee_input_raw: result.transferFee.deductingFromEstimatedAmountIn.toString(10),
        transfer_fee_output_raw: result.transferFee.deductedFromEstimatedAmountOut.toString(10),
        pool_fee_on_input: true,
        all_trade: result.estimatedAmountIn.eq(input),
        tick_cache_age_ms: Math.max(0, performance.now() - pool.tickCacheAtMs),
      };
    } catch (error) {
      void this.ensureTickCache(pool, true).catch(() => undefined);
      return this.unavailable(request, error instanceof Error ? error.message : String(error));
    }
  }

  private async attachPool(descriptor: PoolDescriptor): Promise<void> {
    const connection = this.requireConnection();
    const fetcher = this.requireFetcher();
    const address = new PublicKey(descriptor.pool_id);
    const account = await connection.getAccountInfoAndContext(address, "processed");
    if (account.value === null) throw new Error(`Orca pool account ${descriptor.pool_id} was not found`);
    const data = Orca.ParsableWhirlpool.parse(address, account.value);
    if (data === null) throw new Error(`Orca pool account ${descriptor.pool_id} could not be decoded`);
    if (Orca.PoolUtil.isInitializedWithAdaptiveFee(data)) {
      throw new Error(`Orca adaptive-fee pool ${descriptor.pool_id} is not supported by the local quote cache`);
    }
    const tokenExtensionContext = await Orca.TokenExtensionUtil.buildTokenExtensionContext(
      fetcher,
      data,
      Orca.IGNORE_CACHE,
    );
    const provenance = new PoolSlotProvenance(account.context.slot);
    let pool!: AttachedPool;
    pool = {
      descriptor,
      address,
      data,
      tokenExtensionContext,
      provenance,
      coreAccountData: Buffer.from(account.value.data),
      poolSubscriptionId: -1,
      tickSubscriptionIds: new Map(),
      retiredTickSubscriptionIds: new Set(),
      tickArraysAToB: [],
      tickArraysBToA: [],
      tickAccountData: new Map(),
      tickCacheAtMs: 0,
      stateEmitter: new DebouncedStateEmitter(
        () => this.emitPoolStateNow(pool),
        this.poolStateEmitMinIntervalMs,
      ),
    };
    pool.poolSubscriptionId = connection.onAccountChange(
      address,
      (updated, context) => this.updatePool(pool, updated, context),
      "processed",
    );
    this.pools.set(descriptor.pool_id, pool);
    await this.ensureTickCache(pool, true);
    this.requestPoolState(pool, "initial", "immediate");
  }

  private updatePool(pool: AttachedPool, account: AccountInfo<Buffer>, context: Context): void {
    if (context.slot <= pool.provenance.coreStateSlot) return;
    const data = this.decodePoolAccount(pool, account);
    pool.data = data;
    pool.coreAccountData = Buffer.from(account.data);
    if (!pool.provenance.acceptCore(context.slot)) return;
    this.requestPoolState(pool, `core:${context.slot}`, "immediate");
  }

  private updateTick(
    pool: AttachedPool,
    address: PublicKey,
    account: AccountInfo<Buffer>,
    context: Context,
  ): void {
    const key = address.toBase58();
    const previousSlot = pool.provenance.dependencySlot(key);
    if (previousSlot !== undefined && context.slot <= previousSlot) return;
    const data = this.decodeTickAccount(address, account);
    const previousData = pool.tickAccountData.get(key);
    const changed = previousData === undefined || !previousData.equals(account.data);
    if (!pool.provenance.acceptDependencyVersion(key, context.slot, changed)) return;
    const replace = (items: TickArray[]): TickArray[] => items.map((item) =>
      item.address.equals(address) ? { ...item, data } : item);
    pool.tickArraysAToB = replace(pool.tickArraysAToB);
    pool.tickArraysBToA = replace(pool.tickArraysBToA);
    pool.tickAccountData.set(key, Buffer.from(account.data));
    if (changed) {
      this.requestPoolState(
        pool,
        `dependency:${pool.provenance.dependencyGeneration}`,
        "debounced",
      );
    }
  }

  private async ensureTickCache(pool: AttachedPool, force: boolean): Promise<void> {
    if (
      !force
      && pool.tickArraysAToB.length > 0
      && pool.tickArraysBToA.length > 0
      && performance.now() - pool.tickCacheAtMs <= this.tickCacheMaxAgeMs
    ) return;
    if (pool.tickRefresh !== undefined) return pool.tickRefresh;
    pool.tickRefresh = this.refreshTicks(pool);
    try {
      await pool.tickRefresh;
    } finally {
      pool.tickRefresh = undefined;
    }
  }

  private async refreshTicks(pool: AttachedPool): Promise<void> {
    const connection = this.requireConnection();
    const fetcher = this.requireFetcher();
    const hadCache = pool.tickArraysAToB.length > 0 || pool.tickArraysBToA.length > 0;
    const coreSlotBefore = pool.provenance.coreStateSlot;
    const [aToB, bToA] = await Promise.all([
      Orca.SwapUtils.getTickArrays(
        pool.data.tickCurrentIndex,
        pool.data.tickSpacing,
        true,
        Orca.ORCA_WHIRLPOOL_PROGRAM_ID,
        pool.address,
        fetcher,
        Orca.IGNORE_CACHE,
      ),
      Orca.SwapUtils.getTickArrays(
        pool.data.tickCurrentIndex,
        pool.data.tickSpacing,
        false,
        Orca.ORCA_WHIRLPOOL_PROGRAM_ID,
        pool.address,
        fetcher,
        Orca.IGNORE_CACHE,
      ),
    ]);
    const current = new Map(
      [...aToB, ...bToA].map((item) => [item.address.toBase58(), item] as const),
    );
    if (current.size === 0) throw new Error("Orca returned no nearby tick arrays");
    await this.cleanupRetiredTickSubscriptions(pool, connection);
    if (pool.retiredTickSubscriptionIds.size > 0
      && [...current.keys()].some((address) => !pool.tickSubscriptionIds.has(address))) {
      throw new Error("Orca dependency listener cleanup is pending; refusing a duplicate subscription");
    }
    const addedSubscriptions: Array<readonly [string, number]> = [];
    for (const [address, item] of current) {
      if (!pool.tickSubscriptionIds.has(address)) {
        let subscriptionId = -1;
        subscriptionId = connection.onAccountChange(
          item.address,
          (updated, context) => {
            if (pool.tickSubscriptionIds.get(address) !== subscriptionId) return;
            this.updateTick(pool, item.address, updated, context);
          },
          "processed",
        );
        pool.tickSubscriptionIds.set(address, subscriptionId);
        addedSubscriptions.push([address, subscriptionId]);
      }
    }

    try {
      const addresses = [...current.values()].map((item) => item.address);
      const snapshot = await connection.getMultipleAccountsInfoAndContext(addresses, "processed");
      if (snapshot.value.length !== addresses.length) {
        throw new Error("Orca tick snapshot returned an incomplete account vector");
      }
      const staged = new Map<string, TickArray["data"]>();
      const stagedData = new Map<string, Buffer>();
      for (let index = 0; index < addresses.length; index += 1) {
        const address = addresses[index]!;
        const value = snapshot.value[index];
        if (value === null) throw new Error(`Orca tick account ${address.toBase58()} disappeared`);
        staged.set(address.toBase58(), this.decodeTickAccount(address, value));
        stagedData.set(address.toBase58(), Buffer.from(value.data));
      }
      if (pool.provenance.coreStateSlot !== coreSlotBefore) {
        throw new Error("Orca core changed during tick discovery; dependency refresh must retry");
      }
      const existing = new Map(
        [...pool.tickArraysAToB, ...pool.tickArraysBToA]
          .map((item) => [item.address.toBase58(), item.data] as const),
      );
      for (const address of current.keys()) {
        const currentSlot = pool.provenance.dependencySlot(address);
        if (currentSlot !== undefined && currentSlot >= snapshot.context.slot
          && !existing.has(address)) {
          throw new Error(`Orca tick ${address} has provenance without cached data`);
        }
      }

      const generationBefore = pool.provenance.dependencyGeneration;
      for (const [address, subscriptionId] of pool.tickSubscriptionIds) {
        if (current.has(address)) continue;
        pool.tickSubscriptionIds.delete(address);
        pool.retiredTickSubscriptionIds.add(subscriptionId);
        pool.tickAccountData.delete(address);
        pool.provenance.removeDependency(address);
      }
      const resolved = new Map<string, TickArray["data"]>();
      for (const address of current.keys()) {
        const currentSlot = pool.provenance.dependencySlot(address);
        if (currentSlot !== undefined && currentSlot >= snapshot.context.slot) {
          resolved.set(address, existing.get(address)!);
          continue;
        }
        const data = stagedData.get(address)!;
        const previousData = pool.tickAccountData.get(address);
        const changed = previousData === undefined || !previousData.equals(data);
        pool.provenance.acceptDependencyVersion(address, snapshot.context.slot, changed);
        pool.tickAccountData.set(address, data);
        resolved.set(address, staged.get(address)!);
      }
      const withResolvedData = (items: TickArray[]): TickArray[] => items.map((item) => ({
        ...item,
        data: resolved.get(item.address.toBase58())!,
      }));
      pool.tickArraysAToB = withResolvedData(aToB);
      pool.tickArraysBToA = withResolvedData(bToA);
      pool.tickCacheAtMs = performance.now();
      pool.provenance.noteDependencyRefresh();
      await this.cleanupRetiredTickSubscriptions(pool, connection);
      if (hadCache && pool.provenance.dependencyGeneration !== generationBefore) {
        this.requestPoolState(
          pool,
          `dependency:${pool.provenance.dependencyGeneration}`,
          "debounced",
        );
      }
    } catch (error) {
      await Promise.all(addedSubscriptions.map(async ([address, subscriptionId]) => {
        if (pool.tickSubscriptionIds.get(address) === subscriptionId) {
          pool.tickSubscriptionIds.delete(address);
          pool.retiredTickSubscriptionIds.add(subscriptionId);
        }
      }));
      await this.cleanupRetiredTickSubscriptions(pool, connection);
      throw error;
    }
  }

  private async cleanupRetiredTickSubscriptions(pool: AttachedPool, connection: Connection): Promise<void> {
    await Promise.all([...pool.retiredTickSubscriptionIds].map(async (subscriptionId) => {
      try {
        await connection.removeAccountChangeListener(subscriptionId);
        pool.retiredTickSubscriptionIds.delete(subscriptionId);
      } catch {
        // Callback membership guards make retired ids inert; retain for retry.
      }
    }));
  }

  private decodeTickAccount(address: PublicKey, account: AccountInfo<Buffer>): TickArray["data"] {
    const data = Orca.ParsableTickArray.parse(address, account);
    if (data === null) throw new Error(`Orca tick account ${address.toBase58()} could not be decoded`);
    return data;
  }

  private decodePoolAccount(pool: AttachedPool, account: AccountInfo<Buffer>): WhirlpoolData {
    const data = Orca.ParsableWhirlpool.parse(pool.address, account);
    if (data === null || Orca.PoolUtil.isInitializedWithAdaptiveFee(data)) {
      throw new Error(`Orca pool ${pool.descriptor.pool_id} could not be decoded or became unsupported`);
    }
    return data;
  }

  private async scheduleCoreRefresh(pool: AttachedPool): Promise<void> {
    if (this.closed || pool.provenance.refreshInFlight) return;
    pool.provenance.refreshInFlight = true;
    pool.provenance.refreshGeneration += 1;
    this.refreshesStarted += 1;
    try {
      await this.runRpcJob(
        {
          priority: "refresh",
          coalesceKey: `refresh:orca:${pool.descriptor.pool_id}`,
          description: `refresh Orca Whirlpool core ${pool.descriptor.pool_id}`,
        },
        async () => this.refreshCore(pool),
      );
      this.refreshesCompleted += 1;
    } finally {
      pool.provenance.refreshInFlight = false;
    }
  }

  private async refreshCore(pool: AttachedPool): Promise<void> {
    const account = await this.requireConnection().getAccountInfoAndContext(pool.address, "processed");
    if (account.value === null) throw new Error("Orca refresh returned a missing Whirlpool account");
    if (account.context.slot <= pool.provenance.coreStateSlot) {
      if (account.context.slot === pool.provenance.coreStateSlot) {
        pool.provenance.noteRpcRefresh();
        this.refreshesUnchanged += 1;
      }
      return;
    }
    const data = this.decodePoolAccount(pool, account.value);
    const changed = !pool.coreAccountData.equals(account.value.data);
    pool.data = data;
    pool.coreAccountData = Buffer.from(account.value.data);
    if (account.context.slot > pool.provenance.coreStateSlot) {
      pool.provenance.acceptCore(account.context.slot);
    }
    pool.provenance.noteRpcRefresh();
    if (changed) {
      this.requestPoolState(pool, `refresh:${account.context.slot}`, "immediate");
    } else {
      this.refreshesUnchanged += 1;
    }
  }

  private requestPoolState(
    pool: AttachedPool,
    _reason: string,
    mode: "immediate" | "debounced",
  ): void {
    const provenance = pool.provenance.fields();
    pool.stateEmitter.request([
      provenance.core_state_slot,
      provenance.dependency_slot_min ?? "",
      provenance.dependency_slot_max ?? "",
      provenance.dependency_generation,
      pool.data.tickCurrentIndex,
      pool.data.sqrtPrice.toString(10),
      pool.data.liquidity.toString(10),
    ].join(":"), mode);
  }

  private emitPoolStateNow(pool: AttachedPool): void {
    if (this.closed) return;
    this.callbacks.onPoolState({
      pool_id: pool.descriptor.pool_id,
      label: pool.descriptor.label,
      slot: pool.provenance.coreStateSlot,
      ...pool.provenance.fields(),
      tick_current: pool.data.tickCurrentIndex,
      sqrt_price_x64: pool.data.sqrtPrice.toString(10),
      token_a_mint: pool.data.tokenMintA.toBase58(),
      token_b_mint: pool.data.tokenMintB.toBase58(),
      token_a_decimals: pool.tokenExtensionContext.tokenMintWithProgramA.decimals,
      token_b_decimals: pool.tokenExtensionContext.tokenMintWithProgramB.decimals,
      tick_spacing: pool.data.tickSpacing,
      fee_rate_millionths: pool.data.feeRate,
      tick_cache_age_ms: pool.tickCacheAtMs === 0
        ? null
        : Math.max(0, performance.now() - pool.tickCacheAtMs),
      ...pool.provenance.fields(),
    });
  }

  public runtimeStats(): Record<string, number> {
    let dependencyEmitsCoalesced = 0;
    let externalEmits = 0;
    let refreshInFlight = 0;
    let retiredSubscriptions = 0;
    for (const pool of this.pools.values()) {
      const emitter = pool.stateEmitter.stats();
      dependencyEmitsCoalesced += emitter.coalesced_total;
      externalEmits += emitter.external_emits_total;
      refreshInFlight += pool.provenance.refreshInFlight ? 1 : 0;
      retiredSubscriptions += pool.retiredTickSubscriptionIds.size;
    }
    return {
      pool_count: this.pools.size,
      dependency_emits_coalesced_total: dependencyEmitsCoalesced,
      external_pool_state_emits_total: externalEmits,
      refresh_inflight: refreshInFlight,
      refreshes_started_total: this.refreshesStarted,
      refreshes_completed_total: this.refreshesCompleted,
      refreshes_unchanged_total: this.refreshesUnchanged,
      maintenance_selected_total: this.maintenanceCursor.stats().selected_total,
      maintenance_due_pool_count: this.maintenanceDuePoolCount,
      maintenance_maximum_overdue_ms: this.maintenanceMaximumOverdueMs,
      retired_dependency_subscriptions: retiredSubscriptions,
    };
  }

  public orcaSimulationState(poolId: string): OrcaWhirlpoolSimulationState {
    const pool = this.pools.get(poolId);
    if (pool === undefined) throw new Error(`Orca pool ${poolId} is not configured`);
    const decimalsA = pool.tokenExtensionContext.tokenMintWithProgramA.decimals;
    const decimalsB = pool.tokenExtensionContext.tokenMintWithProgramB.decimals;
    const merge = (arrays: TickArray[]): { start_tick_index: number; ticks: { initialized: boolean; liquidity_net_raw: string; liquidity_gross_raw: string }[] }[] => {
      const seen = new Map<number, { start_tick_index: number; ticks: { initialized: boolean; liquidity_net_raw: string; liquidity_gross_raw: string }[] }>();
      for (const array of arrays) {
        if (array.data === null) continue;
        if (!seen.has(array.data.startTickIndex)) {
          seen.set(array.data.startTickIndex, {
            start_tick_index: array.data.startTickIndex,
            ticks: array.data.ticks.map((tick) => ({
              initialized: tick.initialized,
              liquidity_net_raw: tick.liquidityNet.toString(10),
              liquidity_gross_raw: tick.liquidityGross.toString(10),
            })),
          });
        }
      }
      return [...seen.values()].sort((x, y) => x.start_tick_index - y.start_tick_index);
    };
    return {
      pool_id: pool.descriptor.pool_id,
      label: pool.descriptor.label,
      slot: pool.provenance.coreStateSlot,
      ...pool.provenance.fields(),
      token_a_mint: pool.data.tokenMintA.toBase58(),
      token_b_mint: pool.data.tokenMintB.toBase58(),
      token_a_decimals: decimalsA,
      token_b_decimals: decimalsB,
      sqrt_price_x64: pool.data.sqrtPrice.toString(10),
      liquidity_raw: pool.data.liquidity.toString(10),
      tick_current_index: pool.data.tickCurrentIndex,
      tick_spacing: pool.data.tickSpacing,
      fee_rate: pool.data.feeRate,
      protocol_fee_rate: pool.data.protocolFeeRate,
      fee_growth_global_a: pool.data.feeGrowthGlobalA.toString(10),
      fee_growth_global_b: pool.data.feeGrowthGlobalB.toString(10),
      protocol_fee_owed_a: pool.data.protocolFeeOwedA.toString(10),
      protocol_fee_owed_b: pool.data.protocolFeeOwedB.toString(10),
      tick_arrays: merge([...pool.tickArraysAToB, ...pool.tickArraysBToA]),
    };
  }

  private unavailable(request: QuoteRequestMessage, error: string): OrcaQuoteResult {
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
    if (this.connection === null) throw new Error("Orca quote engine is not open");
    return this.connection;
  }

  private requireFetcher(): WhirlpoolAccountFetcherInterface {
    if (this.fetcher === null) throw new Error("Orca account fetcher is not open");
    return this.fetcher;
  }
}

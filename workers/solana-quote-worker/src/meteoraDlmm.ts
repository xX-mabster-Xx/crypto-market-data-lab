/** Read-only local exact-input Meteora DLMM quote engine. */

import { createRequire } from "node:module";
import { performance } from "node:perf_hooks";

import BN from "bn.js";
import type { BinArrayAccount, SwapQuote } from "@meteora-ag/dlmm";
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

const DEFAULT_BIN_CACHE_MAX_AGE_MS = 300_000;
const DEFAULT_POOL_STATE_EMIT_MIN_INTERVAL_MS = 100;
const DEFAULT_CORE_REFRESH_AFTER_MS = 15_000;
const DEFAULT_REFRESH_STAGGER_WINDOW_MS = 5_000;

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
  core_state_slot: number;
  dependency_slot_min: number | null;
  dependency_slot_max: number | null;
  dependency_generation: number;
}

export interface MeteoraQuoteResult {
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
  provenance: PoolSlotProvenance;
  coreAccountData: Buffer;
  poolSubscriptionId: number;
  binArrays: Map<string, BinArrayAccount>;
  binAccountData: Map<string, Buffer>;
  binSubscriptionIds: Map<string, number>;
  retiredBinSubscriptionIds: Set<number>;
  binCacheAtMs: number;
  binRefresh?: Promise<void>;
  stateEmitter: DebouncedStateEmitter;
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
    private readonly callbacks: MeteoraEngineCallbacks,
    private readonly runRpcJob: RunRpcJob = scheduleRpc,
  ) {}

  public async open(config: ConfigureMessage): Promise<void> {
    if (this.connection !== null) throw new Error("Meteora quote engine is already open");
    this.closed = false;
    this.binCacheMaxAgeMs = config.tick_cache_max_age_ms ?? DEFAULT_BIN_CACHE_MAX_AGE_MS;
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
    // Deliberately serial to stay below a free RPC's startup burst limit.
    for (const descriptor of config.meteora_dlmm_pools) {
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
        ...pool.binSubscriptionIds.values(),
        ...pool.retiredBinSubscriptionIds,
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

  /** Compatibility hook: maintenance is stale-driven and starts at most one refresh. */
  public async refreshAllPoolStates(): Promise<void> {
    await this.maintainStalePools(performance.now());
  }

  public async maintainStalePools(nowMs = performance.now()): Promise<void> {
    const candidates = [...this.pools.values()];
    const overdue = candidates.map((candidate) => coreRefreshOverdueMs(
      candidate.provenance,
      `meteora-dlmm:${candidate.descriptor.pool_id}`,
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
        `meteora-dlmm:${candidate.descriptor.pool_id}`,
        nowMs,
        this.coreRefreshAfterMs,
        this.refreshStaggerWindowMs,
      ),
    );
    if (pool !== undefined) await this.scheduleCoreRefresh(pool);
  }

  public async quote(request: QuoteRequestMessage): Promise<MeteoraQuoteResult> {
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
        state_slot: pool.provenance.coreStateSlot,
        ...pool.provenance.fields(),
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
        bin_cache_age_ms: Math.max(0, performance.now() - pool.binCacheAtMs),
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
    const provenance = new PoolSlotProvenance(account.context.slot);
    let pool!: AttachedPool;
    pool = {
      descriptor,
      address: publicKey,
      dlmm,
      provenance,
      coreAccountData: Buffer.from(account.value.data),
      poolSubscriptionId: -1,
      binArrays: new Map(),
      binAccountData: new Map(),
      binSubscriptionIds: new Map(),
      retiredBinSubscriptionIds: new Set(),
      binCacheAtMs: 0,
      stateEmitter: new DebouncedStateEmitter(
        () => this.emitPoolStateNow(pool),
        this.poolStateEmitMinIntervalMs,
      ),
    };
    pool.poolSubscriptionId = connection.onAccountChange(
      publicKey,
      (updated, context) => this.updatePair(pool, updated, context),
      "processed",
    );
    this.pools.set(descriptor.pool_id, pool);
    await this.ensureBinCache(pool, true);
    this.requestPoolState(pool, "initial", "immediate");
  }

  private updatePair(pool: AttachedPool, account: AccountInfo<Buffer>, context: Context): void {
    if (context.slot <= pool.provenance.coreStateSlot) return;
    const pair = this.decodePairAccount(pool, account);
    pool.dlmm.lbPair = pair;
    pool.coreAccountData = Buffer.from(account.data);
    if (!pool.provenance.acceptCore(context.slot)) return;
    this.requestPoolState(pool, `core:${context.slot}`, "immediate");
  }

  private updateBin(
    pool: AttachedPool,
    publicKey: PublicKey,
    account: AccountInfo<Buffer>,
    context: Context,
  ): void {
    const key = publicKey.toBase58();
    const previousSlot = pool.provenance.dependencySlot(key);
    if (previousSlot !== undefined && context.slot <= previousSlot) return;
    const decoded = this.decodeBinAccount(pool, account);
    const previousData = pool.binAccountData.get(key);
    const changed = previousData === undefined || !previousData.equals(account.data);
    if (!pool.provenance.acceptDependencyVersion(key, context.slot, changed)) return;
    pool.binArrays.set(publicKey.toBase58(), {
      publicKey,
      account: decoded,
    });
    pool.binAccountData.set(key, Buffer.from(account.data));
    if (changed) {
      this.requestPoolState(
        pool,
        `dependency:${pool.provenance.dependencyGeneration}`,
        "debounced",
      );
    }
  }

  private async ensureBinCache(pool: AttachedPool, force: boolean): Promise<void> {
    if (!force && pool.binArrays.size > 0 && performance.now() - pool.binCacheAtMs <= this.binCacheMaxAgeMs) {
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
    const hadCache = pool.binArrays.size > 0;
    const coreSlotBefore = pool.provenance.coreStateSlot;
    const [forY, forX] = await Promise.all([
      pool.dlmm.getBinArrayForSwap(true, 4),
      pool.dlmm.getBinArrayForSwap(false, 4),
    ]);
    const discovered = new Map<string, PublicKey>();
    for (const item of [...forY, ...forX]) discovered.set(item.publicKey.toBase58(), item.publicKey);
    if (discovered.size === 0) throw new Error("Meteora returned no nearby bin arrays");

    await this.cleanupRetiredBinSubscriptions(pool, connection);
    if (pool.retiredBinSubscriptionIds.size > 0
      && [...discovered.keys()].some((address) => !pool.binSubscriptionIds.has(address))) {
      throw new Error("Meteora dependency listener cleanup is pending; refusing a duplicate subscription");
    }
    const addedSubscriptions: Array<readonly [string, number]> = [];
    for (const [address, publicKey] of discovered) {
      if (!pool.binSubscriptionIds.has(address)) {
        let subscriptionId = -1;
        subscriptionId = connection.onAccountChange(
          publicKey,
          (updated, context) => {
            if (pool.binSubscriptionIds.get(address) !== subscriptionId) return;
            this.updateBin(pool, publicKey, updated, context);
          },
          "processed",
        );
        pool.binSubscriptionIds.set(address, subscriptionId);
        addedSubscriptions.push([address, subscriptionId]);
      }
    }

    try {
      const addresses = [...discovered.values()];
      const snapshot = await connection.getMultipleAccountsInfoAndContext(addresses, "processed");
      if (snapshot.value.length !== addresses.length) {
        throw new Error("Meteora bin snapshot returned an incomplete account vector");
      }
      const staged = new Map<string, BinArrayAccount>();
      const stagedData = new Map<string, Buffer>();
      for (let index = 0; index < addresses.length; index += 1) {
        const publicKey = addresses[index]!;
        const value = snapshot.value[index];
        if (value === null) throw new Error(`Meteora bin account ${publicKey.toBase58()} disappeared`);
        staged.set(publicKey.toBase58(), {
          publicKey,
          account: this.decodeBinAccount(pool, value),
        });
        stagedData.set(publicKey.toBase58(), Buffer.from(value.data));
      }
      if (pool.provenance.coreStateSlot !== coreSlotBefore) {
        throw new Error("Meteora core changed during bin discovery; dependency refresh must retry");
      }
      for (const address of discovered.keys()) {
        const currentSlot = pool.provenance.dependencySlot(address);
        if (currentSlot !== undefined && currentSlot >= snapshot.context.slot
          && !pool.binArrays.has(address)) {
          throw new Error(`Meteora bin ${address} has provenance without cached data`);
        }
      }

      const generationBefore = pool.provenance.dependencyGeneration;
      const next = new Map(pool.binArrays);
      for (const [address, subscriptionId] of pool.binSubscriptionIds) {
        if (discovered.has(address)) continue;
        pool.binSubscriptionIds.delete(address);
        pool.retiredBinSubscriptionIds.add(subscriptionId);
        next.delete(address);
        pool.binAccountData.delete(address);
        pool.provenance.removeDependency(address);
      }
      for (const address of discovered.keys()) {
        const currentSlot = pool.provenance.dependencySlot(address);
        if (currentSlot !== undefined && currentSlot >= snapshot.context.slot) continue;
        const data = stagedData.get(address)!;
        const previousData = pool.binAccountData.get(address);
        const changed = previousData === undefined || !previousData.equals(data);
        pool.provenance.acceptDependencyVersion(address, snapshot.context.slot, changed);
        next.set(address, staged.get(address)!);
        pool.binAccountData.set(address, data);
      }
      pool.binArrays = next;
      pool.binCacheAtMs = performance.now();
      pool.provenance.noteDependencyRefresh();
      await this.cleanupRetiredBinSubscriptions(pool, connection);
      if (hadCache && pool.provenance.dependencyGeneration !== generationBefore) {
        this.requestPoolState(
          pool,
          `dependency:${pool.provenance.dependencyGeneration}`,
          "debounced",
        );
      }
    } catch (error) {
      await Promise.all(addedSubscriptions.map(async ([address, subscriptionId]) => {
        if (pool.binSubscriptionIds.get(address) === subscriptionId) {
          pool.binSubscriptionIds.delete(address);
          pool.retiredBinSubscriptionIds.add(subscriptionId);
        }
      }));
      await this.cleanupRetiredBinSubscriptions(pool, connection);
      throw error;
    }
  }

  private async cleanupRetiredBinSubscriptions(pool: AttachedPool, connection: Connection): Promise<void> {
    await Promise.all([...pool.retiredBinSubscriptionIds].map(async (subscriptionId) => {
      try {
        await connection.removeAccountChangeListener(subscriptionId);
        pool.retiredBinSubscriptionIds.delete(subscriptionId);
      } catch {
        // Keep the id bounded by the discovered account universe and retry on
        // the next refresh or engine close. Its callback is already inactive.
      }
    }));
  }

  private decodeBinAccount(pool: AttachedPool, account: AccountInfo<Buffer>): BinArrayAccount["account"] {
    return MeteoraSdk.decodeAccount(
      pool.dlmm.program,
      "binArray",
      account.data,
    ) as BinArrayAccount["account"];
  }

  private decodePairAccount(pool: AttachedPool, account: AccountInfo<Buffer>): DlmmClient["lbPair"] {
    return MeteoraSdk.decodeAccount(
      pool.dlmm.program,
      "lbPair",
      account.data,
    ) as DlmmClient["lbPair"];
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
          coalesceKey: `refresh:meteora:${pool.descriptor.pool_id}`,
          description: `refresh Meteora DLMM core ${pool.descriptor.pool_id}`,
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
    if (account.value === null) throw new Error("Meteora refresh returned a missing LB-pair account");
    if (account.context.slot <= pool.provenance.coreStateSlot) {
      if (account.context.slot === pool.provenance.coreStateSlot) {
        pool.provenance.noteRpcRefresh();
        this.refreshesUnchanged += 1;
      }
      return;
    }
    const changed = !pool.coreAccountData.equals(account.value.data);
    pool.dlmm.lbPair = this.decodePairAccount(pool, account.value);
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
      pool.dlmm.lbPair.activeId,
      pool.dlmm.lbPair.binStep,
    ].join(":"), mode);
  }

  private emitPoolStateNow(pool: AttachedPool): void {
    if (this.closed) return;
    this.callbacks.onPoolState({
      pool_id: pool.descriptor.pool_id,
      label: pool.descriptor.label,
      slot: pool.provenance.coreStateSlot,
      active_id: pool.dlmm.lbPair.activeId,
      token_a_mint: pool.dlmm.tokenX.mint.address.toBase58(),
      token_b_mint: pool.dlmm.tokenY.mint.address.toBase58(),
      token_a_decimals: pool.dlmm.tokenX.mint.decimals,
      token_b_decimals: pool.dlmm.tokenY.mint.decimals,
      bin_step: pool.dlmm.lbPair.binStep,
      bin_cache_age_ms: pool.binCacheAtMs === 0
        ? null
        : Math.max(0, performance.now() - pool.binCacheAtMs),
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
      retiredSubscriptions += pool.retiredBinSubscriptionIds.size;
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

  private unavailable(request: QuoteRequestMessage, error: string): MeteoraQuoteResult {
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
    if (this.connection === null) throw new Error("Meteora quote engine is not open");
    return this.connection;
  }
}

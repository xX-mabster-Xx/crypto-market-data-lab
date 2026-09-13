/** Read-only local exact-input Orca Whirlpool quote engine. */

import { createRequire } from "node:module";

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
import { sharedRpcFetchMiddleware } from "./rpcPacer.js";

const DEFAULT_TICK_CACHE_MAX_AGE_MS = 300_000;

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
}

export interface OrcaQuoteResult {
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
  slot: number;
  poolSubscriptionId: number;
  tickSubscriptionIds: Map<string, number>;
  tickArraysAToB: TickArray[];
  tickArraysBToA: TickArray[];
  tickCacheAtMs: number;
  tickRefresh?: Promise<void>;
}

export interface OrcaWhirlpoolSimulationState {
  pool_id: string;
  label: string;
  slot: number;
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

  public constructor(private readonly callbacks: OrcaEngineCallbacks) {}

  public async open(config: ConfigureMessage): Promise<void> {
    if (this.connection !== null) throw new Error("Orca quote engine is already open");
    this.tickCacheMaxAgeMs = config.tick_cache_max_age_ms ?? DEFAULT_TICK_CACHE_MAX_AGE_MS;
    this.connection = new Connection(config.rpc_http_url, {
      commitment: "processed",
      wsEndpoint: config.rpc_ws_url,
      fetchMiddleware: sharedRpcFetchMiddleware,
      confirmTransactionInitialTimeout: 10_000,
    });
    this.fetcher = Orca.buildDefaultAccountFetcher(this.connection);
    for (const descriptor of config.orca_whirlpool_pools) {
      await this.attachPool(descriptor);
    }
  }

  public async close(): Promise<void> {
    const connection = this.connection;
    if (connection !== null) {
      const subscriptions = [...this.pools.values()].flatMap((pool) => [
        pool.poolSubscriptionId,
        ...pool.tickSubscriptionIds.values(),
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

  /** Reconcile all Whirlpool accounts in one current RPC snapshot. */
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
        throw new Error("Orca refresh returned a missing Whirlpool account");
      }
      if (snapshot.context.slot < pool.slot) continue;
      const data = Orca.ParsableWhirlpool.parse(pool.address, account);
      if (data === null || Orca.PoolUtil.isInitializedWithAdaptiveFee(data)) {
        throw new Error(`Orca pool ${pool.descriptor.pool_id} became unsupported during refresh`);
      }
      pool.data = data;
      pool.slot = snapshot.context.slot;
      this.emitPoolState(pool);
    }
  }

  public async quote(request: QuoteRequestMessage): Promise<OrcaQuoteResult> {
    const pool = this.pools.get(request.pool_id);
    if (pool === undefined) return this.unavailable(request, "pool is not configured in this worker");
    if (request.minimum_state_slot !== undefined && pool.slot < request.minimum_state_slot) {
      return {
        ...this.unavailable(request, "worker state is older than required minimum slot"),
        status: "stale_state",
        state_slot: pool.slot,
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
        state_slot: pool.slot,
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
        tick_cache_age_ms: Math.max(0, Date.now() - pool.tickCacheAtMs),
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
    const pool: AttachedPool = {
      descriptor,
      address,
      data,
      tokenExtensionContext,
      slot: account.context.slot,
      poolSubscriptionId: -1,
      tickSubscriptionIds: new Map(),
      tickArraysAToB: [],
      tickArraysBToA: [],
      tickCacheAtMs: 0,
    };
    pool.poolSubscriptionId = connection.onAccountChange(
      address,
      (updated, context) => this.updatePool(pool, updated, context),
      "processed",
    );
    this.pools.set(descriptor.pool_id, pool);
    await this.ensureTickCache(pool, true);
    this.emitPoolState(pool);
  }

  private updatePool(pool: AttachedPool, account: AccountInfo<Buffer>, context: Context): void {
    if (context.slot < pool.slot) return;
    const data = Orca.ParsableWhirlpool.parse(pool.address, account);
    if (data === null || Orca.PoolUtil.isInitializedWithAdaptiveFee(data)) return;
    pool.data = data;
    pool.slot = context.slot;
    this.emitPoolState(pool);
  }

  private updateTick(
    pool: AttachedPool,
    address: PublicKey,
    account: AccountInfo<Buffer>,
    context: Context,
  ): void {
    const data = Orca.ParsableTickArray.parse(address, account);
    if (data === null) return;
    const replace = (items: TickArray[]): TickArray[] => items.map((item) =>
      item.address.equals(address) ? { ...item, data } : item);
    pool.tickArraysAToB = replace(pool.tickArraysAToB);
    pool.tickArraysBToA = replace(pool.tickArraysBToA);
    pool.slot = Math.max(pool.slot, context.slot);
    pool.tickCacheAtMs = Date.now();
    this.emitPoolState(pool);
  }

  private async ensureTickCache(pool: AttachedPool, force: boolean): Promise<void> {
    if (
      !force
      && pool.tickArraysAToB.length > 0
      && pool.tickArraysBToA.length > 0
      && Date.now() - pool.tickCacheAtMs <= this.tickCacheMaxAgeMs
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
    for (const [address, subscriptionId] of pool.tickSubscriptionIds) {
      if (!current.has(address)) {
        await connection.removeAccountChangeListener(subscriptionId);
        pool.tickSubscriptionIds.delete(address);
      }
    }
    for (const [address, item] of current) {
      if (!pool.tickSubscriptionIds.has(address)) {
        const subscriptionId = connection.onAccountChange(
          item.address,
          (updated, context) => this.updateTick(pool, item.address, updated, context),
          "processed",
        );
        pool.tickSubscriptionIds.set(address, subscriptionId);
      }
    }
    pool.tickArraysAToB = aToB;
    pool.tickArraysBToA = bToA;
    pool.tickCacheAtMs = Date.now();
  }

  private emitPoolState(pool: AttachedPool): void {
    this.callbacks.onPoolState({
      pool_id: pool.descriptor.pool_id,
      label: pool.descriptor.label,
      slot: pool.slot,
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
        : Math.max(0, Date.now() - pool.tickCacheAtMs),
    });
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
      slot: pool.slot,
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
      state_slot: pool?.slot ?? 0,
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

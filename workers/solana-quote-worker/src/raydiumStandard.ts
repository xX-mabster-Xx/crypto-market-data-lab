/** Read-only local exact-input Raydium CPMM and legacy AMM-v4 quote engine. */

import BN from "bn.js";
import { performance } from "node:perf_hooks";
import {
  Connection,
  type AccountInfo,
  type Context,
  PublicKey,
} from "@solana/web3.js";
import {
  AMM_V4,
  CREATE_CPMM_POOL_PROGRAM,
  CpmmConfigInfoLayout,
  CpmmPoolInfoLayout,
  CurveCalculator,
  LIQUIDITY_FEES_DENOMINATOR,
  LIQUIDITY_FEES_NUMERATOR,
  liquidityStateV4Layout,
  splAccountLayout,
} from "@raydium-io/raydium-sdk-v2";

import type {
  ConfigureMessage,
  QuoteRequestMessage,
  RaydiumStandardPoolDescriptor,
} from "./protocol.js";
import {
  coreRefreshDue,
  DebouncedStateEmitter,
  PoolSlotProvenance,
  type RunRpcJob,
} from "./engineRuntime.js";
import { scheduleRpc, sharedRpcFetch } from "./rpcPacer.js";

const TOKEN_PROGRAM_ID = new PublicKey("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
const SLOT_DEBOUNCE_MS = 25;
const MAX_PENDING_SLOTS = 16;
const DEFAULT_POOL_STATE_EMIT_MIN_INTERVAL_MS = 100;
const DEFAULT_CORE_REFRESH_AFTER_MS = 15_000;
const DEFAULT_REFRESH_STAGGER_WINDOW_MS = 5_000;

type AmmState = ReturnType<typeof liquidityStateV4Layout.decode>;
type CpmmState = ReturnType<typeof CpmmPoolInfoLayout.decode>;
type CpmmConfig = ReturnType<typeof CpmmConfigInfoLayout.decode>;
type StandardProtocol = "raydium_amm_v4" | "raydium_cpmm";

export interface RaydiumStandardPoolStateNotice {
  pool_id: string;
  label: string;
  slot: number;
  token_a_mint: string;
  token_b_mint: string;
  token_a_decimals: number;
  token_b_decimals: number;
  reserve_a_raw: string;
  reserve_b_raw: string;
  trade_fee_numerator: string;
  trade_fee_denominator: string;
  core_state_slot: number;
  dependency_slot_min: number | null;
  dependency_slot_max: number | null;
  dependency_generation: number;
}

export interface RaydiumStandardQuoteResult {
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
  fund_fee_raw?: string;
  creator_fee_raw?: string;
  pool_fee_on_input?: boolean;
  price_impact_pct?: string;
  all_trade?: boolean;
  error?: string;
}

export interface RaydiumStandardEngineCallbacks {
  onPoolState: (protocol: StandardProtocol, notice: RaydiumStandardPoolStateNotice) => void;
}

export interface RaydiumCpmmSimulationState {
  pool_id: string;
  label: string;
  slot: number;
  core_state_slot?: number;
  dependency_slot_min?: number | null;
  dependency_slot_max?: number | null;
  dependency_generation?: number;
  token_a_mint: string;
  token_b_mint: string;
  token_a_decimals: number;
  token_b_decimals: number;
  reserve_a_raw: string;
  reserve_b_raw: string;
  vault_a_raw: string;
  vault_b_raw: string;
  protocol_fees_a_raw: string;
  protocol_fees_b_raw: string;
  fund_fees_a_raw: string;
  fund_fees_b_raw: string;
  creator_fees_a_raw: string;
  creator_fees_b_raw: string;
  fee_on: 0 | 1 | 2;
  trade_fee_rate: string;
  creator_fee_rate: string;
  protocol_fee_rate: string;
  fund_fee_rate: string;
}

interface PendingSlot {
  pool?: AccountInfo<Buffer>;
  vaultA?: AccountInfo<Buffer>;
  vaultB?: AccountInfo<Buffer>;
  timer?: ReturnType<typeof setTimeout>;
}

interface PoolCommon {
  descriptor: RaydiumStandardPoolDescriptor;
  address: PublicKey;
  vaultAAddress: PublicKey;
  vaultBAddress: PublicKey;
  vaultAAmount: BN;
  vaultBAmount: BN;
  provenance: PoolSlotProvenance;
  subscriptionIds: number[];
  pending: Map<number, PendingSlot>;
  stateEmitter: DebouncedStateEmitter;
}

interface AmmPool extends PoolCommon {
  protocol: "raydium_amm_v4";
  state: AmmState;
}

interface CpmmPool extends PoolCommon {
  protocol: "raydium_cpmm";
  state: CpmmState;
  config: CpmmConfig;
  configAddress: PublicKey;
}

type AttachedPool = AmmPool | CpmmPool;

interface InitialShape {
  descriptor: RaydiumStandardPoolDescriptor;
  address: PublicKey;
  protocol: StandardProtocol;
  vaultAAddress: PublicKey;
  vaultBAddress: PublicKey;
  configAddress?: PublicKey;
}

function decodeVault(account: AccountInfo<Buffer>, expectedMint: PublicKey): BN {
  if (!account.owner.equals(TOKEN_PROGRAM_ID)) {
    throw new Error("Raydium standard pool uses an unsupported Token-2022 vault");
  }
  const decoded = splAccountLayout.decode(account.data);
  if (!decoded.mint.equals(expectedMint)) throw new Error("Raydium vault mint does not match pool state");
  return new BN(decoded.amount.toString(), 10);
}

function ceilDiv(numerator: BN, denominator: BN): BN {
  if (denominator.lten(0)) throw new Error("fee denominator must be positive");
  if (numerator.isZero()) return new BN(0);
  return numerator.add(denominator).subn(1).div(denominator);
}

function percentImpact(inputAfterFee: BN, output: BN, reserveIn: BN, reserveOut: BN): string {
  const idealNumerator = inputAfterFee.mul(reserveOut);
  const actualNumerator = output.mul(reserveIn);
  if (idealNumerator.isZero() || actualNumerator.gte(idealNumerator)) return "0.00000000";
  const scale = new BN("10000000000", 10); // percent with eight fractional digits
  const raw = idealNumerator.sub(actualNumerator).mul(scale).div(idealNumerator).toString(10);
  return `${raw.slice(0, -8) || "0"}.${raw.slice(-8).padStart(8, "0")}`;
}

/**
 * Uses two bounded RPC snapshots at startup, then only accountSubscribe push.
 * Pool and both vault writes are committed together per slot after a tiny
 * debounce, preventing transient mixed-reserve quotes from being published.
 */
export class RaydiumStandardQuoteEngine {
  private connection: Connection | null = null;
  private pools = new Map<string, AttachedPool>();
  private poolStateEmitMinIntervalMs = DEFAULT_POOL_STATE_EMIT_MIN_INTERVAL_MS;
  private coreRefreshAfterMs = DEFAULT_CORE_REFRESH_AFTER_MS;
  private refreshStaggerWindowMs = DEFAULT_REFRESH_STAGGER_WINDOW_MS;
  private closed = false;
  private refreshesStarted = 0;
  private refreshesCompleted = 0;
  private refreshesUnchanged = 0;

  public constructor(
    private readonly callbacks: RaydiumStandardEngineCallbacks,
    private readonly runRpcJob: RunRpcJob = scheduleRpc,
  ) {}

  public cpmmSimulationState(poolId: string): RaydiumCpmmSimulationState {
    const pool = this.pools.get(poolId);
    if (pool === undefined || pool.protocol !== "raydium_cpmm") {
      throw new Error("Raydium CPMM simulation state is not available");
    }
    const [decimalsA, decimalsB] = this.decimals(pool);
    return {
      pool_id: pool.descriptor.pool_id,
      label: pool.descriptor.label,
      slot: pool.provenance.coreStateSlot,
      ...pool.provenance.fields(),
      token_a_mint: pool.state.mintA.toBase58(),
      token_b_mint: pool.state.mintB.toBase58(),
      token_a_decimals: decimalsA,
      token_b_decimals: decimalsB,
      reserve_a_raw: this.reserves(pool)[0].toString(10),
      reserve_b_raw: this.reserves(pool)[1].toString(10),
      vault_a_raw: pool.vaultAAmount.toString(10),
      vault_b_raw: pool.vaultBAmount.toString(10),
      protocol_fees_a_raw: pool.state.protocolFeesMintA.toString(10),
      protocol_fees_b_raw: pool.state.protocolFeesMintB.toString(10),
      fund_fees_a_raw: pool.state.fundFeesMintA.toString(10),
      fund_fees_b_raw: pool.state.fundFeesMintB.toString(10),
      creator_fees_a_raw: pool.state.creatorFeesMintA.toString(10),
      creator_fees_b_raw: pool.state.creatorFeesMintB.toString(10),
      fee_on: pool.state.feeOn as 0 | 1 | 2,
      trade_fee_rate: pool.config.tradeFeeRate.toString(10),
      creator_fee_rate: pool.config.creatorFeeRate.toString(10),
      protocol_fee_rate: pool.config.protocolFeeRate.toString(10),
      fund_fee_rate: pool.config.fundFeeRate.toString(10),
    };
  }

  /** Emit a full immutable CPMM snapshot bundle for the requested pool ids. */
  public cpmmSimulationSnapshot(poolIds: readonly string[]): RaydiumCpmmSimulationState[] {
    return poolIds.map((poolId) => this.cpmmSimulationState(poolId));
  }

  public async open(config: ConfigureMessage): Promise<void> {
    if (this.connection !== null) throw new Error("Raydium standard quote engine is already open");
    this.closed = false;
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
    if (config.raydium_standard_pools.length === 0) return;

    const connection = this.requireConnection();
    const descriptors = config.raydium_standard_pools;
    const addresses = descriptors.map((item) => new PublicKey(item.pool_id));
    const identification = await connection.getMultipleAccountsInfoAndContext(addresses, "processed");
    const shapes = descriptors.map((descriptor, index): InitialShape => {
      const address = addresses[index];
      const account = identification.value[index];
      if (account === null) throw new Error(`Raydium standard pool ${descriptor.pool_id} was not found`);
      if (account.owner.equals(AMM_V4)) {
        if (descriptor.protocol !== "raydium_amm_v4") {
          throw new Error(`Raydium pool ${descriptor.pool_id} is AMM v4, not ${descriptor.protocol}`);
        }
        const state = liquidityStateV4Layout.decode(account.data);
        return {
          descriptor,
          address,
          protocol: "raydium_amm_v4",
          vaultAAddress: state.baseVault,
          vaultBAddress: state.quoteVault,
        };
      }
      if (account.owner.equals(CREATE_CPMM_POOL_PROGRAM)) {
        if (descriptor.protocol !== "raydium_cpmm") {
          throw new Error(`Raydium pool ${descriptor.pool_id} is CPMM, not ${descriptor.protocol}`);
        }
        const state = CpmmPoolInfoLayout.decode(account.data);
        if (!state.mintProgramA.equals(TOKEN_PROGRAM_ID) || !state.mintProgramB.equals(TOKEN_PROGRAM_ID)) {
          throw new Error(`Raydium CPMM ${descriptor.pool_id} uses Token-2022 transfer rules not yet modeled`);
        }
        return {
          descriptor,
          address,
          protocol: "raydium_cpmm",
          vaultAAddress: state.vaultA,
          vaultBAddress: state.vaultB,
          configAddress: state.configId,
        };
      }
      throw new Error(`Raydium standard pool ${descriptor.pool_id} has unsupported owner`);
    });

    // Fetch each pool again in the same RPC-context snapshot as its vaults.
    const snapshotKeys = shapes.flatMap((shape) => [
      shape.address,
      shape.vaultAAddress,
      shape.vaultBAddress,
      ...(shape.configAddress === undefined ? [] : [shape.configAddress]),
    ]);
    const snapshot = await connection.getMultipleAccountsInfoAndContext(snapshotKeys, "processed");
    let cursor = 0;
    for (const shape of shapes) {
      const poolAccount = snapshot.value[cursor++];
      const vaultAAccount = snapshot.value[cursor++];
      const vaultBAccount = snapshot.value[cursor++];
      const configAccount = shape.configAddress === undefined ? undefined : snapshot.value[cursor++];
      if (poolAccount === null || vaultAAccount === null || vaultBAccount === null) {
        throw new Error(`Raydium standard pool ${shape.descriptor.pool_id} has a missing state account`);
      }
      const attached = shape.protocol === "raydium_amm_v4"
        ? this.buildAmmPool(shape, poolAccount, vaultAAccount, vaultBAccount, snapshot.context.slot)
        : this.buildCpmmPool(shape, poolAccount, vaultAAccount, vaultBAccount, configAccount, snapshot.context.slot);
      this.subscribe(attached);
      this.pools.set(shape.descriptor.pool_id, attached);
      this.requestPoolState(attached, "initial", "immediate");
    }
  }

  public async close(): Promise<void> {
    this.closed = true;
    const connection = this.connection;
    if (connection !== null) {
      for (const pool of this.pools.values()) {
        pool.stateEmitter.dispose();
        for (const pending of pool.pending.values()) {
          if (pending.timer !== undefined) clearTimeout(pending.timer);
        }
      }
      await Promise.all(
        [...this.pools.values()].flatMap((pool) => pool.subscriptionIds)
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
    for (const pool of this.pools.values()) {
      if (pool.provenance.refreshInFlight || !coreRefreshDue(
        pool.provenance,
        `${pool.protocol}:${pool.descriptor.pool_id}`,
        nowMs,
        this.coreRefreshAfterMs,
        this.refreshStaggerWindowMs,
      )) continue;
      await this.scheduleCoreRefresh(pool);
      return;
    }
  }

  public async quote(request: QuoteRequestMessage): Promise<RaydiumStandardQuoteResult> {
    const pool = this.pools.get(request.pool_id);
    if (pool === undefined) return this.unavailable(request, "pool is not configured in this worker");
    if (request.protocol !== pool.protocol) {
      return this.unavailable(request, "request protocol does not match configured Raydium pool");
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
    const [mintA, mintB] = this.mints(pool);
    const aToB = request.input_mint === mintA.toBase58() && request.output_mint === mintB.toBase58();
    const bToA = request.input_mint === mintB.toBase58() && request.output_mint === mintA.toBase58();
    if (!aToB && !bToA) {
      return this.unavailable(request, "input/output mint pair does not match configured pool");
    }
    try {
      return pool.protocol === "raydium_amm_v4"
        ? this.quoteAmm(pool, request, aToB)
        : this.quoteCpmm(pool, request, aToB);
    } catch (error) {
      return this.unavailable(request, error instanceof Error ? error.message : String(error));
    }
  }

  private buildAmmPool(
    shape: InitialShape,
    poolAccount: AccountInfo<Buffer>,
    vaultAAccount: AccountInfo<Buffer>,
    vaultBAccount: AccountInfo<Buffer>,
    slot: number,
  ): AmmPool {
    const state = liquidityStateV4Layout.decode(poolAccount.data);
    const provenance = new PoolSlotProvenance(slot);
    provenance.acceptDependency("vault-a", slot);
    provenance.acceptDependency("vault-b", slot);
    let pool!: AmmPool;
    pool = {
      descriptor: shape.descriptor,
      protocol: "raydium_amm_v4",
      address: shape.address,
      state,
      vaultAAddress: state.baseVault,
      vaultBAddress: state.quoteVault,
      vaultAAmount: decodeVault(vaultAAccount, state.baseMint),
      vaultBAmount: decodeVault(vaultBAccount, state.quoteMint),
      provenance,
      subscriptionIds: [],
      pending: new Map(),
      stateEmitter: new DebouncedStateEmitter(
        () => this.emitPoolStateNow(pool),
        this.poolStateEmitMinIntervalMs,
      ),
    };
    return pool;
  }

  private buildCpmmPool(
    shape: InitialShape,
    poolAccount: AccountInfo<Buffer>,
    vaultAAccount: AccountInfo<Buffer>,
    vaultBAccount: AccountInfo<Buffer>,
    configAccount: AccountInfo<Buffer> | null | undefined,
    slot: number,
  ): CpmmPool {
    if (shape.configAddress === undefined || configAccount === undefined || configAccount === null) {
      throw new Error(`Raydium CPMM ${shape.descriptor.pool_id} has no fee-config account`);
    }
    const state = CpmmPoolInfoLayout.decode(poolAccount.data);
    const provenance = new PoolSlotProvenance(slot);
    provenance.acceptDependency("vault-a", slot);
    provenance.acceptDependency("vault-b", slot);
    provenance.acceptDependency("config", slot);
    let pool!: CpmmPool;
    pool = {
      descriptor: shape.descriptor,
      protocol: "raydium_cpmm",
      address: shape.address,
      state,
      config: CpmmConfigInfoLayout.decode(configAccount.data),
      configAddress: shape.configAddress,
      vaultAAddress: state.vaultA,
      vaultBAddress: state.vaultB,
      vaultAAmount: decodeVault(vaultAAccount, state.mintA),
      vaultBAmount: decodeVault(vaultBAccount, state.mintB),
      provenance,
      subscriptionIds: [],
      pending: new Map(),
      stateEmitter: new DebouncedStateEmitter(
        () => this.emitPoolStateNow(pool),
        this.poolStateEmitMinIntervalMs,
      ),
    };
    return pool;
  }

  private subscribe(pool: AttachedPool): void {
    const connection = this.requireConnection();
    pool.subscriptionIds.push(
      connection.onAccountChange(
        pool.address,
        (account, context) => this.stage(pool, "pool", account, context),
        "processed",
      ),
      connection.onAccountChange(
        pool.vaultAAddress,
        (account, context) => this.stage(pool, "vaultA", account, context),
        "processed",
      ),
      connection.onAccountChange(
        pool.vaultBAddress,
        (account, context) => this.stage(pool, "vaultB", account, context),
        "processed",
      ),
    );
    if (pool.protocol === "raydium_cpmm") {
      pool.subscriptionIds.push(
        connection.onAccountChange(
          pool.configAddress,
          (account, context) => {
            if (!pool.provenance.acceptDependency("config", context.slot)) return;
            pool.config = CpmmConfigInfoLayout.decode(account.data);
            this.requestPoolState(
              pool,
              `dependency:${pool.provenance.dependencyGeneration}`,
              "debounced",
            );
          },
          "processed",
        ),
      );
    }
  }

  private stage(
    pool: AttachedPool,
    kind: "pool" | "vaultA" | "vaultB",
    account: AccountInfo<Buffer>,
    context: Context,
  ): void {
    if (context.slot < pool.provenance.coreStateSlot) return;
    const pending = pool.pending.get(context.slot) ?? {};
    pending[kind] = account;
    if (pending.pool !== undefined && pending.vaultA !== undefined && pending.vaultB !== undefined) {
      if (pending.timer !== undefined) clearTimeout(pending.timer);
      pending.timer = setTimeout(() => this.commit(pool, context.slot), SLOT_DEBOUNCE_MS);
    }
    pool.pending.set(context.slot, pending);
    while (pool.pending.size > MAX_PENDING_SLOTS) {
      const oldest = pool.pending.keys().next().value as number | undefined;
      if (oldest === undefined) break;
      const dropped = pool.pending.get(oldest);
      if (dropped?.timer !== undefined) clearTimeout(dropped.timer);
      pool.pending.delete(oldest);
    }
  }

  private commit(pool: AttachedPool, slot: number): void {
    const pending = pool.pending.get(slot);
    if (pending?.pool === undefined || pending.vaultA === undefined || pending.vaultB === undefined) return;
    if (slot <= pool.provenance.coreStateSlot) {
      pool.pending.delete(slot);
      return;
    }
    if (pool.protocol === "raydium_amm_v4") {
      const state = liquidityStateV4Layout.decode(pending.pool.data);
      if (!state.baseVault.equals(pool.vaultAAddress) || !state.quoteVault.equals(pool.vaultBAddress)) return;
      pool.state = state;
      pool.vaultAAmount = decodeVault(pending.vaultA, state.baseMint);
      pool.vaultBAmount = decodeVault(pending.vaultB, state.quoteMint);
    } else {
      const state = CpmmPoolInfoLayout.decode(pending.pool.data);
      if (!state.vaultA.equals(pool.vaultAAddress) || !state.vaultB.equals(pool.vaultBAddress)) return;
      pool.state = state;
      pool.vaultAAmount = decodeVault(pending.vaultA, state.mintA);
      pool.vaultBAmount = decodeVault(pending.vaultB, state.mintB);
    }
    if (!pool.provenance.acceptCore(slot)) return;
    pool.provenance.acceptDependency("vault-a", slot);
    pool.provenance.acceptDependency("vault-b", slot);
    for (const [pendingSlot, item] of pool.pending) {
      if (pendingSlot <= slot) {
        if (item.timer !== undefined) clearTimeout(item.timer);
        pool.pending.delete(pendingSlot);
      }
    }
    this.requestPoolState(pool, `core:${slot}`, "immediate");
  }

  private quoteAmm(
    pool: AmmPool,
    request: QuoteRequestMessage,
    aToB: boolean,
  ): RaydiumStandardQuoteResult {
    const [reserveA, reserveB] = this.reserves(pool);
    const [reserveIn, reserveOut] = aToB ? [reserveA, reserveB] : [reserveB, reserveA];
    const input = new BN(request.input_amount_raw, 10);
    const feeNumerator = pool.state.swapFeeNumerator.gt(new BN(0))
      ? pool.state.swapFeeNumerator
      : LIQUIDITY_FEES_NUMERATOR;
    const feeDenominator = pool.state.swapFeeDenominator.gt(feeNumerator)
      ? pool.state.swapFeeDenominator
      : LIQUIDITY_FEES_DENOMINATOR;
    const fee = ceilDiv(input.mul(feeNumerator), feeDenominator);
    const inputAfterFee = input.sub(fee);
    if (inputAfterFee.lten(0) || reserveIn.lten(0) || reserveOut.lten(0)) {
      throw new Error("Raydium AMM v4 has insufficient effective reserves");
    }
    const output = reserveOut.mul(inputAfterFee).div(reserveIn.add(inputAfterFee));
    if (output.lten(0) || output.gte(reserveOut)) throw new Error("Raydium AMM v4 quote has no output");
    return {
      ...this.okBase(pool, request),
      output_amount_raw: output.toString(10),
      consumed_input_amount_raw: input.toString(10),
      pool_fee_raw: fee.toString(10),
      pool_fee_on_input: true,
      price_impact_pct: percentImpact(inputAfterFee, output, reserveIn, reserveOut),
      all_trade: true,
    };
  }

  private quoteCpmm(
    pool: CpmmPool,
    request: QuoteRequestMessage,
    aToB: boolean,
  ): RaydiumStandardQuoteResult {
    const [reserveA, reserveB] = this.reserves(pool);
    const [reserveIn, reserveOut] = aToB ? [reserveA, reserveB] : [reserveB, reserveA];
    const input = new BN(request.input_amount_raw, 10);
    const creatorFeeOnInput = pool.state.feeOn === 0 || pool.state.feeOn === 2;
    const result = CurveCalculator.swapBaseInput(
      input,
      reserveIn,
      reserveOut,
      pool.config.tradeFeeRate,
      pool.config.creatorFeeRate,
      pool.config.protocolFeeRate,
      pool.config.fundFeeRate,
      creatorFeeOnInput,
    );
    if (result.outputAmount.lten(0)) throw new Error("Raydium CPMM quote has no output");
    return {
      ...this.okBase(pool, request),
      output_amount_raw: result.outputAmount.toString(10),
      consumed_input_amount_raw: result.inputAmount.toString(10),
      pool_fee_raw: result.tradeFee.toString(10),
      protocol_fee_raw: result.protocolFee.toString(10),
      fund_fee_raw: result.fundFee.toString(10),
      creator_fee_raw: result.creatorFee.toString(10),
      pool_fee_on_input: creatorFeeOnInput,
      all_trade: result.inputAmount.eq(input),
    };
  }

  private okBase(
    pool: AttachedPool,
    request: QuoteRequestMessage,
  ): Omit<RaydiumStandardQuoteResult, "output_amount_raw"> {
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
    };
  }

  private mints(pool: AttachedPool): [PublicKey, PublicKey] {
    return pool.protocol === "raydium_amm_v4"
      ? [pool.state.baseMint, pool.state.quoteMint]
      : [pool.state.mintA, pool.state.mintB];
  }

  private decimals(pool: AttachedPool): [number, number] {
    return pool.protocol === "raydium_amm_v4"
      ? [pool.state.baseDecimal.toNumber(), pool.state.quoteDecimal.toNumber()]
      : [pool.state.mintDecimalA, pool.state.mintDecimalB];
  }

  private reserves(pool: AttachedPool): [BN, BN] {
    if (pool.protocol === "raydium_amm_v4") {
      return [
        pool.vaultAAmount.sub(pool.state.baseNeedTakePnl),
        pool.vaultBAmount.sub(pool.state.quoteNeedTakePnl),
      ];
    }
    return [
      pool.vaultAAmount
        .sub(pool.state.protocolFeesMintA)
        .sub(pool.state.fundFeesMintA)
        .sub(pool.state.creatorFeesMintA),
      pool.vaultBAmount
        .sub(pool.state.protocolFeesMintB)
        .sub(pool.state.fundFeesMintB)
        .sub(pool.state.creatorFeesMintB),
    ];
  }

  private fee(pool: AttachedPool): [BN, BN] {
    if (pool.protocol === "raydium_cpmm") {
      return [pool.config.tradeFeeRate, new BN(1_000_000)];
    }
    const numerator = pool.state.swapFeeNumerator.gt(new BN(0))
      ? pool.state.swapFeeNumerator
      : LIQUIDITY_FEES_NUMERATOR;
    const denominator = pool.state.swapFeeDenominator.gt(numerator)
      ? pool.state.swapFeeDenominator
      : LIQUIDITY_FEES_DENOMINATOR;
    return [numerator, denominator];
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
          coalesceKey: `refresh:${pool.protocol}:${pool.descriptor.pool_id}`,
          description: `refresh ${pool.protocol} state ${pool.descriptor.pool_id}`,
        },
        async () => this.refreshCore(pool),
      );
      this.refreshesCompleted += 1;
    } finally {
      pool.provenance.refreshInFlight = false;
    }
  }

  private async refreshCore(pool: AttachedPool): Promise<void> {
    const connection = this.requireConnection();
    const keys = [
      pool.address,
      pool.vaultAAddress,
      pool.vaultBAddress,
      ...(pool.protocol === "raydium_cpmm" ? [pool.configAddress] : []),
    ];
    const snapshot = await connection.getMultipleAccountsInfoAndContext(keys, "processed");
    const [poolAccount, vaultAAccount, vaultBAccount, configAccount] = snapshot.value;
    if (poolAccount == null || vaultAAccount == null || vaultBAccount == null) {
      throw new Error(`Raydium standard refresh missed state for ${pool.descriptor.pool_id}`);
    }
    if (snapshot.context.slot <= pool.provenance.coreStateSlot) {
      if (snapshot.context.slot === pool.provenance.coreStateSlot) {
        pool.provenance.noteRpcRefresh();
        this.refreshesUnchanged += 1;
      }
      return;
    }
    const before = this.poolStateFingerprint(pool);
    if (pool.protocol === "raydium_amm_v4") {
      const state = liquidityStateV4Layout.decode(poolAccount.data);
      if (!state.baseVault.equals(pool.vaultAAddress) || !state.quoteVault.equals(pool.vaultBAddress)) {
        throw new Error(`Raydium AMM v4 ${pool.descriptor.pool_id} changed vault identity`);
      }
      pool.state = state;
      pool.vaultAAmount = decodeVault(vaultAAccount, state.baseMint);
      pool.vaultBAmount = decodeVault(vaultBAccount, state.quoteMint);
    } else {
      if (configAccount == null) {
        throw new Error(`Raydium CPMM refresh missed fee config for ${pool.descriptor.pool_id}`);
      }
      const state = CpmmPoolInfoLayout.decode(poolAccount.data);
      if (!state.vaultA.equals(pool.vaultAAddress) || !state.vaultB.equals(pool.vaultBAddress)) {
        throw new Error(`Raydium CPMM ${pool.descriptor.pool_id} changed vault identity`);
      }
      pool.state = state;
      pool.config = CpmmConfigInfoLayout.decode(configAccount.data);
      pool.vaultAAmount = decodeVault(vaultAAccount, state.mintA);
      pool.vaultBAmount = decodeVault(vaultBAccount, state.mintB);
      pool.provenance.acceptDependency("config", snapshot.context.slot);
    }
    if (snapshot.context.slot > pool.provenance.coreStateSlot) {
      pool.provenance.acceptCore(snapshot.context.slot);
    }
    pool.provenance.acceptDependency("vault-a", snapshot.context.slot);
    pool.provenance.acceptDependency("vault-b", snapshot.context.slot);
    pool.provenance.noteRpcRefresh();
    for (const [slot, pending] of pool.pending) {
      if (slot <= pool.provenance.coreStateSlot) {
        if (pending.timer !== undefined) clearTimeout(pending.timer);
        pool.pending.delete(slot);
      }
    }
    if (before !== this.poolStateFingerprint(pool)) {
      this.requestPoolState(pool, `refresh:${snapshot.context.slot}`, "immediate");
    } else {
      this.refreshesUnchanged += 1;
    }
  }

  private poolStateFingerprint(pool: AttachedPool): string {
    const [reserveA, reserveB] = this.reserves(pool);
    const [feeNumerator, feeDenominator] = this.fee(pool);
    return [
      reserveA.toString(10),
      reserveB.toString(10),
      feeNumerator.toString(10),
      feeDenominator.toString(10),
    ].join(":");
  }

  private requestPoolState(
    pool: AttachedPool,
    _reason: string,
    mode: "immediate" | "debounced",
  ): void {
    const provenance = pool.provenance.fields();
    pool.stateEmitter.request([
      this.poolStateFingerprint(pool),
      provenance.core_state_slot,
      provenance.dependency_slot_min ?? "",
      provenance.dependency_slot_max ?? "",
      provenance.dependency_generation,
    ].join(":"), mode);
  }

  private emitPoolStateNow(pool: AttachedPool): void {
    if (this.closed) return;
    const [mintA, mintB] = this.mints(pool);
    const [decimalsA, decimalsB] = this.decimals(pool);
    const [reserveA, reserveB] = this.reserves(pool);
    const [feeNumerator, feeDenominator] = this.fee(pool);
    if (reserveA.lten(0) || reserveB.lten(0)) return;
    this.callbacks.onPoolState(pool.protocol, {
      pool_id: pool.descriptor.pool_id,
      label: pool.descriptor.label,
      slot: pool.provenance.coreStateSlot,
      token_a_mint: mintA.toBase58(),
      token_b_mint: mintB.toBase58(),
      token_a_decimals: decimalsA,
      token_b_decimals: decimalsB,
      reserve_a_raw: reserveA.toString(10),
      reserve_b_raw: reserveB.toString(10),
      trade_fee_numerator: feeNumerator.toString(10),
      trade_fee_denominator: feeDenominator.toString(10),
      ...pool.provenance.fields(),
    });
  }

  public runtimeStats(): Record<string, number> {
    let dependencyEmitsCoalesced = 0;
    let externalEmits = 0;
    let refreshInFlight = 0;
    for (const pool of this.pools.values()) {
      const emitter = pool.stateEmitter.stats();
      dependencyEmitsCoalesced += emitter.coalesced_total;
      externalEmits += emitter.external_emits_total;
      refreshInFlight += pool.provenance.refreshInFlight ? 1 : 0;
    }
    return {
      pool_count: this.pools.size,
      dependency_emits_coalesced_total: dependencyEmitsCoalesced,
      external_pool_state_emits_total: externalEmits,
      refresh_inflight: refreshInFlight,
      refreshes_started_total: this.refreshesStarted,
      refreshes_completed_total: this.refreshesCompleted,
      refreshes_unchanged_total: this.refreshesUnchanged,
    };
  }

  private unavailable(request: QuoteRequestMessage, error: string): RaydiumStandardQuoteResult {
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
    if (this.connection === null) throw new Error("Raydium standard quote engine is not open");
    return this.connection;
  }
}

/**
 * Read-only local Drift perpetual-market feed.
 *
 * Drift no longer has a reliable hosted public DLOB endpoint.  This worker
 * derives the public L2 view locally from the protocol's on-chain user-order
 * accounts (DLOB), the protocol market/oracle accounts, and vAMM fallback
 * liquidity.  It intentionally has no instruction, signer, wallet-file, or
 * transaction path.  The SDK requires an IWallet-shaped object to construct
 * a client, so we supply a fixed public key whose signing methods always
 * throw; no private key exists in this process.
 *
 * The JSON-lines messages are consumed by Python's venue-neutral perp data
 * layer.  They are live-only: this worker writes no tick data to disk.
 */

import readline from "node:readline";

import { Connection, PublicKey, type Transaction } from "@solana/web3.js";
import {
  BASE_PRECISION,
  BN,
  calculateLongShortFundingRate,
  calculateReservePrice,
  DLOBSubscriber,
  DriftClient,
  FUNDING_RATE_PRECISION,
  getMarketsAndOraclesForSubscription,
  MainnetPerpMarkets,
  MarketType,
  OrderSubscriber,
  PRICE_PRECISION,
  SlotSubscriber,
  type IWallet,
  type L2Level,
  type PerpMarketConfig,
} from "@drift-labs/sdk";

import { emit, redactUrls, safeError } from "./protocol.js";

const MAX_BASES = 32;
const MAX_DEPTH = 50;
const MIN_UPDATE_FREQUENCY_MS = 200;
const MAX_UPDATE_FREQUENCY_MS = 10_000;

interface ConfigureMessage {
  type: "configure";
  rpc_http_url: string;
  rpc_ws_url?: string;
  bases: string[];
  depth?: number;
  update_frequency_ms?: number;
}

interface ShutdownMessage {
  type: "shutdown";
}

type WorkerMessage = ConfigureMessage | ShutdownMessage;

function requiredString(value: unknown, field: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`${field} must be a non-empty string`);
  }
  return value.trim();
}

function endpoint(value: string, scheme: "https:" | "wss:", field: string): string {
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error(`${field} must be a URL`);
  }
  if (parsed.protocol !== scheme || !parsed.hostname) {
    throw new Error(`${field} must use ${scheme}//`);
  }
  return value;
}

function integerInRange(
  value: unknown,
  field: string,
  minimum: number,
  maximum: number,
  fallback: number,
): number {
  if (value === undefined) return fallback;
  if (!Number.isInteger(value) || typeof value !== "number" || value < minimum || value > maximum) {
    throw new Error(`${field} must be an integer in [${minimum}, ${maximum}]`);
  }
  return value;
}

function parseMessage(value: unknown): WorkerMessage {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("worker message must be a JSON object");
  }
  const payload = value as Record<string, unknown>;
  const type = requiredString(payload.type, "type");
  if (type === "shutdown") return { type };
  if (type !== "configure") throw new Error(`unsupported worker message type ${JSON.stringify(type)}`);
  if (!Array.isArray(payload.bases) || payload.bases.length === 0 || payload.bases.length > MAX_BASES) {
    throw new Error(`bases must contain 1..${MAX_BASES} strings`);
  }
  const bases = payload.bases.map((item, index) => requiredString(item, `bases[${index}]`).toUpperCase());
  if (new Set(bases).size !== bases.length) throw new Error("bases must be unique");
  const httpUrl = endpoint(requiredString(payload.rpc_http_url, "rpc_http_url"), "https:", "rpc_http_url");
  const wsUrl = payload.rpc_ws_url === undefined
    ? undefined
    : endpoint(requiredString(payload.rpc_ws_url, "rpc_ws_url"), "wss:", "rpc_ws_url");
  return {
    type,
    rpc_http_url: httpUrl,
    ...(wsUrl === undefined ? {} : { rpc_ws_url: wsUrl }),
    bases,
    depth: integerInRange(payload.depth, "depth", 1, MAX_DEPTH, 20),
    update_frequency_ms: integerInRange(
      payload.update_frequency_ms,
      "update_frequency_ms",
      MIN_UPDATE_FREQUENCY_MS,
      MAX_UPDATE_FREQUENCY_MS,
      400,
    ),
  };
}

function safeDiagnostic(error: unknown): string {
  if (error instanceof Error && error.stack) return redactUrls(error.stack).slice(0, 2_048);
  return safeError(error);
}

/** A non-signing identity used solely to satisfy the SDK constructor type. */
// The SDK currently pins a slightly older `@solana/web3.js` than the other
// local quote SDKs.  Keep this object structural and cast only at the SDK
// boundary; it never signs, so the version distinction cannot affect a
// transaction path (there is none here).
const readOnlyWallet = {
  publicKey: new PublicKey("11111111111111111111111111111111"),
  signTransaction: async (_transaction: Transaction): Promise<Transaction> => {
    throw new Error("Drift read-only worker cannot sign transactions");
  },
  signAllTransactions: async (_transactions: Transaction[]): Promise<Transaction[]> => {
    throw new Error("Drift read-only worker cannot sign transactions");
  },
  supportedTransactionVersions: new Set([0]),
} as unknown as IWallet;

/** Decimal formatting without going through JS floating point. */
function scaled(value: BN, precision: BN): string {
  const sign = value.isNeg() ? "-" : "";
  const raw = value.abs().toString(10);
  const precisionText = precision.toString(10);
  if (!/^1(?:0+)$/.test(precisionText)) {
    throw new Error("expected a power-of-ten Drift precision");
  }
  const decimalPlaces = precisionText.length - 1;
  if (decimalPlaces === 0) return `${sign}${raw}`;
  const padded = raw.padStart(decimalPlaces + 1, "0");
  const integer = padded.slice(0, -decimalPlaces);
  const fractional = padded.slice(-decimalPlaces).replace(/0+$/u, "");
  return fractional.length === 0 ? `${sign}${integer}` : `${sign}${integer}.${fractional}`;
}

function levelPayload(level: L2Level): Record<string, unknown> {
  const sources = Object.entries(level.sources)
    .filter(([, size]) => size.gt(new BN(0)))
    .map(([source]) => source)
    .sort();
  return {
    price: scaled(level.price, PRICE_PRECISION),
    size: scaled(level.size, BASE_PRECISION),
    liquidity_sources: sources,
  };
}

function levelsAreExecutable(bids: L2Level[], asks: L2Level[]): boolean {
  return bids.length > 0 && asks.length > 0 && bids[0]!.price.lt(asks[0]!.price);
}

function selectedMarkets(bases: readonly string[]): PerpMarketConfig[] {
  const wanted = new Set(bases);
  return MainnetPerpMarkets.filter((market) => wanted.has(market.baseAssetSymbol.toUpperCase()));
}

class DriftRuntime {
  private readonly config: Required<Pick<ConfigureMessage, "rpc_http_url" | "bases" | "depth" | "update_frequency_ms">>
    & Pick<ConfigureMessage, "rpc_ws_url">;
  private readonly connection: Connection;
  private readonly markets: PerpMarketConfig[];
  private readonly unavailableBases: string[];
  private readonly driftClient: DriftClient;
  private readonly slotSubscriber: SlotSubscriber;
  private readonly orderSubscriber: OrderSubscriber;
  private readonly dlobSubscriber: DLOBSubscriber;
  private closed = false;
  private publishing = false;
  private publishPending = false;
  private updates = 0;
  private malformedOrUnavailableBooks = 0;

  constructor(message: ConfigureMessage) {
    const depth = message.depth ?? 20;
    const updateFrequency = message.update_frequency_ms ?? 400;
    this.config = {
      rpc_http_url: message.rpc_http_url,
      ...(message.rpc_ws_url === undefined ? {} : { rpc_ws_url: message.rpc_ws_url }),
      bases: message.bases,
      depth,
      update_frequency_ms: updateFrequency,
    };
    this.connection = new Connection(message.rpc_http_url, {
      commitment: "processed",
      ...(message.rpc_ws_url === undefined ? {} : { wsEndpoint: message.rpc_ws_url }),
    });
    this.markets = selectedMarkets(message.bases);
    const loaded = new Set(this.markets.map((market) => market.baseAssetSymbol.toUpperCase()));
    this.unavailableBases = message.bases.filter((base) => !loaded.has(base));
    if (this.markets.length === 0) {
      throw new Error("none of the requested bases is an active Drift SDK perpetual market");
    }
    const { perpMarketIndexes, spotMarketIndexes, oracleInfos } = getMarketsAndOraclesForSubscription(
      "mainnet-beta",
      this.markets,
      [],
    );
    this.driftClient = new DriftClient({
      connection: this.connection as never,
      wallet: readOnlyWallet,
      env: "mainnet-beta",
      perpMarketIndexes,
      spotMarketIndexes,
      oracleInfos,
      // Do not query or subscribe to any account for the placeholder public key.
      skipLoadUsers: true,
      accountSubscription: {
        type: "websocket",
        commitment: "processed",
        resubTimeoutMs: 30_000,
      },
    });
    this.slotSubscriber = new SlotSubscriber(this.connection as never, { resubTimeoutMs: 30_000 });
    this.orderSubscriber = new OrderSubscriber({
      driftClient: this.driftClient,
      subscriptionConfig: {
        type: "websocket",
        resubTimeoutMs: 30_000,
        // One slow periodic reconciliation catches a missed program-account WS
        // event without turning this into a polling quote feed.
        resyncIntervalMs: 120_000,
        commitment: "processed",
      },
      fastDecode: true,
      decodeData: false,
      fetchAllNonIdleUsers: false,
    });
    this.dlobSubscriber = new DLOBSubscriber({
      driftClient: this.driftClient,
      dlobSource: this.orderSubscriber,
      slotSource: this.slotSubscriber,
      updateFrequency,
      protectedMakerView: false,
    });
  }

  async start(): Promise<void> {
    await this.driftClient.subscribe();
    await this.slotSubscriber.subscribe();
    // The initial fetch gets public users with open orders.  Afterwards this
    // stays synchronized by Solana program-account WebSocket notifications.
    await this.orderSubscriber.subscribe();
    await this.dlobSubscriber.subscribe();
    this.dlobSubscriber.eventEmitter.on("update", () => {
      void this.schedulePublish();
    });
    this.dlobSubscriber.eventEmitter.on("error", (error) => {
      emit({ type: "drift_perp_error", stage: "dlob_update", error: safeError(error) });
    });
    this.driftClient.eventEmitter.on("error", (error) => {
      emit({ type: "drift_perp_error", stage: "market_subscription", error: safeError(error) });
    });
    for (const market of this.markets) this.emitContract(market);
    emit({
      type: "ready",
      venue: "DRIFT",
      transport: "local_typescript_sdk_onchain_dlob_vamm_and_oracle_websocket",
      configured_markets: this.markets.map((market) => market.symbol),
      unavailable_requested_bases: this.unavailableBases,
      depth: this.config.depth,
      update_frequency_ms: this.config.update_frequency_ms,
      initial_open_order_accounts: this.orderSubscriber.usersAccounts.size,
      wallet_or_private_key_used: false,
      transactions_submitted: false,
    });
    await this.schedulePublish();
  }

  private emitContract(config: PerpMarketConfig): void {
    const market = this.driftClient.getPerpMarketAccount(config.marketIndex);
    if (market === undefined) return;
    emit({
      type: "drift_perp_contract",
      venue_symbol: config.symbol,
      base: config.baseAssetSymbol.toUpperCase(),
      settlement: "USDC",
      market_index: config.marketIndex,
      contract_type: "linear_perpetual",
      funding_period_seconds: market.amm.fundingPeriod.toString(10),
      tick_size: scaled(market.amm.orderTickSize, PRICE_PRECISION),
      quantity_step: scaled(market.amm.orderStepSize, BASE_PRECISION),
      public_taker_fee_bps: null,
      fee_source: null,
    });
  }

  private async schedulePublish(): Promise<void> {
    if (this.closed) return;
    if (this.publishing) {
      this.publishPending = true;
      return;
    }
    this.publishing = true;
    try {
      do {
        this.publishPending = false;
        this.publishAllMarkets();
      } while (this.publishPending && !this.closed);
    } catch (error) {
      emit({ type: "drift_perp_error", stage: "quote_projection", error: safeError(error) });
    } finally {
      this.publishing = false;
    }
  }

  private publishAllMarkets(): void {
    const receivedAtMs = Date.now();
    const currentSlot = Math.max(this.slotSubscriber.getSlot(), this.orderSubscriber.getSlot());
    const latestSlot = new BN(Math.max(0, currentSlot));
    for (const config of this.markets) {
      const market = this.driftClient.getPerpMarketAccount(config.marketIndex);
      if (market === undefined) {
        this.malformedOrUnavailableBooks += 1;
        continue;
      }
      try {
        const l2 = this.dlobSubscriber.getL2({
          marketIndex: config.marketIndex,
          marketType: MarketType.PERP,
          depth: this.config.depth,
          includeVamm: true,
          numVammOrders: this.config.depth,
          latestSlot,
        });
        const mmOracle = this.driftClient.getMMOracleDataForPerpMarket(config.marketIndex);
        const oracle = this.driftClient.getOracleDataForPerpMarket(config.marketIndex);
        const [estimatedLong, estimatedShort] = calculateLongShortFundingRate(
          market,
          mmOracle,
          oracle,
        );
        const executable = levelsAreExecutable(l2.bids, l2.asks);
        if (!executable) this.malformedOrUnavailableBooks += 1;
        emit({
          type: "drift_perp_update",
          venue_symbol: config.symbol,
          base: config.baseAssetSymbol.toUpperCase(),
          market_index: config.marketIndex,
          slot: currentSlot,
          received_at_ms: receivedAtMs,
          book_status: executable ? "ok" : "crossed_or_empty_not_executable",
          bids: l2.bids.map(levelPayload),
          asks: l2.asks.map(levelPayload),
          mark_price: scaled(calculateReservePrice(market, mmOracle), PRICE_PRECISION),
          index_price: scaled(oracle.price, PRICE_PRECISION),
          funding_rate_long: scaled(estimatedLong, FUNDING_RATE_PRECISION),
          funding_rate_short: scaled(estimatedShort, FUNDING_RATE_PRECISION),
          funding_rate_kind: "sdk_estimated_per_funding_period_long_short",
          funding_period_seconds: market.amm.fundingPeriod.toString(10),
          open_order_accounts: this.orderSubscriber.usersAccounts.size,
        });
        this.updates += 1;
      } catch (error) {
        emit({
          type: "drift_perp_error",
          stage: "market_projection",
          venue_symbol: config.symbol,
          error: safeError(error),
        });
      }
    }
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    await Promise.allSettled([
      this.dlobSubscriber.unsubscribe(),
      this.orderSubscriber.unsubscribe(),
      this.slotSubscriber.unsubscribe(),
      this.driftClient.unsubscribe(),
    ]);
    emit({
      type: "stopped",
      updates: this.updates,
      malformed_or_unavailable_books: this.malformedOrUnavailableBooks,
      final_open_order_accounts: this.orderSubscriber.usersAccounts.size,
    });
  }
}

let runtime: DriftRuntime | null = null;
let closing = false;

async function shutdown(exitCode = 0): Promise<void> {
  if (closing) return;
  closing = true;
  await runtime?.close().catch((error) => {
    emit({ type: "drift_perp_error", stage: "shutdown", error: safeError(error) });
  });
  process.exitCode = exitCode;
}

process.on("uncaughtException", (error) => {
  emit({ type: "drift_perp_error", stage: "uncaught_exception", error: safeError(error) });
  void shutdown(1).finally(() => process.exit(1));
});
process.on("unhandledRejection", (error) => {
  emit({ type: "drift_perp_error", stage: "unhandled_rejection", error: safeError(error) });
  void shutdown(1).finally(() => process.exit(1));
});

const input = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
for await (const line of input) {
  if (closing || line.trim().length === 0) continue;
  try {
    const message = parseMessage(JSON.parse(line));
    if (message.type === "shutdown") {
      await shutdown();
      break;
    }
    if (runtime !== null) throw new Error("Drift perp worker is already configured");
    runtime = new DriftRuntime(message);
    await runtime.start();
  } catch (error) {
    emit({ type: "drift_perp_error", stage: "configure", error: safeDiagnostic(error) });
  }
}

await shutdown();

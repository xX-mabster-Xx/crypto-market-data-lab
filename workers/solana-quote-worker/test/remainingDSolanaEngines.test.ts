import assert from "node:assert/strict";
import { createRequire } from "node:module";
import test from "node:test";

import BN from "bn.js";
import {
  CpmmConfigInfoLayout,
  CpmmPoolInfoLayout,
  CurveCalculator,
  splAccountLayout,
} from "@raydium-io/raydium-sdk-v2";
import {
  type AccountInfo,
  PublicKey,
} from "@solana/web3.js";

import {
  DebouncedStateEmitter,
  PoolSlotProvenance,
} from "../src/engineRuntime.js";
import { MeteoraDlmmQuoteEngine } from "../src/meteoraDlmm.js";
import { OrcaWhirlpoolQuoteEngine } from "../src/orcaWhirlpool.js";
import { RaydiumClmmQuoteEngine } from "../src/raydiumClmm.js";
import { RaydiumStandardQuoteEngine } from "../src/raydiumStandard.js";

const require = createRequire(import.meta.url);
const Orca = require("@orca-so/whirlpools-sdk") as {
  SwapUtils: { getTickArrays: (...args: unknown[]) => Promise<unknown[]> };
};

const ZERO_KEY = new PublicKey(Buffer.alloc(32));
const TOKEN_PROGRAM_ID = new PublicKey("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");

function account(data: Buffer, owner = ZERO_KEY): AccountInfo<Buffer> {
  return { data, executable: false, lamports: 0, owner, rentEpoch: 0 };
}

function configData(tradeFeeRate: bigint): Buffer {
  const data = Buffer.alloc(CpmmConfigInfoLayout.span);
  data.writeBigUInt64LE(tradeFeeRate, 12);
  data.writeBigUInt64LE(1n, 20);
  data.writeBigUInt64LE(1n, 28);
  data.writeBigUInt64LE(1n, 108);
  return data;
}

function vaultData(amount: bigint): Buffer {
  const data = Buffer.alloc(splAccountLayout.span);
  data.writeBigUInt64LE(amount, 64);
  return data;
}

function standardHarness() {
  const engine = new RaydiumStandardQuoteEngine({ onPoolState: () => undefined });
  const coreData = Buffer.alloc(CpmmPoolInfoLayout.span);
  const newerConfigData = configData(1_000n);
  const newerVaultAData = vaultData(5_000n);
  const newerVaultBData = vaultData(6_000n);
  const provenance = new PoolSlotProvenance(100, 0);
  provenance.acceptDependency("vault-a", 120, 0);
  provenance.acceptDependency("vault-b", 120, 0);
  provenance.acceptDependency("config", 120, 0);
  const pool = {
    descriptor: { protocol: "raydium_cpmm", pool_id: "cpmm", label: "CPMM" },
    protocol: "raydium_cpmm",
    address: ZERO_KEY,
    state: CpmmPoolInfoLayout.decode(coreData),
    config: CpmmConfigInfoLayout.decode(newerConfigData),
    configAddress: ZERO_KEY,
    configAccountData: newerConfigData,
    configAccountOwner: ZERO_KEY,
    coreAccountData: coreData,
    coreAccountOwner: ZERO_KEY,
    vaultAAddress: ZERO_KEY,
    vaultBAddress: ZERO_KEY,
    vaultAAmount: new BN(5_000),
    vaultBAmount: new BN(6_000),
    vaultAAccountData: newerVaultAData,
    vaultBAccountData: newerVaultBData,
    vaultAAccountOwner: TOKEN_PROGRAM_ID,
    vaultBAccountOwner: TOKEN_PROGRAM_ID,
    provenance,
    subscriptionIds: [],
    pending: new Map(),
    stateEmitter: new DebouncedStateEmitter(() => undefined, 100),
  };
  let slot = 105;
  let values: Array<AccountInfo<Buffer> | null> = [
    account(Buffer.from(coreData)),
    account(vaultData(100n), TOKEN_PROGRAM_ID),
    account(vaultData(200n), TOKEN_PROGRAM_ID),
    account(configData(2_000n)),
  ];
  (engine as any).pools.set("cpmm", pool);
  Object.assign(engine as object, {
    connection: {
      getMultipleAccountsInfoAndContext: async () => ({ context: { slot }, value: values }),
    },
  });
  return {
    engine,
    pool,
    setSnapshot(nextSlot: number, nextValues: Array<AccountInfo<Buffer> | null>) {
      slot = nextSlot;
      values = nextValues;
    },
  };
}

function meteoraHarness() {
  const dependencyA = new PublicKey(Buffer.alloc(32, 7));
  const dependencyB = new PublicKey(Buffer.alloc(32, 9));
  const notices: any[] = [];
  const quoteInputs: any[][] = [];
  const engine = new MeteoraDlmmQuoteEngine({ onPoolState: (notice) => notices.push(notice) });
  const provenance = new PoolSlotProvenance(100, 0);
  provenance.acceptDependency(dependencyA.toBase58(), 110, 0);
  let discovered = [dependencyA];
  let snapshotSlot = 120;
  let snapshotValues: Array<AccountInfo<Buffer> | null> = [account(Buffer.from("rpc-bin"))];
  let nextSubscriptionId = 10;
  const removed: number[] = [];
  const pool: any = {
    descriptor: { protocol: "meteora_dlmm", pool_id: "meteora", label: "Meteora" },
    address: new PublicKey(Buffer.alloc(32, 6)),
    dlmm: {
      program: {},
      lbPair: { activeId: 0, binStep: 1 },
      tokenX: { mint: { address: ZERO_KEY, decimals: 6 } },
      tokenY: { mint: { address: dependencyA, decimals: 6 } },
      getBinArrayForSwap: async () => discovered.map((publicKey) => ({
        publicKey,
        account: { marker: "sdk-contextless" },
      })),
      swapQuote: (_input: BN, _swapForY: boolean, _slippage: BN, bins: any[]) => {
        quoteInputs.push(bins);
        return {
          outAmount: new BN(9),
          consumedInAmount: new BN(10),
          fee: new BN(1),
          protocolFee: new BN(0),
          feeOnInput: true,
          priceImpact: { toString: () => "0" },
        };
      },
    },
    provenance,
    coreAccountData: Buffer.from("core"),
    poolSubscriptionId: 1,
    binArrays: new Map([[dependencyA.toBase58(), {
      publicKey: dependencyA,
      account: { marker: "old-bin" },
    }]]),
    binAccountData: new Map([[dependencyA.toBase58(), Buffer.from("old-bin")]]),
    binSubscriptionIds: new Map([[dependencyA.toBase58(), 2]]),
    retiredBinSubscriptionIds: new Set(),
    binCacheAtMs: 1,
    stateEmitter: undefined,
  };
  pool.stateEmitter = new DebouncedStateEmitter(
    () => (engine as any).emitPoolStateNow(pool),
    100_000,
  );
  Object.assign(engine as object, {
    connection: {
      getMultipleAccountsInfoAndContext: async () => ({
        context: { slot: snapshotSlot },
        value: snapshotValues,
      }),
      onAccountChange: () => nextSubscriptionId++,
      removeAccountChangeListener: async (id: number) => { removed.push(id); },
    },
  });
  (engine as any).decodeBinAccount = (_pool: unknown, value: AccountInfo<Buffer>) => ({
    marker: value.data.equals(Buffer.from("bad"))
      ? (() => { throw new Error("synthetic bin decode failure"); })()
      : value.data.toString("utf8"),
  });
  (engine as any).decodePairAccount = (_pool: unknown, value: AccountInfo<Buffer>) => ({
    activeId: value.data[0] ?? 0,
    binStep: 1,
  });
  (engine as any).pools.set("meteora", pool);
  return {
    dependencyA,
    dependencyB,
    engine,
    notices,
    pool,
    provenance,
    quoteInputs,
    removed,
    setDiscovery(keys: PublicKey[]) { discovered = keys; },
    setSnapshot(slot: number, values: Array<AccountInfo<Buffer> | null>) {
      snapshotSlot = slot;
      snapshotValues = values;
    },
  };
}

function orcaPoolData() {
  return {
    tickCurrentIndex: 0,
    tickSpacing: 1,
    sqrtPrice: new BN(1),
    liquidity: new BN(1),
    tokenMintA: ZERO_KEY,
    tokenMintB: new PublicKey(Buffer.alloc(32, 8)),
    feeRate: 300,
    protocolFeeRate: 10,
    feeGrowthGlobalA: new BN(0),
    feeGrowthGlobalB: new BN(0),
    protocolFeeOwedA: new BN(0),
    protocolFeeOwedB: new BN(0),
  };
}

function tickData(marker: number) {
  return {
    startTickIndex: marker,
    ticks: [{ initialized: true, liquidityNet: new BN(marker), liquidityGross: new BN(marker) }],
  };
}

test("R-03 Meteora context-bearing refresh repairs a subscribed stale bin", async () => {
  const dependency = new PublicKey(Buffer.alloc(32, 7));
  const address = dependency.toBase58();
  const provenance = new PoolSlotProvenance(100, 0);
  provenance.acceptDependency(address, 110, 0);
  const engine = new MeteoraDlmmQuoteEngine({ onPoolState: () => undefined });
  const rpcData = Buffer.from("rpc-bin");
  const pool = {
    descriptor: { protocol: "meteora_dlmm", pool_id: "meteora", label: "Meteora" },
    address: ZERO_KEY,
    dlmm: {
      program: {},
      lbPair: { activeId: 0, binStep: 1 },
      tokenX: { mint: { address: ZERO_KEY, decimals: 6 } },
      tokenY: { mint: { address: dependency, decimals: 6 } },
      getBinArrayForSwap: async () => [{ publicKey: dependency, account: { marker: "rpc" } }],
    },
    provenance,
    coreAccountData: Buffer.alloc(1),
    poolSubscriptionId: 1,
    binArrays: new Map([[address, { publicKey: dependency, account: { marker: "old" } }]]),
    binAccountData: new Map([[address, Buffer.from("old-bin")]]),
    binSubscriptionIds: new Map([[address, 2]]),
    retiredBinSubscriptionIds: new Set(),
    binCacheAtMs: 1,
    stateEmitter: new DebouncedStateEmitter(() => undefined, 100),
  };
  Object.assign(engine as object, {
    connection: {
      getMultipleAccountsInfoAndContext: async () => ({
        context: { slot: 120 },
        value: [account(rpcData)],
      }),
      onAccountChange: () => 3,
      removeAccountChangeListener: async () => undefined,
    },
  });
  (engine as any).decodeBinAccount = (_key: PublicKey, value: AccountInfo<Buffer>) => ({
    marker: value.data.toString("utf8"),
  });

  await (engine as any).refreshBins(pool);

  assert.equal(pool.binArrays.get(address)?.account.marker, "rpc-bin");
  assert.equal(provenance.dependencySlot(address), 120);
  assert.equal(provenance.dependencyGeneration, 2);
  pool.stateEmitter.dispose();
});

test("R-03/O-03 Meteora reconciles versions, membership, quote inputs and unchanged validation", async () => {
  const harness = meteoraHarness();
  const { dependencyA, dependencyB, engine, notices, pool, provenance, quoteInputs } = harness;
  const keyA = dependencyA.toBase58();
  const keyB = dependencyB.toBase58();

  await (engine as any).refreshBins(pool);
  pool.stateEmitter.flushForTest();
  assert.equal(pool.binArrays.get(keyA).account.marker, "rpc-bin");
  assert.equal(provenance.dependencyGeneration, 2);
  assert.equal(notices.length, 1);
  assert.equal(notices[0].dependency_slot_max, 120);

  harness.setSnapshot(121, [account(Buffer.from("rpc-bin"))]);
  await (engine as any).refreshBins(pool);
  pool.stateEmitter.flushForTest();
  assert.equal(provenance.dependencySlot(keyA), 121);
  assert.equal(provenance.dependencyGeneration, 2, "identical bytes are validation, not semantic change");
  assert.equal(notices.length, 1, "unchanged refresh must not emit");

  const fullyValidatedAt = pool.binCacheAtMs;
  (engine as any).updateBin(pool, dependencyA, account(Buffer.from("ws-new")), { slot: 140 });
  assert.equal(
    pool.binCacheAtMs,
    fullyValidatedAt,
    "one WS account must not freshen the entire dependency snapshot",
  );
  assert.equal(provenance.dependencyGeneration, 3);
  assert.doesNotThrow(() => {
    (engine as any).updateBin(pool, dependencyA, account(Buffer.from("bad")), { slot: 139 });
  }, "a stale malformed WS value must be rejected before decode");
  harness.setSnapshot(130, [account(Buffer.from("rpc-older"))]);
  await (engine as any).refreshBins(pool);
  assert.equal(pool.binArrays.get(keyA).account.marker, "ws-new");
  assert.equal(provenance.dependencySlot(keyA), 140);
  assert.equal(provenance.dependencyGeneration, 3);

  harness.setSnapshot(140, [account(Buffer.from("same-slot-loser"))]);
  await (engine as any).refreshBins(pool);
  assert.equal(pool.binArrays.get(keyA).account.marker, "ws-new", "same slot is first-writer-wins");

  const cacheAtBeforeMissing = pool.binCacheAtMs;
  harness.setSnapshot(150, [null]);
  await assert.rejects((engine as any).refreshBins(pool), /disappeared/u);
  assert.equal(pool.binCacheAtMs, cacheAtBeforeMissing, "failed validation cannot freshen cache age");
  assert.equal(provenance.dependencyGeneration, 3);

  harness.setSnapshot(150, [account(Buffer.from("bad"))]);
  await assert.rejects((engine as any).refreshBins(pool), /decode failure/u);
  assert.equal(pool.binCacheAtMs, cacheAtBeforeMissing);
  assert.equal(provenance.dependencyGeneration, 3);

  harness.setDiscovery([dependencyB]);
  harness.setSnapshot(150, [account(Buffer.from("replacement"))]);
  await (engine as any).refreshBins(pool);
  pool.stateEmitter.flushForTest();
  assert.equal(pool.binArrays.has(keyA), false);
  assert.equal(provenance.dependencySlot(keyA), undefined);
  assert.equal(pool.binArrays.get(keyB).account.marker, "replacement");
  assert.equal(provenance.dependencySlot(keyB), 150);
  assert.equal(provenance.dependencyGeneration, 5, "remove and add are both semantic changes");
  assert.ok(harness.removed.includes(2));
  assert.equal(notices.at(-1).dependency_generation, 5);
  assert.equal(notices.at(-1).dependency_slot_min, 150);

  const result = await engine.quote({
    type: "quote_request",
    request_id: "quote",
    protocol: "meteora_dlmm",
    pool_id: "meteora",
    input_mint: ZERO_KEY.toBase58(),
    output_mint: dependencyA.toBase58(),
    input_amount_raw: "10",
  });
  assert.equal(result.status, "ok");
  assert.equal(quoteInputs.length, 1);
  assert.equal(quoteInputs[0][0].account.marker, "replacement", "real quote sees repaired data");
  pool.stateEmitter.dispose();
});

test("R-03 Orca context-bearing refresh repairs a subscribed stale tick", async () => {
  const dependency = new PublicKey(Buffer.alloc(32, 8));
  const address = dependency.toBase58();
  const provenance = new PoolSlotProvenance(100, 0);
  provenance.acceptDependency(address, 110, 0);
  const engine = new OrcaWhirlpoolQuoteEngine({ onPoolState: () => undefined });
  const oldGetTickArrays = Orca.SwapUtils.getTickArrays;
  Orca.SwapUtils.getTickArrays = async () => [{ address: dependency, data: { marker: "rpc" } }];
  const pool = {
    descriptor: { protocol: "orca_whirlpool", pool_id: "orca", label: "Orca" },
    address: ZERO_KEY,
    data: {
      tickCurrentIndex: 0,
      tickSpacing: 1,
      sqrtPrice: new BN(1),
      liquidity: new BN(1),
    },
    tokenExtensionContext: {},
    provenance,
    coreAccountData: Buffer.alloc(1),
    poolSubscriptionId: 1,
    tickSubscriptionIds: new Map([[address, 2]]),
    retiredTickSubscriptionIds: new Set(),
    tickArraysAToB: [{ address: dependency, data: { marker: "old" } }],
    tickArraysBToA: [{ address: dependency, data: { marker: "old" } }],
    tickAccountData: new Map([[address, Buffer.from("old-tick")]]),
    tickCacheAtMs: 1,
    stateEmitter: new DebouncedStateEmitter(() => undefined, 100),
  };
  Object.assign(engine as object, {
    fetcher: {},
    connection: {
      getMultipleAccountsInfoAndContext: async () => ({
        context: { slot: 120 },
        value: [account(Buffer.from("rpc-tick"))],
      }),
      onAccountChange: () => 3,
      removeAccountChangeListener: async () => undefined,
    },
  });
  (engine as any).decodeTickAccount = (_key: PublicKey, value: AccountInfo<Buffer>) => ({
    marker: value.data.toString("utf8"),
  });

  try {
    await (engine as any).refreshTicks(pool);
    assert.equal(pool.tickArraysAToB[0].data.marker, "rpc-tick");
    assert.equal(provenance.dependencySlot(address), 120);
    assert.equal(provenance.dependencyGeneration, 2);
  } finally {
    Orca.SwapUtils.getTickArrays = oldGetTickArrays;
    pool.stateEmitter.dispose();
  }
});

test("R-03/O-03 Orca reconciles versions, membership, simulation inputs and decode failures", async () => {
  const dependencyA = new PublicKey(Buffer.alloc(32, 8));
  const dependencyB = new PublicKey(Buffer.alloc(32, 10));
  const keyA = dependencyA.toBase58();
  const keyB = dependencyB.toBase58();
  const notices: any[] = [];
  const provenance = new PoolSlotProvenance(100, 0);
  provenance.acceptDependency(keyA, 110, 0);
  const engine = new OrcaWhirlpoolQuoteEngine({ onPoolState: (notice) => notices.push(notice) });
  const oldGetTickArrays = Orca.SwapUtils.getTickArrays;
  let discovered = [dependencyA];
  let snapshotSlot = 120;
  let snapshotValues: Array<AccountInfo<Buffer> | null> = [account(Buffer.from([2]))];
  let nextSubscriptionId = 10;
  const removed: number[] = [];
  Orca.SwapUtils.getTickArrays = async () => discovered.map((address) => ({
    address,
    data: tickData(99),
  }));
  const pool: any = {
    descriptor: { protocol: "orca_whirlpool", pool_id: "orca", label: "Orca" },
    address: new PublicKey(Buffer.alloc(32, 12)),
    data: orcaPoolData(),
    tokenExtensionContext: {
      tokenMintWithProgramA: { decimals: 6 },
      tokenMintWithProgramB: { decimals: 6 },
    },
    provenance,
    coreAccountData: Buffer.from("core"),
    poolSubscriptionId: 1,
    tickSubscriptionIds: new Map([[keyA, 2]]),
    retiredTickSubscriptionIds: new Set(),
    tickArraysAToB: [{ address: dependencyA, data: tickData(1) }],
    tickArraysBToA: [{ address: dependencyA, data: tickData(1) }],
    tickAccountData: new Map([[keyA, Buffer.from([1])]]),
    tickCacheAtMs: 1,
    stateEmitter: undefined,
  };
  pool.stateEmitter = new DebouncedStateEmitter(
    () => (engine as any).emitPoolStateNow(pool),
    100_000,
  );
  Object.assign(engine as object, {
    fetcher: {},
    connection: {
      getMultipleAccountsInfoAndContext: async () => ({
        context: { slot: snapshotSlot },
        value: snapshotValues,
      }),
      onAccountChange: () => nextSubscriptionId++,
      removeAccountChangeListener: async (id: number) => { removed.push(id); },
    },
  });
  (engine as any).decodeTickAccount = (_address: PublicKey, value: AccountInfo<Buffer>) => {
    if (value.data.equals(Buffer.from("bad"))) throw new Error("synthetic decode failure");
    return tickData(value.data[0] ?? 0);
  };
  (engine as any).pools.set("orca", pool);

  try {
    await (engine as any).refreshTicks(pool);
    pool.stateEmitter.flushForTest();
    assert.equal(pool.tickArraysAToB[0].data.startTickIndex, 2);
    assert.equal(provenance.dependencyGeneration, 2);
    assert.equal(notices.length, 1);

    snapshotSlot = 121;
    snapshotValues = [account(Buffer.from([2]))];
    await (engine as any).refreshTicks(pool);
    pool.stateEmitter.flushForTest();
    assert.equal(provenance.dependencySlot(keyA), 121);
    assert.equal(provenance.dependencyGeneration, 2);
    assert.equal(notices.length, 1, "unchanged refresh must not emit");

    const fullyValidatedAt = pool.tickCacheAtMs;
    (engine as any).updateTick(pool, dependencyA, account(Buffer.from([4])), { slot: 140 });
    assert.equal(
      pool.tickCacheAtMs,
      fullyValidatedAt,
      "one WS account must not freshen the entire dependency snapshot",
    );
    assert.doesNotThrow(() => {
      (engine as any).updateTick(pool, dependencyA, account(Buffer.from("bad")), { slot: 139 });
    }, "a stale malformed WS value must be rejected before decode");
    snapshotSlot = 130;
    snapshotValues = [account(Buffer.from([3]))];
    await (engine as any).refreshTicks(pool);
    assert.equal(pool.tickArraysAToB[0].data.startTickIndex, 4);
    assert.equal(provenance.dependencySlot(keyA), 140);
    assert.equal(provenance.dependencyGeneration, 3);

    snapshotSlot = 140;
    snapshotValues = [account(Buffer.from([5]))];
    await (engine as any).refreshTicks(pool);
    assert.equal(pool.tickArraysAToB[0].data.startTickIndex, 4, "same-slot RPC loses deterministically");

    const cacheAtBeforeFailure = pool.tickCacheAtMs;
    snapshotSlot = 150;
    snapshotValues = [account(Buffer.from("bad"))];
    await assert.rejects((engine as any).refreshTicks(pool), /decode failure/u);
    assert.equal(pool.tickCacheAtMs, cacheAtBeforeFailure);
    assert.equal(provenance.dependencyGeneration, 3);

    snapshotValues = [null];
    await assert.rejects((engine as any).refreshTicks(pool), /disappeared/u);
    assert.equal(pool.tickCacheAtMs, cacheAtBeforeFailure);
    assert.equal(provenance.dependencyGeneration, 3);

    discovered = [dependencyB];
    snapshotValues = [account(Buffer.from([6]))];
    await (engine as any).refreshTicks(pool);
    pool.stateEmitter.flushForTest();
    assert.equal(pool.tickArraysAToB[0].address.toBase58(), keyB);
    assert.equal(pool.tickArraysAToB[0].data.startTickIndex, 6);
    assert.equal(provenance.dependencySlot(keyA), undefined);
    assert.equal(provenance.dependencySlot(keyB), 150);
    assert.equal(provenance.dependencyGeneration, 5);
    assert.ok(removed.includes(2));
    assert.equal(notices.at(-1).dependency_generation, 5);

    const simulation = engine.orcaSimulationState("orca");
    assert.deepEqual(simulation.tick_arrays.map((item) => item.start_tick_index), [6]);
    assert.equal(simulation.dependency_slot_max, 150);
  } finally {
    Orca.SwapUtils.getTickArrays = oldGetTickArrays;
    pool.stateEmitter.dispose();
  }
});

test("dependency debounce is subsumed by an immediate core update and shutdown clears timers/subscriptions", async () => {
  const meteora = meteoraHarness();
  (meteora.engine as any).requestPoolState(meteora.pool, "initial", "immediate");
  (meteora.engine as any).updateBin(
    meteora.pool,
    meteora.dependencyA,
    account(Buffer.from("dependency")),
    { slot: 111 },
  );
  assert.equal(meteora.pool.stateEmitter.stats().pending, 1);
  (meteora.engine as any).updatePair(meteora.pool, account(Buffer.from([5])), { slot: 105 });
  assert.equal(meteora.pool.stateEmitter.stats().pending, 0);
  assert.equal(meteora.notices.at(-1).core_state_slot, 105);
  assert.equal(meteora.notices.at(-1).dependency_generation, 2);
  const meteoraNoticeCount = meteora.notices.length;
  (meteora.engine as any).updateBin(
    meteora.pool,
    meteora.dependencyA,
    account(Buffer.from("pending-at-close")),
    { slot: 112 },
  );
  await meteora.engine.close();
  await new Promise<void>((resolve) => setTimeout(resolve, 5));
  assert.equal(meteora.notices.length, meteoraNoticeCount);
  assert.ok(meteora.removed.includes(1));
  assert.ok(meteora.removed.includes(2));

  const dependency = new PublicKey(Buffer.alloc(32, 8));
  const notices: any[] = [];
  const removed: number[] = [];
  const orca = new OrcaWhirlpoolQuoteEngine({ onPoolState: (notice) => notices.push(notice) });
  const provenance = new PoolSlotProvenance(100, 0);
  provenance.acceptDependency(dependency.toBase58(), 110, 0);
  const pool: any = {
    descriptor: { protocol: "orca_whirlpool", pool_id: "orca-close", label: "Orca" },
    address: ZERO_KEY,
    data: orcaPoolData(),
    tokenExtensionContext: {
      tokenMintWithProgramA: { decimals: 6 },
      tokenMintWithProgramB: { decimals: 6 },
    },
    provenance,
    coreAccountData: Buffer.from("core"),
    poolSubscriptionId: 3,
    tickSubscriptionIds: new Map([[dependency.toBase58(), 4]]),
    retiredTickSubscriptionIds: new Set(),
    tickArraysAToB: [{ address: dependency, data: tickData(1) }],
    tickArraysBToA: [{ address: dependency, data: tickData(1) }],
    tickAccountData: new Map([[dependency.toBase58(), Buffer.from([1])]]),
    tickCacheAtMs: 1,
    stateEmitter: undefined,
  };
  pool.stateEmitter = new DebouncedStateEmitter(
    () => (orca as any).emitPoolStateNow(pool),
    100_000,
  );
  Object.assign(orca as object, {
    connection: { removeAccountChangeListener: async (id: number) => { removed.push(id); } },
  });
  (orca as any).decodeTickAccount = (_address: PublicKey, value: AccountInfo<Buffer>) => tickData(value.data[0] ?? 0);
  (orca as any).decodePoolAccount = () => ({ ...orcaPoolData(), tickCurrentIndex: 1 });
  (orca as any).pools.set("orca-close", pool);

  (orca as any).requestPoolState(pool, "initial", "immediate");
  (orca as any).updateTick(pool, dependency, account(Buffer.from([2])), { slot: 111 });
  assert.equal(pool.stateEmitter.stats().pending, 1);
  (orca as any).updatePool(pool, account(Buffer.from("new-core")), { slot: 105 });
  assert.equal(pool.stateEmitter.stats().pending, 0);
  assert.equal(notices.at(-1).core_state_slot, 105);
  assert.equal(notices.at(-1).dependency_generation, 2);
  const orcaNoticeCount = notices.length;
  (orca as any).updateTick(pool, dependency, account(Buffer.from([3])), { slot: 112 });
  assert.equal(pool.stateEmitter.stats().pending, 1);
  await orca.close();
  await new Promise<void>((resolve) => setTimeout(resolve, 5));
  assert.equal(notices.length, orcaNoticeCount);
  assert.deepEqual(removed.sort((a, b) => a - b), [3, 4]);
});

test("R-03 failed listener cleanup is retried without accumulating duplicate subscriptions", async () => {
  const meteora = meteoraHarness();
  let meteoraSubscriptions = 0;
  let meteoraRemovalFails = true;
  (meteora.engine as any).connection.onAccountChange = () => 100 + meteoraSubscriptions++;
  (meteora.engine as any).connection.removeAccountChangeListener = async () => {
    if (meteoraRemovalFails) throw new Error("synthetic listener removal failure");
  };
  meteora.setDiscovery([meteora.dependencyB]);
  meteora.setSnapshot(150, [null]);
  await assert.rejects((meteora.engine as any).refreshBins(meteora.pool), /disappeared/u);
  assert.equal(meteoraSubscriptions, 1);
  assert.equal(meteora.pool.retiredBinSubscriptionIds.size, 1);
  meteora.setSnapshot(151, [account(Buffer.from("replacement"))]);
  await assert.rejects((meteora.engine as any).refreshBins(meteora.pool), /cleanup is pending/u);
  assert.equal(meteoraSubscriptions, 1, "Meteora must not add another listener while cleanup is pending");
  meteoraRemovalFails = false;
  await (meteora.engine as any).refreshBins(meteora.pool);
  assert.equal(meteoraSubscriptions, 2, "Meteora may replace the listener after cleanup succeeds");
  assert.equal(meteora.pool.retiredBinSubscriptionIds.size, 0);
  meteora.pool.stateEmitter.dispose();

  const dependencyA = new PublicKey(Buffer.alloc(32, 51));
  const dependencyB = new PublicKey(Buffer.alloc(32, 52));
  const keyA = dependencyA.toBase58();
  const provenance = new PoolSlotProvenance(100, 0);
  provenance.acceptDependency(keyA, 110, 0);
  const orca = new OrcaWhirlpoolQuoteEngine({ onPoolState: () => undefined });
  const oldGetTickArrays = Orca.SwapUtils.getTickArrays;
  Orca.SwapUtils.getTickArrays = async () => [{ address: dependencyB, data: tickData(1) }];
  let orcaSubscriptions = 0;
  let orcaRemovalFails = true;
  let snapshotValue: AccountInfo<Buffer> | null = null;
  const pool: any = {
    descriptor: { protocol: "orca_whirlpool", pool_id: "orca-cleanup", label: "Orca" },
    address: ZERO_KEY,
    data: orcaPoolData(),
    tokenExtensionContext: {},
    provenance,
    coreAccountData: Buffer.from("core"),
    poolSubscriptionId: 1,
    tickSubscriptionIds: new Map([[keyA, 2]]),
    retiredTickSubscriptionIds: new Set(),
    tickArraysAToB: [{ address: dependencyA, data: tickData(1) }],
    tickArraysBToA: [{ address: dependencyA, data: tickData(1) }],
    tickAccountData: new Map([[keyA, Buffer.from([1])]]),
    tickCacheAtMs: 1,
    stateEmitter: new DebouncedStateEmitter(() => undefined, 100),
  };
  Object.assign(orca as object, {
    fetcher: {},
    connection: {
      getMultipleAccountsInfoAndContext: async () => ({
        context: { slot: 150 },
        value: [snapshotValue],
      }),
      onAccountChange: () => 200 + orcaSubscriptions++,
      removeAccountChangeListener: async () => {
        if (orcaRemovalFails) throw new Error("synthetic listener removal failure");
      },
    },
  });
  (orca as any).decodeTickAccount = (_address: PublicKey, value: AccountInfo<Buffer>) => tickData(value.data[0] ?? 0);
  try {
    await assert.rejects((orca as any).refreshTicks(pool), /disappeared/u);
    assert.equal(orcaSubscriptions, 1);
    assert.equal(pool.retiredTickSubscriptionIds.size, 1);
    snapshotValue = account(Buffer.from([2]));
    await assert.rejects((orca as any).refreshTicks(pool), /cleanup is pending/u);
    assert.equal(orcaSubscriptions, 1, "Orca must not add another listener while cleanup is pending");
    orcaRemovalFails = false;
    await (orca as any).refreshTicks(pool);
    assert.equal(orcaSubscriptions, 2, "Orca may replace the listener after cleanup succeeds");
    assert.equal(pool.retiredTickSubscriptionIds.size, 0);
  } finally {
    Orca.SwapUtils.getTickArrays = oldGetTickArrays;
    pool.stateEmitter.dispose();
  }
});

test("R-03 dependency refresh aborts when the core revision changes during its context read", async () => {
  const meteora = meteoraHarness();
  let resolveMeteora!: (value: unknown) => void;
  (meteora.engine as any).connection.getMultipleAccountsInfoAndContext = async () => (
    new Promise((resolve) => { resolveMeteora = resolve; })
  );
  const meteoraCacheAt = meteora.pool.binCacheAtMs;
  const meteoraRefresh = (meteora.engine as any).refreshBins(meteora.pool);
  await new Promise<void>((resolve) => setImmediate(resolve));
  meteora.provenance.acceptCore(101);
  resolveMeteora({ context: { slot: 120 }, value: [account(Buffer.from("rpc-bin"))] });
  await assert.rejects(meteoraRefresh, /core changed/u);
  assert.equal(meteora.pool.binCacheAtMs, meteoraCacheAt);
  meteora.pool.stateEmitter.dispose();

  const dependency = new PublicKey(Buffer.alloc(32, 8));
  const provenance = new PoolSlotProvenance(100, 0);
  provenance.acceptDependency(dependency.toBase58(), 110, 0);
  const orca = new OrcaWhirlpoolQuoteEngine({ onPoolState: () => undefined });
  const oldGetTickArrays = Orca.SwapUtils.getTickArrays;
  Orca.SwapUtils.getTickArrays = async () => [{ address: dependency, data: tickData(1) }];
  let resolveOrca!: (value: unknown) => void;
  const pool: any = {
    descriptor: { pool_id: "orca-race", label: "Orca" },
    address: ZERO_KEY,
    data: orcaPoolData(),
    tokenExtensionContext: {},
    provenance,
    coreAccountData: Buffer.from("core"),
    poolSubscriptionId: 1,
    tickSubscriptionIds: new Map([[dependency.toBase58(), 2]]),
    retiredTickSubscriptionIds: new Set(),
    tickArraysAToB: [{ address: dependency, data: tickData(1) }],
    tickArraysBToA: [{ address: dependency, data: tickData(1) }],
    tickAccountData: new Map([[dependency.toBase58(), Buffer.from([1])]]),
    tickCacheAtMs: 1,
    stateEmitter: new DebouncedStateEmitter(() => undefined, 100),
  };
  Object.assign(orca as object, {
    fetcher: {},
    connection: {
      getMultipleAccountsInfoAndContext: async () => (
        new Promise((resolve) => { resolveOrca = resolve; })
      ),
      onAccountChange: () => 3,
      removeAccountChangeListener: async () => undefined,
    },
  });
  (orca as any).decodeTickAccount = (_address: PublicKey, value: AccountInfo<Buffer>) => tickData(value.data[0] ?? 0);
  const orcaCacheAt = pool.tickCacheAtMs;
  const orcaRefresh = (orca as any).refreshTicks(pool);
  await new Promise<void>((resolve) => setImmediate(resolve));
  provenance.acceptCore(101);
  resolveOrca({ context: { slot: 120 }, value: [account(Buffer.from([2]))] });
  try {
    await assert.rejects(orcaRefresh, /core changed/u);
    assert.equal(pool.tickCacheAtMs, orcaCacheAt);
  } finally {
    Orca.SwapUtils.getTickArrays = oldGetTickArrays;
    pool.stateEmitter.dispose();
  }
});

test("R-04 delayed CPMM snapshot advances core without rolling config or vaults back", async () => {
  const { engine, pool } = standardHarness();

  assert.doesNotThrow(() => {
    (engine as any).updateCpmmConfig(pool, account(Buffer.alloc(4)), { slot: 119 });
  }, "an older malformed config must be rejected before decode");

  await (engine as any).refreshCore(pool);

  const state = engine.cpmmSimulationState("cpmm");
  assert.equal(state.core_state_slot, 105);
  assert.equal(state.dependency_slot_max, 120);
  assert.equal(state.trade_fee_rate, "1000");
  assert.equal(state.vault_a_raw, "5000");
  assert.equal(state.vault_b_raw, "6000");
  const input = new BN(1_000);
  const expected = CurveCalculator.swapBaseInput(
    input,
    new BN(5_000),
    new BN(6_000),
    pool.config.tradeFeeRate,
    pool.config.creatorFeeRate,
    pool.config.protocolFeeRate,
    pool.config.fundFeeRate,
    true,
  );
  const quote = await engine.quote({
    type: "quote_request",
    request_id: "cpmm-fee",
    protocol: "raydium_cpmm",
    pool_id: "cpmm",
    input_mint: ZERO_KEY.toBase58(),
    output_mint: ZERO_KEY.toBase58(),
    input_amount_raw: input.toString(10),
  });
  assert.equal(quote.status, "ok");
  assert.equal(quote.output_amount_raw, expected.outputAmount.toString(10));
  assert.equal(quote.pool_fee_raw, expected.tradeFee.toString(10));
  pool.stateEmitter.dispose();
});

test("R-04 staged CPMM WS commit advances core without rolling newer vaults back", () => {
  const { engine, pool } = standardHarness();
  const generationBefore = pool.provenance.dependencyGeneration;
  pool.pending.set(105, {
    pool: account(Buffer.alloc(CpmmPoolInfoLayout.span)),
    vaultA: account(vaultData(100n), TOKEN_PROGRAM_ID),
    vaultB: account(vaultData(200n), TOKEN_PROGRAM_ID),
  });

  (engine as any).commit(pool, 105);

  const state = engine.cpmmSimulationState("cpmm");
  assert.equal(state.core_state_slot, 105);
  assert.equal(state.dependency_slot_min, 120);
  assert.equal(state.vault_a_raw, "5000");
  assert.equal(state.vault_b_raw, "6000");
  assert.equal(state.trade_fee_rate, "1000");
  assert.equal(pool.provenance.dependencyGeneration, generationBefore);
  const capture = engine.cpmmSimulationCapture("cpmm");
  assert.equal(
    capture.accounts.find((item) => item.role === "vault_a")?.data_base64,
    pool.vaultAAccountData.toString("base64"),
  );
  assert.equal(
    capture.accounts.find((item) => item.role === "vault_b")?.data_base64,
    pool.vaultBAccountData.toString("base64"),
  );
  pool.stateEmitter.dispose();
});

test("R-04 CPMM fresh, malformed and equal-slot snapshots commit atomically with immutable evidence", async () => {
  const { engine, pool, setSnapshot } = standardHarness();
  const coreData = Buffer.alloc(CpmmPoolInfoLayout.span);
  const freshConfig = configData(3_000n);
  const freshVaultA = vaultData(7_000n);
  const freshVaultB = vaultData(8_000n);
  setSnapshot(125, [
    account(coreData),
    account(freshVaultA, TOKEN_PROGRAM_ID),
    account(freshVaultB, TOKEN_PROGRAM_ID),
    account(freshConfig),
  ]);
  await (engine as any).refreshCore(pool);
  let state = engine.cpmmSimulationState("cpmm");
  assert.equal(state.core_state_slot, 125);
  assert.equal(state.dependency_slot_min, 125);
  assert.equal(state.trade_fee_rate, "3000");
  assert.equal(state.vault_a_raw, "7000");
  assert.equal(state.vault_b_raw, "8000");

  const capture = engine.cpmmSimulationCapture("cpmm");
  assert.ok(Object.isFrozen(capture));
  assert.ok(Object.isFrozen(capture.accounts));
  assert.deepEqual(capture.accounts.map((item) => [item.role, item.slot]), [
    ["core", 125],
    ["vault_a", 125],
    ["vault_b", 125],
    ["config", 125],
  ]);
  assert.equal(
    capture.accounts.find((item) => item.role === "config")?.data_base64,
    freshConfig.toString("base64"),
  );
  freshConfig.fill(0xff);
  assert.notEqual(
    capture.accounts.find((item) => item.role === "config")?.data_base64,
    freshConfig.toString("base64"),
  );

  const beforeMalformed = engine.cpmmSimulationState("cpmm");
  const generationBeforeMalformed = pool.provenance.dependencyGeneration;
  setSnapshot(130, [
    account(coreData),
    account(vaultData(9_000n), TOKEN_PROGRAM_ID),
    account(vaultData(10_000n), TOKEN_PROGRAM_ID),
    account(Buffer.alloc(4)),
  ]);
  await assert.rejects((engine as any).refreshCore(pool));
  state = engine.cpmmSimulationState("cpmm");
  assert.deepEqual(state, beforeMalformed);
  assert.equal(pool.provenance.dependencyGeneration, generationBeforeMalformed);

  const sameSlotConfig = configData(4_000n);
  (engine as any).updateCpmmConfig(pool, account(sameSlotConfig), { slot: 130 });
  assert.equal(pool.provenance.dependencySlot("config"), 130);
  const generationAt130 = pool.provenance.dependencyGeneration;
  setSnapshot(130, [
    account(coreData),
    account(vaultData(9_000n), TOKEN_PROGRAM_ID),
    account(vaultData(10_000n), TOKEN_PROGRAM_ID),
    account(configData(5_000n)),
  ]);
  await (engine as any).refreshCore(pool);
  state = engine.cpmmSimulationState("cpmm");
  assert.equal(state.core_state_slot, 130);
  assert.equal(state.trade_fee_rate, "4000", "same-slot config must remain first-writer-wins");
  assert.equal(pool.provenance.dependencyGeneration, generationAt130 + 2, "only two changed vaults advance");
  pool.stateEmitter.dispose();
});

type MaintenanceEngine = {
  maintainStalePools(nowMs?: number): Promise<void>;
  runtimeStats(): Record<string, number>;
};

function maintenanceEngines(): Array<[string, MaintenanceEngine]> {
  return [
    ["meteora", new MeteoraDlmmQuoteEngine({ onPoolState: () => undefined })],
    ["orca", new OrcaWhirlpoolQuoteEngine({ onPoolState: () => undefined })],
    ["clmm", new RaydiumClmmQuoteEngine({ onPoolState: () => undefined })],
    ["standard", new RaydiumStandardQuoteEngine({ onPoolState: () => undefined })],
  ];
}

test("O-02 every engine services all 100 stale pools without insertion-order starvation", async () => {
  const engines: Array<[string, MaintenanceEngine, (pool: any) => string]> = [
    ["meteora", new MeteoraDlmmQuoteEngine({ onPoolState: () => undefined }), (pool) => pool.descriptor.pool_id],
    ["orca", new OrcaWhirlpoolQuoteEngine({ onPoolState: () => undefined }), (pool) => pool.descriptor.pool_id],
    ["clmm", new RaydiumClmmQuoteEngine({ onPoolState: () => undefined }), (pool) => pool.descriptor.pool_id],
    ["standard", new RaydiumStandardQuoteEngine({ onPoolState: () => undefined }), (pool) => pool.descriptor.pool_id],
  ];
  for (const [name, engine, poolId] of engines) {
    const pools = new Map<string, any>();
    for (let index = 0; index < 100; index += 1) {
      pools.set(`pool-${index}`, {
        descriptor: { pool_id: `pool-${index}` },
        protocol: "raydium_cpmm",
        provenance: new PoolSlotProvenance(1, 0),
        stateEmitter: { stats: () => ({ pending: 0, coalesced_total: 0, external_emits_total: 0 }) },
        coreMailbox: { stats: () => ({ pending: 0, coalesced_total: 0, errors_total: 0 }) },
        retiredBinSubscriptionIds: new Set(),
        retiredTickSubscriptionIds: new Set(),
      });
    }
    const serviced = new Map<string, number>();
    Object.assign(engine as object, {
      pools,
      coreRefreshAfterMs: 15_000,
      refreshStaggerWindowMs: 5_000,
    });
    (engine as any).scheduleCoreRefresh = async (pool: any) => {
      const id = poolId(pool);
      serviced.set(id, (serviced.get(id) ?? 0) + 1);
      pool.provenance.noteRpcRefresh((engine as any).__now);
    };
    for (let tick = 0; tick < 600; tick += 1) {
      const now = 20_000 + tick * 1_000;
      (engine as any).__now = now;
      await engine.maintainStalePools(now);
    }
    assert.equal(serviced.size, 100, `${name} starved ${100 - serviced.size} pools`);
    assert.ok(Math.max(...serviced.values()) - Math.min(...serviced.values()) <= 1, `${name} was unfair`);
  }
});

test("O-02 rotating selection survives failure, removal/addition, fresh pools and reports backlog", async () => {
  const now = 100_000;
  for (const [name, engine] of maintenanceEngines()) {
    const pools = new Map<string, any>();
    for (let index = 0; index < 5; index += 1) {
      pools.set(`pool-${index}`, {
        descriptor: { pool_id: `pool-${index}` },
        protocol: "raydium_cpmm",
        provenance: new PoolSlotProvenance(1, 0),
        stateEmitter: { stats: () => ({ pending: 0, coalesced_total: 0, external_emits_total: 0 }) },
        coreMailbox: { stats: () => ({ pending: 0, coalesced_total: 0, errors_total: 0 }) },
        retiredBinSubscriptionIds: new Set(),
        retiredTickSubscriptionIds: new Set(),
      });
    }
    pools.set("fresh", {
      descriptor: { pool_id: "fresh" },
      protocol: "raydium_cpmm",
      provenance: new PoolSlotProvenance(1, now),
      stateEmitter: { stats: () => ({ pending: 0, coalesced_total: 0, external_emits_total: 0 }) },
      coreMailbox: { stats: () => ({ pending: 0, coalesced_total: 0, errors_total: 0 }) },
      retiredBinSubscriptionIds: new Set(),
      retiredTickSubscriptionIds: new Set(),
    });
    Object.assign(engine as object, {
      pools,
      coreRefreshAfterMs: 1_000,
      refreshStaggerWindowMs: 0,
    });
    const calls: string[] = [];
    let failFirst = true;
    (engine as any).scheduleCoreRefresh = async (pool: any) => {
      const id = pool.descriptor.pool_id as string;
      calls.push(id);
      if (id === "pool-0" && failFirst) {
        failFirst = false;
        throw new Error("synthetic refresh failure");
      }
      pool.provenance.noteRpcRefresh(now);
    };
    await assert.rejects(engine.maintainStalePools(now), /synthetic/u);
    await engine.maintainStalePools(now);
    pools.delete("pool-1");
    pools.set("pool-5", {
      descriptor: { pool_id: "pool-5" },
      protocol: "raydium_cpmm",
      provenance: new PoolSlotProvenance(1, 0),
      stateEmitter: { stats: () => ({ pending: 0, coalesced_total: 0, external_emits_total: 0 }) },
      coreMailbox: { stats: () => ({ pending: 0, coalesced_total: 0, errors_total: 0 }) },
      retiredBinSubscriptionIds: new Set(),
      retiredTickSubscriptionIds: new Set(),
    });
    for (let count = 0; count < 4; count += 1) await engine.maintainStalePools(now);
    assert.deepEqual(calls, ["pool-0", "pool-1", "pool-2", "pool-3", "pool-4", "pool-5"], name);
    assert.equal(calls.includes("fresh"), false, `${name} refreshed a fresh WS pool`);
    const stats = engine.runtimeStats();
    assert.ok(stats.maintenance_due_pool_count >= 1, `${name} lost due backlog accounting`);
    assert.ok(stats.maintenance_maximum_overdue_ms > 0, `${name} lost overdue age`);
  }
});

test("O-02 a slow in-flight pool is skipped by the next maintenance selection", async () => {
  for (const [name, engine] of maintenanceEngines()) {
    const pools = new Map<string, any>();
    for (let index = 0; index < 3; index += 1) {
      pools.set(`pool-${index}`, {
        descriptor: { pool_id: `pool-${index}` },
        protocol: "raydium_cpmm",
        provenance: new PoolSlotProvenance(1, 0),
      });
    }
    Object.assign(engine as object, {
      pools,
      coreRefreshAfterMs: 1_000,
      refreshStaggerWindowMs: 0,
    });
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const calls: string[] = [];
    (engine as any).scheduleCoreRefresh = async (pool: any) => {
      const id = pool.descriptor.pool_id as string;
      calls.push(id);
      pool.provenance.refreshInFlight = true;
      if (id === "pool-0") await gate;
      pool.provenance.refreshInFlight = false;
      pool.provenance.noteRpcRefresh(100_000);
    };
    const slow = engine.maintainStalePools(100_000);
    await new Promise<void>((resolve) => setImmediate(resolve));
    await engine.maintainStalePools(100_000);
    assert.deepEqual(calls, ["pool-0", "pool-1"], name);
    release();
    await slow;
  }
});

test("R-03/O-03 100,000 callbacks per dependency engine keep bounded emits and exact final provenance", () => {
  const poolCount = 10;
  const updateCount = 100_000;

  const meteoraNotices: any[] = [];
  const meteora = new MeteoraDlmmQuoteEngine({ onPoolState: (notice) => meteoraNotices.push(notice) });
  (meteora as any).decodeBinAccount = (_pool: unknown, value: AccountInfo<Buffer>) => ({
    marker: value.data.readUInt32LE(0),
  });
  const meteoraPools: any[] = [];
  for (let index = 0; index < poolCount; index += 1) {
    const dependency = new PublicKey(Buffer.alloc(32, index + 20));
    const pool: any = {
      descriptor: { pool_id: `meteora-${index}`, label: `Meteora ${index}` },
      dlmm: {
        program: {},
        lbPair: { activeId: 0, binStep: 1 },
        tokenX: { mint: { address: ZERO_KEY, decimals: 6 } },
        tokenY: { mint: { address: dependency, decimals: 6 } },
      },
      provenance: new PoolSlotProvenance(100, 0),
      binArrays: new Map(),
      binAccountData: new Map(),
      binCacheAtMs: 0,
      stateEmitter: undefined,
    };
    pool.stateEmitter = new DebouncedStateEmitter(
      () => (meteora as any).emitPoolStateNow(pool),
      100_000,
    );
    pool.dependency = dependency;
    meteoraPools.push(pool);
  }
  for (let index = 0; index < updateCount; index += 1) {
    const pool = meteoraPools[index % poolCount];
    const slot = Math.floor(index / poolCount) + 1;
    const data = Buffer.allocUnsafe(4);
    data.writeUInt32LE(slot, 0);
    (meteora as any).updateBin(pool, pool.dependency, account(data), { slot });
  }
  for (const pool of meteoraPools) {
    assert.equal(pool.stateEmitter.stats().pending, 1);
    assert.equal(pool.provenance.dependencyGeneration, updateCount / poolCount);
    assert.equal(pool.provenance.dependencySlot(pool.dependency.toBase58()), updateCount / poolCount);
    assert.equal(pool.binArrays.get(pool.dependency.toBase58()).account.marker, updateCount / poolCount);
    pool.stateEmitter.flushForTest();
    pool.stateEmitter.dispose();
  }
  assert.equal(meteoraNotices.length, poolCount);
  assert.ok(meteoraPools.reduce((sum, pool) => sum + pool.stateEmitter.stats().coalesced_total, 0) >= 99_000);

  const orcaNotices: any[] = [];
  const orca = new OrcaWhirlpoolQuoteEngine({ onPoolState: (notice) => orcaNotices.push(notice) });
  (orca as any).decodeTickAccount = (_address: PublicKey, value: AccountInfo<Buffer>) => (
    tickData(value.data.readUInt32LE(0))
  );
  const orcaPools: any[] = [];
  for (let index = 0; index < poolCount; index += 1) {
    const dependency = new PublicKey(Buffer.alloc(32, index + 40));
    const pool: any = {
      descriptor: { pool_id: `orca-${index}`, label: `Orca ${index}` },
      data: orcaPoolData(),
      tokenExtensionContext: {
        tokenMintWithProgramA: { decimals: 6 },
        tokenMintWithProgramB: { decimals: 6 },
      },
      provenance: new PoolSlotProvenance(100, 0),
      tickArraysAToB: [{ address: dependency, data: tickData(0) }],
      tickArraysBToA: [{ address: dependency, data: tickData(0) }],
      tickAccountData: new Map(),
      tickCacheAtMs: 0,
      stateEmitter: undefined,
    };
    pool.stateEmitter = new DebouncedStateEmitter(
      () => (orca as any).emitPoolStateNow(pool),
      100_000,
    );
    pool.dependency = dependency;
    orcaPools.push(pool);
  }
  for (let index = 0; index < updateCount; index += 1) {
    const pool = orcaPools[index % poolCount];
    const slot = Math.floor(index / poolCount) + 1;
    const data = Buffer.allocUnsafe(4);
    data.writeUInt32LE(slot, 0);
    (orca as any).updateTick(pool, pool.dependency, account(data), { slot });
  }
  for (const pool of orcaPools) {
    assert.equal(pool.stateEmitter.stats().pending, 1);
    assert.equal(pool.provenance.dependencyGeneration, updateCount / poolCount);
    assert.equal(pool.provenance.dependencySlot(pool.dependency.toBase58()), updateCount / poolCount);
    assert.equal(pool.tickArraysAToB[0].data.startTickIndex, updateCount / poolCount);
    pool.stateEmitter.flushForTest();
    pool.stateEmitter.dispose();
  }
  assert.equal(orcaNotices.length, poolCount);
  assert.ok(orcaPools.reduce((sum, pool) => sum + pool.stateEmitter.stats().coalesced_total, 0) >= 99_000);
});

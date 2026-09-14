import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import test from "node:test";

import {
  ProtocolEmitter,
  ProtocolOutputDrainTimeoutError,
  ProtocolOutputOverflowError,
  type ProtocolWritable,
} from "../src/outputWriter.js";

class ManualDrainWritable extends EventEmitter implements ProtocolWritable {
  public readonly writes: string[] = [];
  private blocked = false;

  public constructor(private readonly blockOnWrite: number) {
    super();
  }

  public write(chunk: string): boolean {
    assert.equal(this.blocked, false, "emitter wrote again before drain");
    this.writes.push(chunk);
    if (this.writes.length === this.blockOnWrite) {
      this.blocked = true;
      return false;
    }
    return true;
  }

  public release(): void {
    assert.equal(this.blocked, true);
    this.blocked = false;
    this.emit("drain");
  }
}

function parsedLines(writable: ManualDrainWritable): Array<Record<string, unknown>> {
  return writable.writes.map((line) => JSON.parse(line) as Record<string, unknown>);
}

test("stdout backpressure coalesces 100k state updates to latest values for 20 keys", async () => {
  const writable = new ManualDrainWritable(1);
  const emitter = new ProtocolEmitter(writable, { maxStatePendingKeys: 20 });

  emitter.emitState("pool:0", { type: "pool_state", pool_id: "pool-0", sequence: -1 });
  for (let sequence = 0; sequence < 100_000; sequence += 1) {
    const pool = sequence % 20;
    emitter.emitState(
      `pool:${pool}`,
      { type: "pool_state", pool_id: `pool-${pool}`, sequence },
    );
  }

  const blocked = emitter.metrics();
  assert.equal(writable.writes.length, 1);
  assert.equal(blocked.stdout_blocked, true);
  assert.equal(blocked.stdout_state_pending_keys, 20);
  assert.equal(blocked.stdout_state_coalesced_total, 99_980);

  writable.release();
  await emitter.drainAndClose();
  const messages = parsedLines(writable);
  assert.equal(messages.length, 21, "superseded states must not be serialized or written");
  const latest = new Map<string, number>();
  for (const message of messages) {
    latest.set(String(message.pool_id), Number(message.sequence));
  }
  for (let pool = 0; pool < 20; pool += 1) {
    const expected = 99_999 - ((99_999 - pool) % 20);
    assert.equal(latest.get(`pool-${pool}`), expected);
  }
  assert.equal(emitter.metrics().stdout_state_pending_keys, 0);
  assert.equal(emitter.metrics().stdout_drain_total, 1);
});

test("lossless results retain FIFO order and cannot permanently starve state", async () => {
  const writable = new ManualDrainWritable(1);
  const emitter = new ProtocolEmitter(writable, { losslessBurst: 8 });
  emitter.emitState("pool:blocked", { type: "pool_state", sequence: 0 });
  for (let sequence = 0; sequence < 40; sequence += 1) {
    emitter.emitLossless({ type: "quote_result", sequence });
  }
  emitter.emitState("pool:latest", { type: "pool_state", sequence: 99 });

  writable.release();
  await emitter.drainAndClose();
  const messages = parsedLines(writable);
  const results = messages.filter((message) => message.type === "quote_result");
  assert.deepEqual(results.map((message) => message.sequence), [...Array(40).keys()]);
  const latestStateIndex = messages.findIndex((message) => message.sequence === 99);
  assert.ok(latestStateIndex > 0 && latestStateIndex <= 9);
});

test("lossless overflow takes an explicit fatal path at the hard cap", () => {
  const writable = new ManualDrainWritable(1);
  let fatal: Error | undefined;
  const emitter = new ProtocolEmitter(writable, {
    maxLosslessQueue: 2,
    onFatal: (error) => { fatal = error; },
  });
  assert.equal(emitter.emitLossless({ type: "ready" }), true);
  assert.equal(emitter.emitLossless({ type: "quote_result", sequence: 1 }), true);
  assert.equal(emitter.emitLossless({ type: "quote_result", sequence: 2 }), true);
  assert.equal(emitter.emitLossless({ type: "quote_result", sequence: 3 }), false);

  assert.ok(fatal instanceof ProtocolOutputOverflowError);
  assert.equal(fatal.outputQueue, "lossless");
  assert.equal(emitter.metrics().stdout_lossless_queue_high_watermark, 2);
  assert.equal(emitter.metrics().stdout_lossless_overflow_total, 1);
});

test("shutdown fails within its timeout when downstream never drains", async () => {
  const writable = new ManualDrainWritable(1);
  let fatal: Error | undefined;
  const emitter = new ProtocolEmitter(writable, {
    onFatal: (error) => { fatal = error; },
  });
  emitter.emitLossless({ type: "ready" });
  emitter.emitLossless({ type: "quote_result" });
  await assert.rejects(
    emitter.drainAndClose(10),
    ProtocolOutputDrainTimeoutError,
  );
  assert.ok(fatal instanceof ProtocolOutputDrainTimeoutError);
});

test("write failures are counted and become fatal", () => {
  let fatal: Error | undefined;
  const writable: ProtocolWritable = {
    write(): boolean { throw new Error("broken pipe"); },
    once(): void {},
  };
  const emitter = new ProtocolEmitter(writable, {
    onFatal: (error) => { fatal = error; },
  });
  assert.equal(emitter.emitLossless({ type: "ready" }), false);
  assert.ok(fatal instanceof Error);
  assert.equal(emitter.metrics().stdout_write_failures_total, 1);
});

import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import test from "node:test";

import {
  ProtocolEmitter,
  type ProtocolWritable,
} from "../src/outputWriter.js";

class ManualDrainWritable extends EventEmitter implements ProtocolWritable {
  public readonly writes: string[] = [];
  private blocked = false;

  public constructor(private readonly blockOnWrite: number) {
    super();
  }

  public write(chunk: string): boolean {
    this.writes.push(chunk);
    if (this.blockOnWrite > 0 && this.writes.length === this.blockOnWrite) {
      this.blocked = true;
      return false;
    }
    return !this.blocked;
  }

  public release(): void {
    this.blocked = false;
    this.emit("drain");
  }
}

test("F01: backpressure coerces repeated worker_stats emits into one pending key", () => {
  // Block after the first write so state messages queue up and coalesce.
  const writable = new ManualDrainWritable(1);
  const emitter = new ProtocolEmitter(writable, {
    maxLosslessQueue: 100,
    maxStatePendingKeys: 100,
  });

  for (let i = 0; i < 100; i++) {
    emitter.emitState("worker_stats", {
      type: "worker_stats",
      boot_id: "boot-f",
      source_epoch: 1,
      memory: {
        rss_bytes: 1000 + i,
        heap_total_bytes: 2000,
        heap_used_bytes: 3000,
        external_bytes: 4000,
        array_buffers_bytes: 5000,
        uptime_seconds: 10,
      },
      stdout: {
        blocked: false,
        lossless_queue_size: 0,
        state_pending_keys: 1,
        state_coalesced_total: i,
      },
      rpc: {
        queue_total: 0,
        queue_interactive: 0,
        queue_bootstrap: 0,
        queue_refresh: 0,
        active: 0,
        queue_high_watermark: 0,
      },
      pools: {},
      sampled_at_iso: "2026-01-01T00:00:00.000Z",
    });
  }

  const metrics = emitter.metrics();
  assert.equal(metrics.stdout_state_pending_keys, 1);
  assert.ok(metrics.stdout_state_coalesced_total >= 98);
  writable.release();
});

test("F02: worker_stats message contains all required fields with correct types", () => {
  const writable = new ManualDrainWritable(0);
  const emitter = new ProtocolEmitter(writable, {
    maxLosslessQueue: 100,
    maxStatePendingKeys: 100,
  });

  emitter.emitState("worker_stats", {
    type: "worker_stats" as const,
    boot_id: "boot-f",
    source_epoch: 1,
    memory: {
      rss_bytes: 1_000_000,
      heap_total_bytes: 2_000_000,
      heap_used_bytes: 1_500_000,
      external_bytes: 500_000,
      array_buffers_bytes: 100_000,
      uptime_seconds: 120.5,
    },
    stdout: {
      blocked: false,
      lossless_queue_size: 2,
      state_pending_keys: 3,
      state_coalesced_total: 5,
    },
    rpc: {
      queue_total: 1,
      queue_interactive: 0,
      queue_bootstrap: 1,
      queue_refresh: 0,
      active: 2,
      queue_high_watermark: 3,
    },
    pools: {
      raydium_cpmm: {
        pool_count: 3,
        refresh_inflight: 1,
        coalesced_core_updates_total: 10,
        external_pool_state_emits_total: 20,
      },
    },
    sampled_at_iso: "2026-01-01T00:00:00.000Z",
  });

  const metrics = emitter.metrics();
  assert.equal(metrics.stdout_state_messages_written_total, 1);
  const written = JSON.parse(writable.writes[0]);
  assert.equal(written.type, "worker_stats");
  assert.equal(typeof written.memory.rss_bytes, "number");
  assert.equal(typeof written.memory.uptime_seconds, "number");
  assert.equal(typeof written.stdout.blocked, "boolean");
  assert.equal(typeof written.rpc.queue_high_watermark, "number");
  assert.equal(typeof written.pools.raydium_cpmm.pool_count, "number");
});

test("F03: process.memoryUsage returns valid numbers (no NaN/negative)", () => {
  const memory = process.memoryUsage();
  assert.ok(!Number.isNaN(memory.rss));
  assert.ok(!Number.isNaN(memory.heapTotal));
  assert.ok(!Number.isNaN(memory.heapUsed));
  assert.ok(!Number.isNaN(memory.external));
  assert.ok(!Number.isNaN(memory.arrayBuffers));
  assert.ok(memory.rss > 0);
  assert.ok(memory.heapTotal > 0);
  assert.ok(memory.heapUsed >= 0);
  assert.ok(memory.external >= 0);
  assert.ok(memory.arrayBuffers >= 0);
});

test("F04: backpressure-aware writer coerces 1000 stats updates into one key", () => {
  const writable = new ManualDrainWritable(1);
  const emitter = new ProtocolEmitter(writable, {
    maxLosslessQueue: 1_000_000,
    maxStatePendingKeys: 1_000_000,
  });

  for (let i = 0; i < 1_000; i++) {
    emitter.emitState("worker_stats", {
      type: "worker_stats",
      seq: i,
      memory: { rss_bytes: i, heap_total_bytes: 0, heap_used_bytes: 0, external_bytes: 0, array_buffers_bytes: 0, uptime_seconds: 0 },
      stdout: { blocked: true, lossless_queue_size: 0, state_pending_keys: 1, state_coalesced_total: 0 },
      rpc: { queue_total: 0, queue_interactive: 0, queue_bootstrap: 0, queue_refresh: 0, active: 0, queue_high_watermark: 0 },
      pools: {},
      sampled_at_iso: "2026-01-01T00:00:00.000Z",
    });
  }

  const metrics = emitter.metrics();
  assert.equal(metrics.stdout_state_pending_keys, 1);
  assert.ok(metrics.stdout_state_coalesced_total >= 998);
});

test("F05: worker_stats_request message parses correctly", async () => {
  const { parseWorkerInput } = await import("../src/protocol.js");
  const parsed = parseWorkerInput({
    type: "worker_stats_request",
    request_id: "stats-1",
  });
  assert.equal(parsed.type, "worker_stats_request");
  assert.equal(parsed.request_id, "stats-1");
});

test("F06: worker_stats_request without request_id is rejected", async () => {
  const { parseWorkerInput } = await import("../src/protocol.js");
  assert.throws(
    () => parseWorkerInput({ type: "worker_stats_request", request_id: "" }),
    /request_id/,
  );
});

test("F07: worker_stats message round-trips through JSON serialization", () => {
  const raw = {
    type: "worker_stats",
    boot_id: "boot-f",
    source_epoch: 42,
    memory: {
      rss_bytes: 50_000_000,
      heap_total_bytes: 30_000_000,
      heap_used_bytes: 20_000_000,
      external_bytes: 10_000_000,
      array_buffers_bytes: 5_000_000,
      uptime_seconds: 3600,
    },
    stdout: {
      blocked: false,
      lossless_queue_size: 5,
      state_pending_keys: 1,
      state_coalesced_total: 50,
    },
    rpc: {
      queue_total: 3,
      queue_interactive: 1,
      queue_bootstrap: 1,
      queue_refresh: 1,
      active: 2,
      queue_high_watermark: 3,
    },
    pools: {
      raydium_cpmm: {
        pool_count: 2,
        refresh_inflight: 0,
        coalesced_core_updates_total: 5,
        external_pool_state_emits_total: 10,
      },
    },
    sampled_at_iso: "2026-09-17T00:00:00.000Z",
  };

  const serialized = JSON.stringify(raw);
  const parsed = JSON.parse(serialized);
  assert.equal(parsed.type, "worker_stats");
  assert.equal(parsed.memory.rss_bytes, 50_000_000);
  assert.equal(parsed.stdout.blocked, false);
  assert.equal(parsed.rpc.queue_total, 3);
  assert.equal(parsed.pools.raydium_cpmm.pool_count, 2);
  assert.equal(parsed.sampled_at_iso, "2026-09-17T00:00:00.000Z");
});

test("F08: rpc_queue_total equals sum of priority sub-queues", async () => {
  const { rpcSchedulerMetrics } = await import("../src/rpcPacer.js");
  const metrics = rpcSchedulerMetrics();
  assert.equal(
    metrics.rpc_queue_total,
    metrics.rpc_queue_interactive + metrics.rpc_queue_bootstrap + metrics.rpc_queue_refresh,
  );
});

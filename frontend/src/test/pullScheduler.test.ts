import { describe, expect, it } from "vitest";
import type { PullMsgsReq } from "../../goim-ws-types";
import { createPullScheduler } from "../realtime/pullScheduler";

function setup(initialSynced = 0) {
  const sent: PullMsgsReq[] = [];
  const synced = { value: initialSynced };
  let clock = 0;
  const scheduler = createPullScheduler(
    (req) => { sent.push(req); return true; },
    () => synced.value,
    () => clock,
  );
  return { sent, synced, scheduler, tick: (ms: number) => { clock += ms; } };
}

describe("pull scheduler", () => {
  it("keeps a single in-flight pull per conversation and merges later requests", () => {
    const { sent, synced, scheduler } = setup(1);
    scheduler.request({ convId: "p_1_2", fromSeq: 2, toSeq: 4 });
    scheduler.request({ convId: "p_1_2", fromSeq: 2, toSeq: 5 });
    scheduler.request({ convId: "p_1_2", fromSeq: 2, toSeq: 6 });
    expect(sent).toHaveLength(1);

    synced.value = 4;
    scheduler.complete("p_1_2", null);
    expect(sent[1]).toEqual({ convId: "p_1_2", fromSeq: 5, toSeq: 6 });
  });

  it("drops queued requests already covered by the finished pull", () => {
    const { sent, synced, scheduler } = setup(1);
    scheduler.request({ convId: "p_1_2", fromSeq: 2, toSeq: 4 });
    scheduler.request({ convId: "p_1_2", fromSeq: 2, toSeq: 3 });
    synced.value = 4;
    scheduler.complete("p_1_2", null);
    expect(sent).toHaveLength(1);
  });

  it("follows hasMore pages before flushing queued requests", () => {
    const { sent, synced, scheduler } = setup(0);
    scheduler.request({ convId: "g_9", fromSeq: 1, toSeq: 250 });
    scheduler.request({ convId: "g_9", fromSeq: 1, toSeq: 260 });
    synced.value = 100;
    scheduler.complete("g_9", { convId: "g_9", fromSeq: 101, toSeq: 0 });
    expect(sent[1]).toEqual({ convId: "g_9", fromSeq: 101, toSeq: 0 });
    synced.value = 255;
    scheduler.complete("g_9", null);
    expect(sent[2]).toEqual({ convId: "g_9", fromSeq: 256, toSeq: 260 });
  });

  it("does not block other conversations and recovers after reset or timeout", () => {
    const { sent, scheduler, tick } = setup(0);
    scheduler.request({ convId: "a", fromSeq: 1, toSeq: 2 });
    scheduler.request({ convId: "b", fromSeq: 1, toSeq: 2 });
    expect(sent).toHaveLength(2);

    scheduler.reset();
    scheduler.request({ convId: "a", fromSeq: 1, toSeq: 3 });
    expect(sent).toHaveLength(3);

    tick(10_001);
    scheduler.request({ convId: "a", fromSeq: 1, toSeq: 4 });
    expect(sent).toHaveLength(4);
  });
});

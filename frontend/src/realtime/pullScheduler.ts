import type { PullMsgsReq } from "../../goim-ws-types";

/** 在途请求超过这个时间仍未返回，视为丢失，允许重新发起 */
const INFLIGHT_TIMEOUT_MS = 10_000;

/**
 * 补拉调度：同一会话同一时刻最多一个 pullMsgs 在途。
 *
 * 在途期间新产生的补拉请求（连续推送里的空洞、serverAck 不连续等）不立即发送，只保留最后一个；
 * 在途请求返回后，按当时的 syncedSeq 重新计算区间，已覆盖的直接丢弃。
 * 这样空洞或乱序推送不会对同一区间重复发起补拉。
 */
export interface PullScheduler {
  request: (req: PullMsgsReq | null) => void;
  /** 收到 pullMsgsResp 后调用；next 为 hasMore 时的下一页请求 */
  complete: (convId: string, next: PullMsgsReq | null) => void;
  /** 断线重连时清空：在途请求的响应可能已丢失 */
  reset: () => void;
}

export function createPullScheduler(
  send: (req: PullMsgsReq) => boolean,
  syncedSeqOf: (convId: string) => number,
  now: () => number = Date.now,
): PullScheduler {
  const inflight = new Map<string, number>();
  const queued = new Map<string, PullMsgsReq>();

  const dispatch = (req: PullMsgsReq) => {
    if (send(req)) inflight.set(req.convId, now());
  };

  return {
    request: (req) => {
      if (!req) return;
      const startedAt = inflight.get(req.convId);
      if (startedAt !== undefined && now() - startedAt < INFLIGHT_TIMEOUT_MS) {
        queued.set(req.convId, req);
        return;
      }
      queued.delete(req.convId);
      dispatch(req);
    },
    complete: (convId, next) => {
      inflight.delete(convId);
      if (next) {
        dispatch(next);
        return;
      }
      const pending = queued.get(convId);
      queued.delete(convId);
      if (!pending) return;
      const synced = syncedSeqOf(convId);
      if (pending.toSeq > 0 && pending.toSeq <= synced) return;
      dispatch({ convId, fromSeq: synced > 0 ? synced + 1 : pending.fromSeq, toSeq: pending.toSeq });
    },
    reset: () => {
      inflight.clear();
      queued.clear();
    },
  };
}

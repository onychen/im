import { act, cleanup, render } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { HistoryResponse } from "../../goim-api-types";
import type { ConvView, WsMessage } from "../../goim-ws-types";
import { RealtimeBootstrap } from "../components/realtime/RealtimeBootstrap";
import { conversationsApi, friendsApi, groupsApi, settingsApi } from "../lib/api";
import { goimSocket } from "../realtime/socket";
import { useAuthStore } from "../stores/authStore";
import { useChatStore } from "../stores/chatStore";

vi.mock("../realtime/notifications", () => ({ configureNotifications: vi.fn(), notifyIncomingMessage: vi.fn() }));

const view: ConvView = {
  convId: "g_1", convType: 2, targetId: 1, targetName: "Group", targetAvatar: "",
  maxSeq: 1001, readSeq: 0, minSeq: 1, unread: 1, lastMsgBrief: "latest", lastMsgTime: 1001, pinned: false, muted: false,
};

function message(seq: number): WsMessage {
  return { convId: "g_1", convType: 2, seq, msgId: String(seq), fromId: 2, msgType: 1, content: String(seq), status: 0, timestamp: seq };
}

function deferredPage() {
  let resolve!: (page: HistoryResponse) => void;
  const promise = new Promise<HistoryResponse>((done) => { resolve = done; });
  return { promise, resolve };
}

describe("initial history and realtime synchronization", () => {
  let handlers: Parameters<typeof goimSocket.setHandlers>[0];

  beforeEach(() => {
    vi.useFakeTimers();
    vi.spyOn(goimSocket, "setHandlers").mockImplementation((value) => { handlers = value; });
    vi.spyOn(goimSocket, "connect").mockImplementation(() => undefined);
    vi.spyOn(goimSocket, "disconnect").mockImplementation(() => undefined);
    vi.spyOn(goimSocket, "send").mockReturnValue(true);
    vi.spyOn(friendsApi, "list").mockResolvedValue({ items: [], pagination: { limit: 100, offset: 0, total: 0, has_more: false } });
    vi.spyOn(groupsApi, "list").mockResolvedValue([]);
    vi.spyOn(settingsApi, "get").mockResolvedValue({ mute_list: "[]" } as Awaited<ReturnType<typeof settingsApi.get>>);
    useChatStore.getState().resetSession();
    useAuthStore.setState({ accessToken: "test-token", user: { id: 1, username: "alice" }, previewMode: false });
  });

  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  async function connect() {
    await act(async () => { render(<RealtimeBootstrap />); });
    act(() => { handlers.onStateChange("connected"); });
  }

  it("uses HTTP history and supplements pushes received while it is loading", async () => {
    const page = deferredPage();
    const history = vi.spyOn(conversationsApi, "history").mockReturnValue(page.promise);
    await connect();
    act(() => {
      handlers.onMessage({ type: "convSync", data: { conversations: [view] } });
      handlers.onMessage({ type: "msg", data: message(1002) });
    });
    expect(history).toHaveBeenCalledWith("g_1", { beforeSeq: 0, limit: 50 });
    expect(goimSocket.send).not.toHaveBeenCalledWith(expect.objectContaining({ type: "pullMsgs" }));
    await act(async () => { page.resolve({ msgs: [message(12), message(1001)], hasMore: false }); });
    expect(goimSocket.send).toHaveBeenCalledWith({ type: "pullMsgs", data: { convId: "g_1", fromSeq: 1002, toSeq: 1002 } });
    expect(useChatStore.getState().messagesByConversation.g_1.map((item) => item.seq)).toEqual([12, 1001, 1002]);
  });

  it("retries a failed initial window without using a numeric lookback", async () => {
    const history = vi.spyOn(conversationsApi, "history")
      .mockRejectedValueOnce(new Error("temporary failure"))
      .mockResolvedValue({ msgs: [message(1001)], hasMore: false });
    await connect();
    await act(async () => { handlers.onMessage({ type: "convSync", data: { conversations: [view] } }); });
    expect(useChatStore.getState().conversations[0].initialHistoryLoaded).not.toBe(true);
    await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
    expect(history).toHaveBeenCalledTimes(2);
    expect(useChatStore.getState().conversations[0]).toMatchObject({ initialHistoryLoaded: true, syncedSeq: 1001 });
  });

  it("ignores a previous connection's delayed history response", async () => {
    const oldPage = deferredPage();
    vi.spyOn(conversationsApi, "history").mockReturnValueOnce(oldPage.promise)
      .mockResolvedValueOnce({ msgs: [message(1001)], hasMore: false });
    await connect();
    act(() => { handlers.onMessage({ type: "convSync", data: { conversations: [view] } }); });
    await act(async () => {
      handlers.onStateChange("connected");
      handlers.onMessage({ type: "convSync", data: { conversations: [view] } });
    });
    await act(async () => { oldPage.resolve({ msgs: [message(5)], hasMore: true }); });
    expect(useChatStore.getState().conversations[0].syncedSeq).toBe(1001);
    expect(useChatStore.getState().messagesByConversation.g_1.map((item) => item.seq)).toEqual([1001]);
    expect(useChatStore.getState().historyHasMore.g_1).toBe(false);
  });
});

import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ConvView, PullMsgsResp, WsMessage } from "../../goim-ws-types";
import { goimSocket } from "../realtime/socket";
import { useChatStore } from "../stores/chatStore";

function wsMsg(partial: Partial<WsMessage> & Pick<WsMessage, "convId" | "seq" | "fromId">): WsMessage {
  return {
    msgId: String(partial.seq * 1000),
    convType: partial.convId.startsWith("g_") ? 2 : 1,
    msgType: 1,
    content: `m${partial.seq}`,
    status: 0,
    timestamp: 1000 + partial.seq,
    ...partial,
  };
}

function pullResp(partial: Partial<PullMsgsResp> & Pick<PullMsgsResp, "convId" | "fromSeq" | "toSeq">): PullMsgsResp {
  return { watermark: partial.toSeq, msgs: [], hasMore: false, ...partial };
}

function convView(partial: Partial<ConvView> & Pick<ConvView, "convId" | "maxSeq">): ConvView {
  return {
    convType: partial.convId.startsWith("g_") ? 2 : 1,
    targetId: 2,
    targetName: "",
    targetAvatar: "",
    readSeq: 0,
    minSeq: 0,
    unread: 0,
    lastMsgBrief: "",
    lastMsgTime: 0,
    pinned: false,
    muted: false,
    ...partial,
  };
}

describe("chat message state", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.restoreAllMocks();
    vi.spyOn(goimSocket, "send").mockReturnValue(true);
    useChatStore.setState({ mode: null, liveUserId: null, connectionState: "idle", syncCompleted: false, conversations: [], messagesByConversation: {}, historyHasMore: {}, toasts: [] });
  });

  it("hydrates the preview conversations", () => {
    useChatStore.getState().initializePreview();
    expect(useChatStore.getState().conversations.length).toBeGreaterThan(0);
    expect(useChatStore.getState().messagesByConversation["lin-cheng"]).toHaveLength(4);
  });

  it("moves a preview message from pending to sent after acknowledgement", () => {
    useChatStore.getState().initializePreview();
    const conversation = useChatStore.getState().conversations[0];
    useChatStore.getState().sendText(conversation, "hello", true);
    const pending = useChatStore.getState().messagesByConversation[conversation.id];
    expect(pending[pending.length - 1].status).toBe("pending");
    vi.advanceTimersByTime(500);
    const sent = useChatStore.getState().messagesByConversation[conversation.id];
    // 预览消息也带 seq，新消息的 seq 接在其后，排序后位于末尾。
    expect(sent[sent.length - 1]).toMatchObject({ status: "sent", seq: conversation.maxSeq + 1, content: "hello" });
  });

  it("moves the conversation to the top as soon as a local message is sent", () => {
    useChatStore.getState().initializePreview();
    const target = useChatStore.getState().conversations[2];

    useChatStore.getState().sendText(target, "hello", true);

    expect(useChatStore.getState().conversations[0]).toMatchObject({
      id: target.id,
      preview: "hello",
    });
  });

  it("requests the latest window for a conversation seen for the first time", () => {
    useChatStore.getState().initializeLive(1);
    const pulls = useChatStore.getState().applyConvSync([convView({ convId: "p_1_2", maxSeq: 120, readSeq: 100, unread: 20 })]);
    expect(pulls).toEqual([{ convId: "p_1_2", fromSeq: 71, toSeq: 120 }]);
    expect(useChatStore.getState().conversations[0]).toMatchObject({ unread: 20, maxSeq: 120, readSeq: 100, syncedSeq: 0 });
  });

  it("only pulls the missing range after reconnecting", () => {
    useChatStore.getState().initializeLive(1);
    useChatStore.getState().applyConvSync([convView({ convId: "p_1_2", maxSeq: 3 })]);
    useChatStore.getState().applyPulledMsgs(pullResp({ convId: "p_1_2", fromSeq: 1, toSeq: 3, msgs: [1, 2, 3].map((seq) => wsMsg({ convId: "p_1_2", seq, fromId: 2 })) }), 1);
    expect(useChatStore.getState().conversations[0].syncedSeq).toBe(3);

    const pulls = useChatStore.getState().applyConvSync([convView({ convId: "p_1_2", maxSeq: 5 })]);
    expect(pulls).toEqual([{ convId: "p_1_2", fromSeq: 4, toSeq: 5 }]);
  });

  it("deduplicates pulled and pushed messages by seq", () => {
    useChatStore.getState().initializeLive(1);
    useChatStore.getState().applyConvSync([convView({ convId: "p_1_2", maxSeq: 1 })]);
    const resp = pullResp({ convId: "p_1_2", fromSeq: 1, toSeq: 1, msgs: [wsMsg({ convId: "p_1_2", seq: 1, fromId: 2, content: "once" })] });
    useChatStore.getState().applyPulledMsgs(resp, 1);
    useChatStore.getState().applyPulledMsgs(resp, 1);
    expect(useChatStore.getState().receiveMessage(wsMsg({ convId: "p_1_2", seq: 1, fromId: 2, content: "once" }), 1)).toBeNull();
    expect(useChatStore.getState().messagesByConversation["p_1_2"]).toHaveLength(1);
  });

  it("continues pulling from toSeq + 1 when the response has more", () => {
    useChatStore.getState().initializeLive(1);
    useChatStore.getState().applyConvSync([convView({ convId: "g_9", maxSeq: 250 })]);
    const next = useChatStore.getState().applyPulledMsgs(pullResp({ convId: "g_9", fromSeq: 1, toSeq: 100, watermark: 250, hasMore: true }), 1);
    expect(next).toEqual({ convId: "g_9", fromSeq: 101, toSeq: 0 });
  });

  it("advances syncedSeq across holes below the watermark but never beyond it", () => {
    useChatStore.getState().initializeLive(1);
    useChatStore.getState().receiveMessage(wsMsg({ convId: "p_1_2", seq: 1, fromId: 2 }), 1);
    // seq=2 是 Kafka 写失败留下的空洞；补拉 2..3 只返回 3，水位为 3。
    useChatStore.getState().applyPulledMsgs(pullResp({ convId: "p_1_2", fromSeq: 2, toSeq: 3, watermark: 3, msgs: [wsMsg({ convId: "p_1_2", seq: 3, fromId: 2 })] }), 1);
    expect(useChatStore.getState().conversations[0].syncedSeq).toBe(3);

    // 防御：即便响应的 toSeq 超过水位，也只推进到水位。
    useChatStore.getState().applyPulledMsgs(pullResp({ convId: "p_1_2", fromSeq: 4, toSeq: 9, watermark: 5 }), 1);
    expect(useChatStore.getState().conversations[0]).toMatchObject({ syncedSeq: 5, maxSeq: 5 });
  });

  it("requests a gap pull when a pushed seq is not contiguous", () => {
    useChatStore.getState().initializeLive(1);
    useChatStore.getState().receiveMessage(wsMsg({ convId: "p_1_2", seq: 1, fromId: 2 }), 1);
    const gap = useChatStore.getState().receiveMessage(wsMsg({ convId: "p_1_2", seq: 4, fromId: 2 }), 1);
    expect(gap).toEqual({ convId: "p_1_2", fromSeq: 2, toSeq: 4 });
    expect(useChatStore.getState().conversations[0]).toMatchObject({ syncedSeq: 1, maxSeq: 4 });
  });

  it("keeps confirmed messages ordered by seq and pending messages after them", () => {
    useChatStore.getState().initializeLive(1);
    useChatStore.getState().addPrivateConversation("p_1_2", 2, "Bob");
    useChatStore.getState().sendText(useChatStore.getState().conversations[0], "pending", false);
    // 服务端消息的时间戳早于本地待确认消息，但 seq 更大；旧实现会按时间把它排到待确认消息之前或之后不定。
    useChatStore.getState().receiveMessage(wsMsg({ convId: "p_1_2", seq: 2, fromId: 2, timestamp: 1 }), 1);
    useChatStore.getState().receiveMessage(wsMsg({ convId: "p_1_2", seq: 1, fromId: 2, timestamp: 9_999_999_999_999 }), 1);
    const order = useChatStore.getState().messagesByConversation["p_1_2"].map((item) => item.seq ?? "pending");
    expect(order).toEqual([1, 2, "pending"]);
  });

  it("assigns the acknowledged seq to the pending message", () => {
    useChatStore.getState().initializeLive(1);
    useChatStore.getState().addPrivateConversation("p_1_2", 2, "Bob");
    const conversation = useChatStore.getState().conversations[0];
    useChatStore.getState().sendText(conversation, "hi", false);
    const local = useChatStore.getState().messagesByConversation["p_1_2"][0];
    useChatStore.getState().acknowledge({ clientMsgId: local.clientMsgId!, serverMsgId: "372036854775807001", convId: "p_1_2", seq: 1, timestamp: 2000 });
    expect(useChatStore.getState().messagesByConversation["p_1_2"][0]).toMatchObject({ seq: 1, serverMsgId: "372036854775807001", status: "sent" });
  });

  it("marks the target message revoked and takes it out of unread when a revoke notice arrives", () => {
    useChatStore.getState().initializeLive(1);
    useChatStore.getState().receiveMessage(wsMsg({ convId: "p_1_2", seq: 1, fromId: 2, content: "secret" }), 1);
    expect(useChatStore.getState().conversations[0].unread).toBe(1);
    const notice = wsMsg({ convId: "p_1_2", seq: 2, fromId: 2, msgType: 6, content: JSON.stringify({ targetSeq: 1, targetMsgId: "1000", operatorId: 2 }) });
    useChatStore.getState().receiveMessage(notice, 1);
    const messages = useChatStore.getState().messagesByConversation["p_1_2"];
    expect(messages).toHaveLength(1);
    expect(messages[0]).toMatchObject({ status: "revoked", content: "消息已撤回" });
    expect(useChatStore.getState().conversations[0]).toMatchObject({ unread: 0, maxSeq: 2 });

    // 重复投递的通知不重复扣减。
    useChatStore.getState().receiveMessage(wsMsg({ convId: "p_1_2", seq: 3, fromId: 2 }), 1);
    useChatStore.getState().receiveMessage(notice, 1);
    expect(useChatStore.getState().conversations[0].unread).toBe(1);
  });

  it("does not touch unread when an already-read message is revoked", () => {
    useChatStore.getState().initializeLive(1);
    useChatStore.getState().applyConvSync([convView({ convId: "p_1_2", maxSeq: 3, readSeq: 2, unread: 1 })]);
    useChatStore.getState().receiveMessage(wsMsg({ convId: "p_1_2", seq: 4, fromId: 2, msgType: 6, content: JSON.stringify({ targetSeq: 2, targetMsgId: "2000", operatorId: 2 }) }), 1);
    expect(useChatStore.getState().conversations[0].unread).toBe(1);
  });

  it("marks the own message revoked when the revoke ack arrives", () => {
    useChatStore.getState().initializeLive(1);
    useChatStore.getState().receiveMessage(wsMsg({ convId: "p_1_2", seq: 1, fromId: 1, content: "oops" }), 1);
    useChatStore.getState().acknowledge({ clientMsgId: "revoke:p_1_2:1", serverMsgId: "99", convId: "p_1_2", seq: 2, timestamp: 3000 });
    expect(useChatStore.getState().messagesByConversation["p_1_2"][0].status).toBe("revoked");
  });

  it("prepends history without moving the sync position", () => {
    useChatStore.getState().initializeLive(1);
    useChatStore.getState().applyConvSync([convView({ convId: "p_1_2", maxSeq: 60 })]);
    useChatStore.getState().applyPulledMsgs(pullResp({ convId: "p_1_2", fromSeq: 11, toSeq: 60, msgs: [59, 60].map((seq) => wsMsg({ convId: "p_1_2", seq, fromId: 2 })) }), 1);
    useChatStore.getState().prependHistory("p_1_2", [57, 58, 59].map((seq) => wsMsg({ convId: "p_1_2", seq, fromId: 2 })), true, 1);
    const state = useChatStore.getState();
    expect(state.messagesByConversation["p_1_2"].map((item) => item.seq)).toEqual([57, 58, 59, 60]);
    expect(state.conversations[0]).toMatchObject({ syncedSeq: 60, maxSeq: 60 });
    expect(state.historyHasMore["p_1_2"]).toBe(true);

    useChatStore.getState().prependHistory("p_1_2", [], false, 1);
    expect(useChatStore.getState().historyHasMore["p_1_2"]).toBe(false);
  });

  it("shows and expires toasts", () => {
    useChatStore.getState().pushToast("撤回失败", "error");
    expect(useChatStore.getState().toasts).toHaveLength(1);
    vi.advanceTimersByTime(3100);
    expect(useChatStore.getState().toasts).toHaveLength(0);
  });

  it("reports the read position only when it advances", () => {
    useChatStore.getState().initializeLive(1);
    useChatStore.getState().applyConvSync([convView({ convId: "p_1_2", maxSeq: 5, readSeq: 2, unread: 3 })]);
    expect(useChatStore.getState().markConversationRead("p_1_2")).toBe(5);
    expect(useChatStore.getState().conversations[0]).toMatchObject({ unread: 0, readSeq: 5 });
    expect(useChatStore.getState().markConversationRead("p_1_2")).toBe(0);
  });

  it("fails a pending message by clientMsgId", () => {
    useChatStore.getState().initializeLive(1);
    useChatStore.getState().addPrivateConversation("p_1_2", 2, "Bob");
    useChatStore.getState().sendText(useChatStore.getState().conversations[0], "hi", false);
    const local = useChatStore.getState().messagesByConversation["p_1_2"][0];
    useChatStore.getState().failMessage(local.clientMsgId!, "不是好友");
    expect(useChatStore.getState().messagesByConversation["p_1_2"][0]).toMatchObject({ status: "failed", error: "不是好友" });
  });

  it("clears live messages when the authenticated user changes", () => {
    useChatStore.getState().initializeLive(1);
    useChatStore.getState().receiveMessage(wsMsg({ convId: "p_1_2", seq: 1, fromId: 2 }), 1);
    expect(useChatStore.getState().messagesByConversation["p_1_2"]).toHaveLength(1);

    useChatStore.getState().initializeLive(2);
    expect(useChatStore.getState().liveUserId).toBe(2);
    expect(useChatStore.getState().messagesByConversation).toEqual({});
  });

  it("marks an empty conversation synchronization as completed", () => {
    useChatStore.getState().initializeLive(1);
    expect(useChatStore.getState().syncCompleted).toBe(false);

    expect(useChatStore.getState().applyConvSync([])).toEqual([]);
    expect(useChatStore.getState().syncCompleted).toBe(true);
    expect(useChatStore.getState().conversations).toEqual([]);
  });

  it("creates and surfaces a conversation for an incoming message", () => {
    useChatStore.getState().initializeLive(1);
    useChatStore.getState().receiveMessage(wsMsg({ convId: "p_1_2", seq: 1, fromId: 2, content: "hello" }), 1);
    expect(useChatStore.getState().conversations[0]).toMatchObject({ id: "p_1_2", targetId: 2, unread: 1, preview: "hello" });
    expect(useChatStore.getState().messagesByConversation["p_1_2"]).toHaveLength(1);
    useChatStore.getState().setConversationIdentity("p_1_2", "Alice");
    expect(useChatStore.getState().conversations[0].name).toBe("Alice");
  });

  it("uses the group ID as the target for a conversation received from another member", () => {
    useChatStore.getState().initializeLive(226);
    useChatStore.getState().receiveMessage(wsMsg({ convId: "g_22", seq: 1, fromId: 225, content: "group hello" }), 226);
    expect(useChatStore.getState().conversations[0]).toMatchObject({ id: "g_22", targetId: 22, group: true });
    expect(useChatStore.getState().messagesByConversation["g_22"][0]).toMatchObject({ senderId: 225, seq: 1 });
  });

  it("hydrates an existing group conversation name", () => {
    useChatStore.getState().initializeLive(226);
    useChatStore.getState().receiveMessage(wsMsg({ convId: "g_22", seq: 1, fromId: 225 }), 226);
    useChatStore.getState().addGroupConversation(22, "联调群");
    expect(useChatStore.getState().conversations[0]).toMatchObject({ name: "联调群", targetId: 22 });
  });

  it("removes a group conversation and its messages after leaving or removal", () => {
    useChatStore.getState().initializeLive(227);
    useChatStore.getState().receiveMessage(wsMsg({ convId: "g_22", seq: 1, fromId: 225 }), 227);
    useChatStore.getState().removeConversation("g_22");
    expect(useChatStore.getState().conversations).toEqual([]);
    expect(useChatStore.getState().messagesByConversation["g_22"]).toBeUndefined();
  });
});

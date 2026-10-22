import { create } from "zustand";
import type { ConvView, PullMsgsReq, PullMsgsResp, SendMessage, ServerAck, WsMessage } from "../../goim-ws-types";
import { ConvType, MsgStatus, MsgType, parseRevokeClientMsgId, parseRevokeNotice, privatePeerId } from "../../goim-ws-types";
import { conversations as previewConversations, messagesByConversation as previewMessages } from "../mocks/data";
import { goimSocket, type ConnectionState } from "../realtime/socket";

export interface ChatConversation {
  id: string;
  name: string;
  preview: string;
  time: string;
  unread: number;
  targetId: number;
  convType: ConvType;
  online?: boolean;
  muted?: boolean;
  group?: boolean;
  avatarUrl?: string;
  /** 已知的会话最大 seq（来自 convSync / 推送 / serverAck） */
  maxSeq: number;
  /** 本地已连续同步到的 seq；(syncedSeq, maxSeq] 是需要补拉的区间，0 表示尚未同步 */
  syncedSeq: number;
  /** 已上报的已读位置 */
  readSeq: number;
}

export interface ChatMessage {
  id: string;
  clientMsgId?: string;
  serverMsgId?: string;
  /** 会话内序号，收到 serverAck 或服务端消息后才有 */
  seq?: number;
  convId: string;
  from: "me" | "other";
  senderId: number;
  content: string;
  time: string;
  timestamp: number;
  status: "sent" | "pending" | "failed" | "revoked";
  outbound?: SendMessage;
  error?: string;
}

interface ChatState {
  mode: "preview" | "live" | null;
  liveUserId: number | null;
  connectionState: ConnectionState;
  syncCompleted: boolean;
  conversations: ChatConversation[];
  messagesByConversation: Record<string, ChatMessage[]>;
  /** 各会话是否还有更早的历史（HTTP 分页）；未记录视为 true */
  historyHasMore: Record<string, boolean>;
  /** 聊天页轻提示（撤回失败等），由 ChatPage 渲染 */
  toasts: { id: string; message: string; tone: "success" | "error" | "info" }[];
  pushToast: (message: string, tone?: "success" | "error" | "info") => void;
  initializePreview: () => void;
  initializeLive: (userId: number) => void;
  resetSession: () => void;
  setConnectionState: (state: ConnectionState) => void;
  sendText: (conversation: ChatConversation, content: string, preview: boolean) => void;
  retryMessage: (convId: string, messageId: string, preview: boolean) => void;
  /** 本地标记已读；返回需要上报的 readSeq，0 表示无需上报 */
  markConversationRead: (convId: string) => number;
  /** 处理 serverAck；返回需要补拉的区间（ack 的 seq 与本地不连续时） */
  acknowledge: (ack: ServerAck) => PullMsgsReq | null;
  failMessage: (clientMsgId: string, error: string) => void;
  /** 处理在线推送；返回需要补拉的区间 */
  receiveMessage: (message: WsMessage, currentUserId: number) => PullMsgsReq | null;
  setConversationIdentity: (convId: string, name: string, avatarUrl?: string, online?: boolean) => void;
  /** 应用会话同步结果；返回各会话需要补拉的区间 */
  applyConvSync: (views: ConvView[]) => PullMsgsReq[];
  /** 合并补拉结果；hasMore 时返回下一页请求 */
  applyPulledMsgs: (resp: PullMsgsResp, currentUserId: number) => PullMsgsReq | null;
  /** 合并上滑加载的历史消息（seq < 当前最小 seq），不影响 syncedSeq / maxSeq */
  prependHistory: (convId: string, msgs: WsMessage[], hasMore: boolean, currentUserId: number) => void;
  revokeMessage: (convId: string, seq: number) => void;
  addGroupConversation: (groupId: number, name: string) => void;
  addPrivateConversation: (id: string, targetId: number, name: string, avatarUrl?: string) => void;
  setConversationMuted: (convId: string, muted: boolean) => void;
  removeConversation: (convId: string) => void;
}

/** 首次同步某个会话时只拉最近这么多条，更早的走 HTTP 历史分页 */
const INITIAL_PULL_COUNT = 50;
const REVOKED_TEXT = "消息已撤回";

const previewTargets: Record<string, number> = {
  "lin-cheng": 2001,
  "product-team": 3001,
  "zhou-yu": 2002,
  "design-room": 3002,
  "chen-xi": 2003,
  "lu-yao": 2004,
};

function formatTime(timestamp: number) {
  return new Intl.DateTimeFormat("zh-CN", { hour: "2-digit", minute: "2-digit", hour12: false }).format(timestamp);
}

function previewState() {
  const conversations: ChatConversation[] = previewConversations.map((item) => {
    const count = previewMessages[item.id]?.length ?? 0;
    return {
      ...item,
      targetId: previewTargets[item.id],
      convType: item.group ? ConvType.Group : ConvType.Private,
      maxSeq: count,
      syncedSeq: count,
      readSeq: Math.max(0, count - item.unread),
    };
  });
  const messagesByConversation: Record<string, ChatMessage[]> = {};
  for (const [convId, messages] of Object.entries(previewMessages)) {
    messagesByConversation[convId] = messages.map((message, index) => ({
      id: `preview-${convId}-${message.id}`,
      seq: index + 1,
      convId,
      from: message.from,
      senderId: message.from === "me" ? 0 : -1,
      content: message.content,
      time: message.time,
      timestamp: Date.now() - (messages.length - index) * 60_000,
      status: message.status ?? "sent",
    }));
  }
  return { conversations, messagesByConversation };
}

function serverMessageToChat(message: WsMessage, currentUserId: number): ChatMessage {
  const revoked = message.status === MsgStatus.Revoked;
  return {
    id: `server-${message.convId}-${message.seq}`,
    clientMsgId: message.clientMsgId,
    serverMsgId: message.msgId,
    seq: message.seq,
    convId: message.convId,
    from: message.fromId === currentUserId ? "me" : "other",
    senderId: message.fromId,
    content: revoked ? REVOKED_TEXT : message.content,
    time: formatTime(message.timestamp),
    timestamp: message.timestamp,
    status: revoked ? "revoked" : "sent",
  };
}

function asRevoked(message: ChatMessage): ChatMessage {
  return message.status === "revoked" ? message : { ...message, content: REVOKED_TEXT, status: "revoked" };
}

/**
 * 全序排序：已确认消息（有 seq）按 seq 升序在前；待确认消息（无 seq）排在其后按本地时间排序。
 * 旧实现在"有 seq / 无 seq"混排时退化为按时间比较，比较函数不满足传递性，结果依赖引擎实现。
 */
function compareMessages(a: ChatMessage, b: ChatMessage) {
  if (a.seq && b.seq) return a.seq - b.seq;
  if (a.seq) return -1;
  if (b.seq) return 1;
  return a.timestamp - b.timestamp;
}

function sortMessages(messages: ChatMessage[]) {
  return messages.sort(compareMessages);
}

/**
 * 把服务端消息合并进本地列表：按 seq 去重；带 clientMsgId 的消息替换本地待确认消息；
 * msgType=6 的撤回通知不展示，只把目标消息标为已撤回。
 */
function mergeServerMessages(existing: ChatMessage[], incoming: WsMessage[], currentUserId: number) {
  const out = [...existing];
  const indexBySeq = new Map<number, number>();
  out.forEach((message, index) => { if (message.seq) indexBySeq.set(message.seq, index); });
  const revokedSeqs: number[] = [];

  for (const message of incoming) {
    if (message.msgType === MsgType.Revoke) {
      const notice = parseRevokeNotice(message.content);
      if (notice) revokedSeqs.push(notice.targetSeq);
      continue;
    }
    const known = indexBySeq.get(message.seq);
    if (known !== undefined) {
      if (message.status === MsgStatus.Revoked) out[known] = asRevoked(out[known]);
      continue;
    }
    const converted = serverMessageToChat(message, currentUserId);
    const pendingIndex = message.clientMsgId ? out.findIndex((item) => !item.seq && item.clientMsgId === message.clientMsgId) : -1;
    if (pendingIndex >= 0) {
      out[pendingIndex] = { ...converted, id: out[pendingIndex].id };
      indexBySeq.set(message.seq, pendingIndex);
    } else {
      out.push(converted);
      indexBySeq.set(message.seq, out.length - 1);
    }
  }

  for (const seq of revokedSeqs) {
    const index = indexBySeq.get(seq);
    if (index !== undefined) out[index] = asRevoked(out[index]);
  }
  return sortMessages(out);
}

/** 推进会话 seq：连续则前移 syncedSeq，不连续则返回需要补拉的区间。 */
function advanceSeq(conversation: ChatConversation, seq: number) {
  const maxSeq = Math.max(conversation.maxSeq, seq);
  if (seq <= conversation.syncedSeq) return { maxSeq, syncedSeq: conversation.syncedSeq, gap: null };
  if (conversation.syncedSeq === 0 || seq === conversation.syncedSeq + 1) return { maxSeq, syncedSeq: seq, gap: null };
  const gap: PullMsgsReq = { convId: conversation.id, fromSeq: conversation.syncedSeq + 1, toSeq: seq };
  return { maxSeq, syncedSeq: conversation.syncedSeq, gap };
}

function lastPreview(messages: ChatMessage[]) {
  return messages.length > 0 ? messages[messages.length - 1].content : undefined;
}

function newConversationFor(message: WsMessage, currentUserId: number): ChatConversation {
  const group = message.convType === ConvType.Group;
  const targetId = group ? Number(message.convId.slice(2)) : privatePeerId(message.convId, currentUserId);
  return {
    id: message.convId,
    name: group ? `群聊 #${targetId}` : `用户 #${targetId}`,
    preview: "",
    time: "",
    unread: 0,
    targetId,
    convType: message.convType,
    group,
    maxSeq: 0,
    syncedSeq: 0,
    readSeq: 0,
  };
}

function updateMessage(messages: Record<string, ChatMessage[]>, convId: string, messageId: string, patch: Partial<ChatMessage>) {
  return { ...messages, [convId]: (messages[convId] ?? []).map((item) => item.id === messageId ? { ...item, ...patch } : item) };
}

export const useChatStore = create<ChatState>((set, get) => ({
  mode: null,
  liveUserId: null,
  connectionState: "idle",
  syncCompleted: false,
  conversations: [],
  messagesByConversation: {},
  historyHasMore: {},
  toasts: [],
  pushToast: (message, tone = "info") => {
    const id = `toast-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    set((state) => ({ toasts: [...state.toasts, { id, message, tone }] }));
    setTimeout(() => set((state) => ({ toasts: state.toasts.filter((item) => item.id !== id) })), 3000);
  },


  initializePreview: () => {
    if (get().mode === "preview") return;
    set({ mode: "preview", liveUserId: null, connectionState: "connected", syncCompleted: true, historyHasMore: {}, ...previewState() });
  },
  initializeLive: (userId) => {
    if (get().mode === "live" && get().liveUserId === userId) return;
    set({ mode: "live", liveUserId: userId, connectionState: "connecting", syncCompleted: false, conversations: [], messagesByConversation: {}, historyHasMore: {} });
  },
  resetSession: () => set({ mode: null, liveUserId: null, connectionState: "idle", syncCompleted: false, conversations: [], messagesByConversation: {}, historyHasMore: {} }),
  setConnectionState: (connectionState) => set({ connectionState }),

  sendText: (conversation, content, preview) => {
    const clientMsgId = crypto.randomUUID();
    const timestamp = Date.now();
    const outbound: SendMessage = { msgId: clientMsgId, convType: conversation.convType, toId: conversation.targetId, msgType: MsgType.Text, content, timestamp };
    const localMessage: ChatMessage = { id: `client-${clientMsgId}`, clientMsgId, convId: conversation.id, from: "me", senderId: get().liveUserId ?? 0, content, time: formatTime(timestamp), timestamp, status: "pending", outbound };
    set((state) => ({
      messagesByConversation: { ...state.messagesByConversation, [conversation.id]: [...(state.messagesByConversation[conversation.id] ?? []), localMessage] },
      conversations: [
        ...state.conversations
          .filter((item) => item.id === conversation.id)
          .map((item) => ({ ...item, preview: content, time: "刚刚" })),
        ...state.conversations.filter((item) => item.id !== conversation.id),
      ],
    }));

    if (preview) {
      window.setTimeout(() => simulatePreviewAck(conversation.id, clientMsgId), 420);
      return;
    }
    if (!goimSocket.send({ type: "msg", data: outbound })) {
      set((state) => ({ messagesByConversation: updateMessage(state.messagesByConversation, conversation.id, localMessage.id, { status: "failed", error: "连接不可用" }) }));
    }
  },

  retryMessage: (convId, messageId, preview) => {
    const message = get().messagesByConversation[convId]?.find((item) => item.id === messageId);
    if (!message?.outbound) return;
    set((state) => ({ messagesByConversation: updateMessage(state.messagesByConversation, convId, messageId, { status: "pending", error: undefined }) }));
    // 重试沿用同一个 clientMsgId，服务端据此幂等返回原 seq。
    if (preview) {
      window.setTimeout(() => simulatePreviewAck(convId, message.outbound!.msgId), 420);
    } else if (!goimSocket.send({ type: "msg", data: message.outbound })) {
      set((state) => ({ messagesByConversation: updateMessage(state.messagesByConversation, convId, messageId, { status: "failed", error: "连接不可用" }) }));
    }
  },

  markConversationRead: (convId) => {
    const conversation = get().conversations.find((item) => item.id === convId);
    if (!conversation) return 0;
    const readSeq = Math.max(conversation.readSeq, conversation.maxSeq);
    const advanced = readSeq > conversation.readSeq;
    if (!advanced && conversation.unread === 0) return 0;
    set((state) => ({ conversations: state.conversations.map((item) => item.id === convId ? { ...item, unread: 0, readSeq } : item) }));
    return advanced ? readSeq : 0;
  },

  acknowledge: (ack) => {
    const state = get();
    const conversation = state.conversations.find((item) => item.id === ack.convId);
    const advanced = conversation ? advanceSeq(conversation, ack.seq) : null;
    const revoke = parseRevokeClientMsgId(ack.clientMsgId);
    const current = state.messagesByConversation[ack.convId] ?? [];
    const messages = revoke
      ? current.map((item) => item.seq === revoke.seq ? asRevoked(item) : item)
      : sortMessages(current.map((item) => item.clientMsgId === ack.clientMsgId
        ? { ...item, serverMsgId: ack.serverMsgId, seq: ack.seq, status: item.status === "revoked" ? item.status : "sent" as const, error: undefined }
        : item));
    set({
      messagesByConversation: { ...state.messagesByConversation, [ack.convId]: messages },
      conversations: advanced
        ? state.conversations.map((item) => item.id === ack.convId ? { ...item, maxSeq: advanced.maxSeq, syncedSeq: advanced.syncedSeq, preview: lastPreview(messages) ?? item.preview } : item)
        : state.conversations,
    });
    return advanced?.gap ?? null;
  },

  failMessage: (clientMsgId, error) => set((state) => {
    for (const [convId, messages] of Object.entries(state.messagesByConversation)) {
      const target = messages.find((item) => item.clientMsgId === clientMsgId && item.status === "pending");
      if (target) return { messagesByConversation: updateMessage(state.messagesByConversation, convId, target.id, { status: "failed", error }) };
    }
    return state;
  }),

  receiveMessage: (message, currentUserId) => {
    const state = get();
    const current = state.messagesByConversation[message.convId] ?? [];
    const isNotice = message.msgType === MsgType.Revoke;
    if (!isNotice && current.some((item) => item.seq === message.seq)) return null;

    const base = state.conversations.find((item) => item.id === message.convId) ?? newConversationFor(message, currentUserId);
    const merged = mergeServerMessages(current, [message], currentUserId);
    const advanced = advanceSeq(base, message.seq);
    const isNew = message.seq > base.maxSeq;
    const countsUnread = isNew && !isNotice && message.fromId !== currentUserId && message.seq > base.readSeq;
    // 撤回通知：被撤回的原消息此前计入过未读（他人发送且在已读位置之后）时扣回 1。
    // 用 isNew 防止重复投递的通知重复扣减；本地已是撤回状态的目标不再扣。
    let revokedUnread = false;
    if (isNotice && isNew && base.unread > 0) {
      const notice = parseRevokeNotice(message.content);
      const target = notice ? current.find((item) => item.seq === notice.targetSeq) : undefined;
      revokedUnread = Boolean(notice
        && notice.targetSeq > base.readSeq
        && notice.operatorId !== currentUserId
        && target?.status !== "revoked"
        && (target ? target.from === "other" : true));
    }
    const updated: ChatConversation = {
      ...base,
      maxSeq: advanced.maxSeq,
      syncedSeq: advanced.syncedSeq,
      preview: lastPreview(merged) ?? base.preview,
      time: formatTime(message.timestamp),
      unread: base.unread + (countsUnread ? 1 : 0) - (revokedUnread ? 1 : 0),
    };
    set({
      messagesByConversation: { ...state.messagesByConversation, [message.convId]: merged },
      conversations: [updated, ...state.conversations.filter((item) => item.id !== message.convId)],
    });
    return advanced.gap;
  },

  setConversationIdentity: (convId, name, avatarUrl, online) => set((state) => ({ conversations: state.conversations.map((conversation) => conversation.id === convId ? { ...conversation, name, avatarUrl, online: online ?? conversation.online } : conversation) })),

  applyConvSync: (views) => {
    const state = get();
    const pulls: PullMsgsReq[] = [];
    const conversations = views.map((view): ChatConversation => {
      const previous = state.conversations.find((item) => item.id === view.convId);
      const syncedSeq = previous?.syncedSeq ?? 0;
      if (view.maxSeq > syncedSeq) {
        const fromSeq = syncedSeq > 0 ? syncedSeq + 1 : Math.max(1, view.minSeq, view.maxSeq - INITIAL_PULL_COUNT + 1);
        pulls.push({ convId: view.convId, fromSeq, toSeq: view.maxSeq });
      }
      return {
        id: view.convId,
        name: view.targetName || previous?.name || `会话 ${view.targetId}`,
        preview: view.lastMsgBrief,
        time: view.lastMsgTime ? formatTime(view.lastMsgTime) : "",
        unread: view.unread,
        targetId: view.targetId,
        convType: view.convType,
        group: view.convType === ConvType.Group,
        avatarUrl: view.targetAvatar || previous?.avatarUrl,
        online: previous?.online,
        muted: previous?.muted ?? view.muted,
        maxSeq: Math.max(view.maxSeq, previous?.maxSeq ?? 0),
        syncedSeq,
        readSeq: Math.max(view.readSeq, previous?.readSeq ?? 0),
      };
    });
    set({ syncCompleted: true, conversations });
    return pulls;
  },

  applyPulledMsgs: (resp, currentUserId) => {
    const state = get();
    const current = state.messagesByConversation[resp.convId] ?? [];
    const merged = mergeServerMessages(current, resp.msgs, currentUserId);
    const conversation = state.conversations.find((item) => item.id === resp.convId);
    const contiguous = conversation ? conversation.syncedSeq === 0 || resp.fromSeq <= conversation.syncedSeq + 1 : false;
    const newest = resp.msgs.length > 0 ? resp.msgs[resp.msgs.length - 1] : undefined;
    // 服务端已把 toSeq 截到已投递水位；这里再取一次 min 作防御。水位以下缺失的 seq 是空洞，可直接跨过。
    const reached = Math.min(resp.toSeq, resp.watermark ?? resp.toSeq);
    set({
      messagesByConversation: { ...state.messagesByConversation, [resp.convId]: merged },
      conversations: conversation
        ? state.conversations.map((item) => item.id === resp.convId ? {
          ...item,
          maxSeq: Math.max(item.maxSeq, reached),
          syncedSeq: contiguous ? Math.max(item.syncedSeq, reached) : item.syncedSeq,
          preview: lastPreview(merged) ?? item.preview,
          time: newest ? formatTime(newest.timestamp) : item.time,
        } : item)
        : state.conversations,
    });
    return resp.hasMore ? { convId: resp.convId, fromSeq: reached + 1, toSeq: 0 } : null;
  },

  prependHistory: (convId, msgs, hasMore, currentUserId) => set((state) => ({
    messagesByConversation: { ...state.messagesByConversation, [convId]: mergeServerMessages(state.messagesByConversation[convId] ?? [], msgs, currentUserId) },
    historyHasMore: { ...state.historyHasMore, [convId]: hasMore },
  })),

  revokeMessage: (convId, seq) => set((state) => {
    const messages = (state.messagesByConversation[convId] ?? []).map((message) => message.seq === seq ? asRevoked(message) : message);
    return {
      messagesByConversation: { ...state.messagesByConversation, [convId]: messages },
      conversations: state.conversations.map((item) => item.id === convId ? { ...item, preview: lastPreview(messages) ?? item.preview } : item),
    };
  }),

  addGroupConversation: (groupId, name) => set((state) => {
    const id = state.mode === "preview" ? `preview-group-${groupId}` : `g_${groupId}`;
    if (state.conversations.some((conversation) => conversation.id === id)) {
      return { conversations: state.conversations.map((conversation) => conversation.id === id ? { ...conversation, name, targetId: groupId, convType: ConvType.Group, group: true } : conversation) };
    }
    return {
      conversations: [{ id, name, preview: "群聊已创建", time: "刚刚", unread: 0, targetId: groupId, convType: ConvType.Group, group: true, maxSeq: 0, syncedSeq: 0, readSeq: 0 }, ...state.conversations],
      messagesByConversation: { ...state.messagesByConversation, [id]: state.messagesByConversation[id] ?? [] },
    };
  }),
  addPrivateConversation: (id, targetId, name, avatarUrl) => set((state) => {
    if (state.conversations.some((conversation) => conversation.id === id)) return state;
    return {
      conversations: [{ id, name, avatarUrl, preview: "还没有消息，打个招呼吧", time: "", unread: 0, targetId, convType: ConvType.Private, maxSeq: 0, syncedSeq: 0, readSeq: 0 }, ...state.conversations],
      messagesByConversation: { ...state.messagesByConversation, [id]: state.messagesByConversation[id] ?? [] },
    };
  }),
  setConversationMuted: (convId, muted) => set((state) => {
    const conversation = state.conversations.find((item) => item.id === convId);
    if (!conversation || Boolean(conversation.muted) === muted) return state;
    return { conversations: state.conversations.map((item) => item.id === convId ? { ...item, muted } : item) };
  }),
  removeConversation: (convId) => set((state) => {
    const messagesByConversation = { ...state.messagesByConversation };
    delete messagesByConversation[convId];
    return { conversations: state.conversations.filter((conversation) => conversation.id !== convId), messagesByConversation };
  }),
}));

/** 预览模式下模拟服务端回执：按会话本地 maxSeq 递增分配 seq。 */
function simulatePreviewAck(convId: string, clientMsgId: string) {
  const { conversations, acknowledge } = useChatStore.getState();
  const seq = (conversations.find((item) => item.id === convId)?.maxSeq ?? 0) + 1;
  acknowledge({ clientMsgId, serverMsgId: String(Date.now()), convId, seq, timestamp: Date.now() });
}

import { useEffect } from "react";
import type { PullMsgsReq, ServerWsMessage } from "../../../goim-ws-types";
import { buildPrivateConvId, ConvType, MsgType, parseRevokeClientMsgId, privatePeerId } from "../../../goim-ws-types";
import { useAuthStore } from "../../stores/authStore";
import { useChatStore } from "../../stores/chatStore";
import { goimSocket } from "../../realtime/socket";
import { friendsApi, groupsApi, settingsApi } from "../../lib/api";
import { configureNotifications, notifyIncomingMessage } from "../../realtime/notifications";
import { createPullScheduler } from "../../realtime/pullScheduler";

let loadedSettings: Awaited<ReturnType<typeof settingsApi.get>> | null = null;

const pulls = createPullScheduler(
  (request) => goimSocket.send({ type: "pullMsgs", data: request }),
  (convId) => useChatStore.getState().conversations.find((item) => item.id === convId)?.syncedSeq ?? 0,
);

function applyMutedConversations() {
  if (!loadedSettings) return;
  let mutedIds = new Set<string>();
  try {
    const parsed = JSON.parse(loadedSettings.mute_list) as unknown;
    if (Array.isArray(parsed)) mutedIds = new Set(parsed.filter((item): item is string => typeof item === "string"));
  } catch { /* Invalid legacy values behave as an empty list. */ }
  const chat = useChatStore.getState();
  for (const conversation of chat.conversations) chat.setConversationMuted(conversation.id, mutedIds.has(conversation.id));
}

async function refreshPrivateConversationIdentities() {
  try {
    const page = await friendsApi.list(100, 0);
    const chat = useChatStore.getState();
    const friendIDs = new Set(page.items.map((friend) => friend.friend_id));
    for (const conversation of chat.conversations) {
      if (!conversation.group && !friendIDs.has(conversation.targetId)) chat.removeConversation(conversation.id);
    }
    for (const friend of page.items) {
      const conversation = chat.conversations.find((item) => !item.group && item.targetId === friend.friend_id);
      const name = friend.nickname || `用户 #${friend.friend_id}`;
      if (conversation) chat.setConversationIdentity(conversation.id, name, friend.avatar_url, friend.online);
      else if (chat.liveUserId) chat.addPrivateConversation(buildPrivateConvId(chat.liveUserId, friend.friend_id), friend.friend_id, name, friend.avatar_url);
    }
  } catch {
    // 在线状态刷新失败不影响现有会话和消息收发。
  }
}

function requestPull(request: PullMsgsReq | null) {
  pulls.request(request);
}

function requestConvSync() {
  goimSocket.send({ type: "convSyncReq", data: {} });
}

function handleServerMessage(message: ServerWsMessage, currentUserId: number, clearSession: () => void) {
  const chat = useChatStore.getState();
  switch (message.type) {
    case "serverAck":
      requestPull(chat.acknowledge(message.data));
      break;
    case "msg": {
      const data = message.data;
      const isNotice = data.msgType === MsgType.Revoke;
      const known = chat.conversations.some((item) => item.id === data.convId);
      if (data.fromId !== currentUserId && !isNotice) {
        const conversation = chat.conversations.find((item) => item.id === data.convId);
        notifyIncomingMessage({ convId: data.convId, title: conversation?.name ?? (data.convType === ConvType.Group ? "群聊新消息" : "好友新消息"), content: data.content });
      }
      requestPull(chat.receiveMessage(data, currentUserId));
      if (known) break;
      // 首次出现的会话补齐名称与头像。
      if (data.convType === ConvType.Private) {
        const targetId = privatePeerId(data.convId, currentUserId);
        void friendsApi.list(100, 0).then((page) => {
          const friend = page.items.find((item) => item.friend_id === targetId);
          if (friend) useChatStore.getState().setConversationIdentity(data.convId, friend.nickname || `用户 #${targetId}`, friend.avatar_url, friend.online);
        }).catch(() => undefined);
      } else {
        void groupsApi.get(Number(data.convId.slice(2))).then((group) => {
          useChatStore.getState().addGroupConversation(group.id, group.name);
        }).catch(() => undefined);
      }
      break;
    }
    case "convSync":
      for (const request of chat.applyConvSync(message.data.conversations)) requestPull(request);
      applyMutedConversations();
      void refreshPrivateConversationIdentities();
      void groupsApi.list().then((groups) => {
        for (const group of groups) useChatStore.getState().addGroupConversation(group.id, group.name);
      }).catch(() => undefined);
      break;
    case "pullMsgsResp":
      pulls.complete(message.data.convId, chat.applyPulledMsgs(message.data, currentUserId));
      break;
    case "error": {
      // 只有发送/撤回失败会带 clientMsgId；readAck、pullMsgs 等失败不影响消息状态。
      const clientMsgId = message.data.clientMsgId;
      if (!clientMsgId) break;
      if (parseRevokeClientMsgId(clientMsgId)) chat.pushToast(`撤回失败：${message.data.message || "请稍后重试"}`, "error");
      else chat.failMessage(clientMsgId, message.data.message);
      break;
    }
    case "kick":
      goimSocket.disconnect();
      clearSession();
      break;
    case "groupRemoved":
      chat.removeConversation(`g_${message.data.groupId}`);
      break;
    case "groupAdded":
      chat.addGroupConversation(message.data.groupId, message.data.name);
      break;
  }
}

export function RealtimeBootstrap() {
  const accessToken = useAuthStore((state) => state.accessToken);
  const previewMode = useAuthStore((state) => state.previewMode);
  const userId = useAuthStore((state) => state.user?.id ?? 0);
  const clearSession = useAuthStore((state) => state.clearSession);

  useEffect(() => {
    if (previewMode) {
      goimSocket.disconnect();
      useChatStore.getState().initializePreview();
      return;
    }
    if (!accessToken || !userId) {
      goimSocket.disconnect();
      useChatStore.getState().resetSession();
      return;
    }

    useChatStore.getState().initializeLive(userId);
    void settingsApi.get().then((settings) => {
      loadedSettings = settings;
      configureNotifications(settings);
      applyMutedConversations();
    }).catch(() => undefined);
    goimSocket.setHandlers({
      onStateChange: (state) => {
        useChatStore.getState().setConnectionState(state);
        if (state === "connected") {
          // 上线与重连统一走 convSync：对比各会话 maxSeq 与本地 syncedSeq，再按区间 pullMsgs。
          // 断线前在途的 pullMsgs 响应可能已丢失，先清空调度状态。
          pulls.reset();
          requestConvSync();
        }
      },
      onMessage: (message) => handleServerMessage(message, userId, clearSession),
    });
    // React StrictMode performs a synchronous setup/cleanup probe in development.
    // Deferring the real connection avoids opening and immediately closing a socket
    // before its handshake completes during that probe.
    const connectTimer = window.setTimeout(() => goimSocket.connect(accessToken), 0);
    const onlineRefreshTimer = window.setInterval(() => void refreshPrivateConversationIdentities(), 15_000);
    return () => {
      window.clearTimeout(connectTimer);
      window.clearInterval(onlineRefreshTimer);
      goimSocket.disconnect(false);
    };
  }, [accessToken, clearSession, previewMode, userId]);

  return null;
}

import type {
  ConversationListResponse,
  HistoryQuery,
  HistoryResponse,
  MarkReadRequest,
  RevokeMessageRequest,
  RevokeMessageResponse,
  SearchMessagesQuery,
  SearchMessagesResponse,
} from "../../goim-api-types";
import type { GoIMApiClient } from "./client";

export const createMessagesApi = (client: GoIMApiClient) => ({
  revoke: (input: RevokeMessageRequest) => client.post<RevokeMessageResponse>("/msg/revoke", input),
  remove: (convId: string, seq: number) => client.delete<void>(`/msg/${seq}?convId=${encodeURIComponent(convId)}`),
  search: ({ q, convId, limit = 20 }: SearchMessagesQuery) => {
    const params = new URLSearchParams({ q, limit: String(limit) });
    if (convId) params.set("convId", convId);
    return client.get<SearchMessagesResponse>(`/msg/search?${params.toString()}`);
  },
});

export const createConversationsApi = (client: GoIMApiClient) => ({
  list: () => client.get<ConversationListResponse>("/conversations"),
  history: (convId: string, { beforeSeq = 0, limit = 30 }: HistoryQuery = {}) =>
    client.get<HistoryResponse>(`/conversations/${encodeURIComponent(convId)}/messages?beforeSeq=${beforeSeq}&limit=${limit}`),
  markRead: (convId: string, input: MarkReadRequest) => client.post<void>(`/conversations/${encodeURIComponent(convId)}/read`, input),
  clear: (convId: string) => client.post<void>(`/conversations/${encodeURIComponent(convId)}/clear`),
  remove: (convId: string) => client.delete<void>(`/conversations/${encodeURIComponent(convId)}`),
});

import type {
  GroupProfileSummary,
  GroupProfileUser,
  GroupRow,
  MessageDetail,
  MessageListItem,
  NoticeDetail,
  NoticeListItem,
  OverviewPayload,
  PagedResponse,
  ProfilePipelineStatusPayload,
  UserProfileDetail,
} from "./types";

interface ApiClientOptions {
  getToken: () => string;
  onUnauthorized: () => void;
}

export class ApiClient {
  private readonly getToken: () => string;
  private readonly onUnauthorized: () => void;

  constructor(options: ApiClientOptions) {
    this.getToken = options.getToken;
    this.onUnauthorized = options.onUnauthorized;
  }

  async getOverview() {
    return this.fetchJson<OverviewPayload>("/api/overview");
  }

  async getProfilePipelineStatus() {
    return this.fetchJson<ProfilePipelineStatusPayload>("/api/profile-pipeline/status");
  }

  async wakeProfilePipeline() {
    return this.fetchJson<{
      triggered: boolean;
      runtime: ProfilePipelineStatusPayload["runtime"];
      storage: ProfilePipelineStatusPayload["storage"];
    }>("/api/profile-pipeline/wake", { method: "POST" });
  }

  async resetProfilePipeline() {
    return this.fetchJson<{
      reset: boolean;
      clear_claims: boolean;
      deleted_counts: Record<string, number>;
      runtime: ProfilePipelineStatusPayload["runtime"];
      storage: ProfilePipelineStatusPayload["storage"];
    }>("/api/profile-pipeline/reset?clear_claims=1", { method: "POST" });
  }

  async getGroups(search: string) {
    return this.fetchJson<PagedResponse<GroupRow>>(
      `/api/groups?limit=200&offset=0&search=${encodeURIComponent(search)}`,
    );
  }

  async getMessages(params: {
    platformId: string;
    groupId: string;
    offset: number;
    limit: number;
    direction: string;
    search: string;
  }) {
    const query = new URLSearchParams({
      platform_id: params.platformId,
      group_id: params.groupId,
      offset: String(params.offset),
      limit: String(params.limit),
      search: params.search,
    });
    if (params.direction) {
      query.set("direction", params.direction);
    }
    return this.fetchJson<PagedResponse<MessageListItem>>(`/api/messages?${query.toString()}`);
  }

  async getMessageDetail(messageId: number) {
    return this.fetchJson<MessageDetail>(`/api/messages/${messageId}`);
  }

  async getNotices(params: {
    platformId: string;
    groupId: string;
    offset: number;
    limit: number;
    noticeType: string;
  }) {
    const query = new URLSearchParams({
      platform_id: params.platformId,
      group_id: params.groupId,
      offset: String(params.offset),
      limit: String(params.limit),
    });
    if (params.noticeType) {
      query.set("notice_type", params.noticeType);
    }
    return this.fetchJson<PagedResponse<NoticeListItem>>(`/api/notices?${query.toString()}`);
  }

  async getNoticeDetail(noticeId: number) {
    return this.fetchJson<NoticeDetail>(`/api/notices/${noticeId}`);
  }

  async getGroupProfileSummary(platformId: string, groupId: string) {
    const query = new URLSearchParams({
      platform_id: platformId,
      group_id: groupId,
    });
    return this.fetchJson<GroupProfileSummary>(`/api/profiles/group?${query.toString()}`);
  }

  async getGroupProfileUsers(params: {
    platformId: string;
    groupId: string;
    offset: number;
    limit: number;
    search: string;
  }) {
    const query = new URLSearchParams({
      platform_id: params.platformId,
      group_id: params.groupId,
      offset: String(params.offset),
      limit: String(params.limit),
      search: params.search,
    });
    return this.fetchJson<PagedResponse<GroupProfileUser>>(
      `/api/profiles/users?${query.toString()}`,
    );
  }

  async getUserProfile(platformId: string, groupId: string, userId: string) {
    const query = new URLSearchParams({
      platform_id: platformId,
      group_id: groupId,
    });
    return this.fetchJson<UserProfileDetail>(
      `/api/profiles/users/${encodeURIComponent(userId)}?${query.toString()}`,
    );
  }

  mediaUrl(relativePath: string) {
    const encoded = relativePath
      .split("/")
      .map((part) => encodeURIComponent(part))
      .join("/");
    const token = this.getToken().trim();
    const suffix = token ? `?token=${encodeURIComponent(token)}` : "";
    return `/api/media/${encoded}${suffix}`;
  }

  private async fetchJson<T>(path: string, init: RequestInit = {}): Promise<T> {
    const headers = new Headers();
    for (const [key, value] of new Headers(init.headers || {})) {
      headers.set(key, value);
    }
    const token = this.getToken().trim();
    if (token) {
      headers.set("X-Auth-Token", token);
    }

    const response = await fetch(path, { ...init, headers });
    if (response.status === 401) {
      this.onUnauthorized();
      throw new Error("unauthorized");
    }
    if (!response.ok) {
      throw new Error((await response.text()) || `HTTP ${response.status}`);
    }
    return (await response.json()) as T;
  }
}

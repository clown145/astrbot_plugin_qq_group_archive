import type { ComponentChildren } from "preact";
import { useEffect, useMemo, useRef, useState } from "preact/hooks";

import { ApiClient } from "./api";
import {
  classNames,
  displayUserLabel,
  formatAttributeLabel,
  formatClaimStatus,
  formatCompactNumber,
  formatDuration,
  formatPercent,
  formatSourceKind,
  formatTime,
  truncateText,
} from "./format";
import type {
  ActiveTab,
  GroupProfileSummary,
  GroupProfileUser,
  GroupRow,
  MessageDetail,
  MessageListItem,
  MobilePane,
  NoticeDetail,
  NoticeListItem,
  OverviewPayload,
  ProfileAttribute,
  ProfileClaim,
  ProfilePipelineStatusPayload,
  SelectedDetail,
  UserProfileDetail,
} from "./types";

const MESSAGE_LIMIT = 50;
const NOTICE_LIMIT = 50;
const PROFILE_LIMIT = 50;

interface ListState<T> {
  items: T[];
  total: number;
  loading: boolean;
  error: string;
}

type DetailState =
  | {
      kind: "message";
      label: string;
      data: MessageDetail;
    }
  | {
      kind: "notice";
      label: string;
      data: NoticeDetail;
    }
  | {
      kind: "profile";
      label: string;
      data: UserProfileDetail;
    }
  | null;

const EMPTY_LIST = <T,>(): ListState<T> => ({
  items: [],
  total: 0,
  loading: false,
  error: "",
});

export function App() {
  const [authToken, setAuthToken] = useState(
    () => window.localStorage.getItem("qq-archive-auth-token") || "",
  );
  const [draftToken, setDraftToken] = useState(authToken);
  const [authDialogOpen, setAuthDialogOpen] = useState(false);
  const authDialogRef = useRef<HTMLDialogElement>(null);

  const [overview, setOverview] = useState<OverviewPayload | null>(null);
  const [overviewError, setOverviewError] = useState("");
  const [pipelineStatus, setPipelineStatus] = useState<ProfilePipelineStatusPayload | null>(null);
  const [pipelineStatusError, setPipelineStatusError] = useState("");
  const [pipelineWaking, setPipelineWaking] = useState(false);
  const [pipelineResetting, setPipelineResetting] = useState(false);
  const [groups, setGroups] = useState<ListState<GroupRow>>(EMPTY_LIST<GroupRow>());
  const [selectedGroup, setSelectedGroup] = useState<GroupRow | null>(null);
  const [activeTab, setActiveTab] = useState<ActiveTab>("messages");
  const [mobilePane, setMobilePane] = useState<MobilePane>("groups");
  const [isMobile, setIsMobile] = useState(() =>
    window.matchMedia("(max-width: 960px)").matches,
  );

  const [groupSearch, setGroupSearch] = useState("");
  const [messageDirection, setMessageDirection] = useState("");
  const [messageSearch, setMessageSearch] = useState("");
  const [noticeType, setNoticeType] = useState("");
  const [profileSearch, setProfileSearch] = useState("");

  const [messages, setMessages] = useState<ListState<MessageListItem>>(EMPTY_LIST<MessageListItem>());
  const [notices, setNotices] = useState<ListState<NoticeListItem>>(EMPTY_LIST<NoticeListItem>());
  const [profileUsers, setProfileUsers] = useState<ListState<GroupProfileUser>>(
    EMPTY_LIST<GroupProfileUser>(),
  );
  const [groupProfileSummary, setGroupProfileSummary] = useState<GroupProfileSummary | null>(
    null,
  );
  const [groupProfileSummaryError, setGroupProfileSummaryError] = useState("");

  const [detail, setDetail] = useState<DetailState>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState("");
  const [selectedDetail, setSelectedDetail] = useState<SelectedDetail | null>(null);

  const workspaceRef = useRef<HTMLDivElement>(null);
  const groupsPanelRef = useRef<HTMLElement>(null);
  const timelinePanelRef = useRef<HTMLElement>(null);
  const detailPanelRef = useRef<HTMLElement>(null);

  const api = useMemo(
    () =>
      new ApiClient({
        getToken: () => authToken,
        onUnauthorized: () => {
          setDraftToken(authToken);
          setAuthDialogOpen(true);
        },
      }),
    [authToken],
  );

  useEffect(() => {
    if (!authDialogRef.current) {
      return;
    }
    const dialog = authDialogRef.current;
    if (authDialogOpen && !dialog.open) {
      dialog.showModal();
      return;
    }
    if (!authDialogOpen && dialog.open) {
      dialog.close();
    }
  }, [authDialogOpen]);

  useEffect(() => {
    const media = window.matchMedia("(max-width: 960px)");
    const onChange = () => setIsMobile(media.matches);
    onChange();
    media.addEventListener("change", onChange);
    return () => media.removeEventListener("change", onChange);
  }, []);

  useEffect(() => {
    void loadOverview();
    void loadPipelineStatus();
  }, [api]);

  useEffect(() => {
    const timer = window.setTimeout(() => {
      void loadGroups();
    }, 200);
    return () => window.clearTimeout(timer);
  }, [api, groupSearch]);

  useEffect(() => {
    setMessages(EMPTY_LIST<MessageListItem>());
    setNotices(EMPTY_LIST<NoticeListItem>());
    setProfileUsers(EMPTY_LIST<GroupProfileUser>());
    setGroupProfileSummary(null);
    setGroupProfileSummaryError("");
    setDetail(null);
    setDetailError("");
    setSelectedDetail(null);
    if (selectedGroup && isMobile) {
      setMobilePane("timeline");
    }
  }, [selectedGroup?.platform_id, selectedGroup?.group_id]);

  useEffect(() => {
    if (!selectedGroup) {
      return;
    }
    const timer = window.setTimeout(() => {
      void loadMessages(true);
    }, 200);
    return () => window.clearTimeout(timer);
  }, [
    api,
    selectedGroup?.platform_id,
    selectedGroup?.group_id,
    messageDirection,
    messageSearch,
  ]);

  useEffect(() => {
    if (!selectedGroup) {
      return;
    }
    const timer = window.setTimeout(() => {
      void loadNotices(true);
    }, 200);
    return () => window.clearTimeout(timer);
  }, [api, selectedGroup?.platform_id, selectedGroup?.group_id, noticeType]);

  useEffect(() => {
    if (!selectedGroup) {
      return;
    }
    const timer = window.setTimeout(() => {
      void loadProfiles(true);
    }, 200);
    return () => window.clearTimeout(timer);
  }, [api, selectedGroup?.platform_id, selectedGroup?.group_id, profileSearch]);

  useEffect(() => {
    if (!isMobile) {
      return;
    }
    scrollToPane(mobilePane, "smooth");
  }, [isMobile, mobilePane]);

  const scrollToPane = (pane: MobilePane, behavior: ScrollBehavior) => {
    if (!workspaceRef.current || !isMobile) {
      return;
    }
    const target =
      pane === "groups"
        ? groupsPanelRef.current
        : pane === "timeline"
          ? timelinePanelRef.current
          : detailPanelRef.current;
    if (!target) {
      return;
    }
    workspaceRef.current.scrollTo({ left: target.offsetLeft, behavior });
  };

  const onWorkspaceScroll = () => {
    if (!isMobile || !workspaceRef.current) {
      return;
    }
    const container = workspaceRef.current;
    const entries: Array<[MobilePane, HTMLElement | null]> = [
      ["groups", groupsPanelRef.current],
      ["timeline", timelinePanelRef.current],
      ["detail", detailPanelRef.current],
    ];
    let nextPane: MobilePane = "groups";
    let minDistance = Number.POSITIVE_INFINITY;
    for (const [pane, element] of entries) {
      if (!element) {
        continue;
      }
      const distance = Math.abs(element.offsetLeft - container.scrollLeft);
      if (distance < minDistance) {
        minDistance = distance;
        nextPane = pane;
      }
    }
    setMobilePane(nextPane);
  };

  async function loadOverview() {
    try {
      setOverviewError("");
      setOverview(await api.getOverview());
    } catch (error) {
      setOverviewError(getErrorMessage(error));
    }
  }

  async function loadPipelineStatus() {
    try {
      setPipelineStatusError("");
      setPipelineStatus(await api.getProfilePipelineStatus());
    } catch (error) {
      setPipelineStatusError(getErrorMessage(error));
    }
  }

  async function wakePipeline() {
    try {
      setPipelineWaking(true);
      setPipelineStatusError("");
      const payload = await api.wakeProfilePipeline();
      setPipelineStatus({
        runtime: payload.runtime,
        storage: payload.storage,
      });
      window.setTimeout(() => {
        void loadPipelineStatus();
        void loadOverview();
      }, 1200);
    } catch (error) {
      setPipelineStatusError(getErrorMessage(error));
    } finally {
      setPipelineWaking(false);
    }
  }

  async function resetPipeline() {
    const confirmed = window.confirm(
      "这会清空画像 claim/attribute 和已完成的画像分析任务，然后从历史消息重新分析。原始聊天记录不会删除。确认继续？",
    );
    if (!confirmed) {
      return;
    }
    try {
      setPipelineResetting(true);
      setPipelineStatusError("");
      const payload = await api.resetProfilePipeline();
      setPipelineStatus({
        runtime: payload.runtime,
        storage: payload.storage,
      });
      window.setTimeout(() => {
        void loadPipelineStatus();
        void loadOverview();
        if (selectedGroup) {
          void loadProfiles(true);
        }
      }, 1200);
    } catch (error) {
      setPipelineStatusError(getErrorMessage(error));
    } finally {
      setPipelineResetting(false);
    }
  }

  async function loadGroups() {
    try {
      setGroups((previous) => ({ ...previous, loading: true, error: "" }));
      const payload = await api.getGroups(groupSearch);
      setGroups({
        items: payload.items,
        total: payload.total,
        loading: false,
        error: "",
      });
      if (!selectedGroup && payload.items.length > 0) {
        selectGroup(payload.items[0]);
      }
    } catch (error) {
      setGroups((previous) => ({
        ...previous,
        loading: false,
        error: getErrorMessage(error),
      }));
    }
  }

  async function refreshSelectedGroup() {
    setMessages(EMPTY_LIST<MessageListItem>());
    setNotices(EMPTY_LIST<NoticeListItem>());
    setProfileUsers(EMPTY_LIST<GroupProfileUser>());
    setGroupProfileSummary(null);
    setGroupProfileSummaryError("");
    setDetail(null);
    setDetailError("");
    setSelectedDetail(null);
    await Promise.allSettled([loadMessages(true), loadNotices(true), loadProfiles(true)]);
  }

  function selectGroup(group: GroupRow) {
    setSelectedGroup(group);
    if (isMobile) {
      setMobilePane("timeline");
    }
  }

  async function loadMessages(reset: boolean) {
    if (!selectedGroup) {
      return;
    }
    const nextOffset = reset ? 0 : messages.items.length;
    try {
      setMessages((previous) => ({ ...previous, loading: true, error: "" }));
      const payload = await api.getMessages({
        platformId: selectedGroup.platform_id,
        groupId: selectedGroup.group_id,
        offset: nextOffset,
        limit: MESSAGE_LIMIT,
        direction: messageDirection,
        search: messageSearch,
      });
      setMessages((previous) => ({
        items: reset ? payload.items : [...previous.items, ...payload.items],
        total: payload.total,
        loading: false,
        error: "",
      }));
    } catch (error) {
      setMessages((previous) => ({
        ...previous,
        loading: false,
        error: getErrorMessage(error),
      }));
    }
  }

  async function loadNotices(reset: boolean) {
    if (!selectedGroup) {
      return;
    }
    const nextOffset = reset ? 0 : notices.items.length;
    try {
      setNotices((previous) => ({ ...previous, loading: true, error: "" }));
      const payload = await api.getNotices({
        platformId: selectedGroup.platform_id,
        groupId: selectedGroup.group_id,
        offset: nextOffset,
        limit: NOTICE_LIMIT,
        noticeType,
      });
      setNotices((previous) => ({
        items: reset ? payload.items : [...previous.items, ...payload.items],
        total: payload.total,
        loading: false,
        error: "",
      }));
    } catch (error) {
      setNotices((previous) => ({
        ...previous,
        loading: false,
        error: getErrorMessage(error),
      }));
    }
  }

  async function loadProfiles(reset: boolean) {
    if (!selectedGroup) {
      return;
    }
    const nextOffset = reset ? 0 : profileUsers.items.length;
    try {
      setProfileUsers((previous) => ({ ...previous, loading: true, error: "" }));
      setGroupProfileSummaryError("");
      if (reset) {
        const [summary, users] = await Promise.all([
          api.getGroupProfileSummary(selectedGroup.platform_id, selectedGroup.group_id),
          api.getGroupProfileUsers({
            platformId: selectedGroup.platform_id,
            groupId: selectedGroup.group_id,
            offset: nextOffset,
            limit: PROFILE_LIMIT,
            search: profileSearch,
          }),
        ]);
        setGroupProfileSummary(summary);
        setProfileUsers({
          items: users.items,
          total: users.total,
          loading: false,
          error: "",
        });
        return;
      }
      const users = await api.getGroupProfileUsers({
        platformId: selectedGroup.platform_id,
        groupId: selectedGroup.group_id,
        offset: nextOffset,
        limit: PROFILE_LIMIT,
        search: profileSearch,
      });
      setProfileUsers((previous) => ({
        items: [...previous.items, ...users.items],
        total: users.total,
        loading: false,
        error: "",
      }));
    } catch (error) {
      const message = getErrorMessage(error);
      setProfileUsers((previous) => ({
        ...previous,
        loading: false,
        error: message,
      }));
      setGroupProfileSummaryError(message);
    }
  }

  async function showMessageDetail(item: MessageListItem | number) {
    const id = typeof item === "number" ? item : item.id;
    try {
      setDetailLoading(true);
      setDetailError("");
      const payload = await api.getMessageDetail(id);
      setSelectedDetail({
        kind: "message",
        title: payload.outline || payload.plain_text || `消息 #${payload.id}`,
        messageId: payload.id,
      });
      setDetail({
        kind: "message",
        label: payload.outline || payload.plain_text || `消息 #${payload.id}`,
        data: payload,
      });
      if (isMobile) {
        setMobilePane("detail");
      }
    } catch (error) {
      setDetailError(getErrorMessage(error));
    } finally {
      setDetailLoading(false);
    }
  }

  async function showNoticeDetail(item: NoticeListItem | number) {
    const id = typeof item === "number" ? item : item.id;
    try {
      setDetailLoading(true);
      setDetailError("");
      const payload = await api.getNoticeDetail(id);
      setSelectedDetail({
        kind: "notice",
        title: payload.notice_type || `通知 #${payload.id}`,
        noticeId: payload.id,
      });
      setDetail({
        kind: "notice",
        label: payload.notice_type || `通知 #${payload.id}`,
        data: payload,
      });
      if (isMobile) {
        setMobilePane("detail");
      }
    } catch (error) {
      setDetailError(getErrorMessage(error));
    } finally {
      setDetailLoading(false);
    }
  }

  async function showProfileDetail(user: GroupProfileUser | string) {
    if (!selectedGroup) {
      return;
    }
    const userId = typeof user === "string" ? user : user.user_id;
    try {
      setDetailLoading(true);
      setDetailError("");
      const payload = await api.getUserProfile(
        selectedGroup.platform_id,
        selectedGroup.group_id,
        userId,
      );
      const label = displayUserLabel(
        payload.summary.last_sender_name,
        payload.summary.last_sender_card,
        payload.summary.user_id,
      );
      setSelectedDetail({
        kind: "profile",
        title: label,
        userId,
      });
      setDetail({
        kind: "profile",
        label,
        data: payload,
      });
      if (isMobile) {
        setMobilePane("detail");
      }
    } catch (error) {
      setDetailError(getErrorMessage(error));
    } finally {
      setDetailLoading(false);
    }
  }

  function saveToken() {
    const value = draftToken.trim();
    window.localStorage.setItem("qq-archive-auth-token", value);
    setAuthToken(value);
    setAuthDialogOpen(false);
  }

  const groupCaption = selectedGroup
    ? `${selectedGroup.group_name || "未命名群"} · ${selectedGroup.platform_id}/${selectedGroup.group_id}`
    : "先选择一个群";
  const detailCaption = selectedDetail
    ? selectedDetail.title
    : "点击一条消息、通知或画像查看详情";

  return (
    <div class="app-shell">
      <div class="backdrop backdrop-one" />
      <div class="backdrop backdrop-two" />
      <header class="hero">
        <div class="hero-copy">
          <p class="eyebrow">AstrBot Plugin</p>
          <h1>QQ Group Archive</h1>
          <p class="subline">
            统一查看群消息、撤回、点表情回应、画像 attribute、claim 证据和原始 OneBot payload。
          </p>
        </div>
        <div class="hero-actions">
          <button class="ghost-button" onClick={() => void loadOverview()}>
            刷新概览
          </button>
          <button
            class="ghost-button"
            onClick={() => {
              setDraftToken(authToken);
              setAuthDialogOpen(true);
            }}
          >
            设置令牌
          </button>
        </div>
      </header>

      <OverviewSection overview={overview} error={overviewError} />
      <PipelineStatusSection
        status={pipelineStatus}
        error={pipelineStatusError}
        waking={pipelineWaking}
        resetting={pipelineResetting}
        onRefresh={() => void loadPipelineStatus()}
        onWake={() => void wakePipeline()}
        onReset={() => void resetPipeline()}
      />

      <main
        ref={workspaceRef}
        class="workspace"
        onScroll={onWorkspaceScroll}
      >
        <section ref={groupsPanelRef} class="panel panel-groups">
          <div class="panel-head">
            <div>
              <p class="panel-title">群聊</p>
              <p class="panel-caption">{formatCompactNumber(groups.total)} 个结果</p>
            </div>
            <button class="ghost-button" onClick={() => void loadGroups()}>
              刷新
            </button>
          </div>
          <div class="panel-toolbar">
            <input
              value={groupSearch}
              onInput={(event) =>
                setGroupSearch((event.currentTarget as HTMLInputElement).value)
              }
              type="search"
              placeholder="搜索群号、平台、群名"
            />
          </div>
          <div class="panel-body panel-scroll">
            {groups.error ? <InlineError message={groups.error} /> : null}
            {groups.loading && groups.items.length === 0 ? <LoadingCard /> : null}
            <div class="group-list">
              {groups.items.map((group) => (
                <button
                  key={`${group.platform_id}:${group.group_id}`}
                  class={classNames(
                    "group-card",
                    selectedGroup?.platform_id === group.platform_id &&
                      selectedGroup?.group_id === group.group_id &&
                      "is-active",
                  )}
                  onClick={() => selectGroup(group)}
                >
                  <div class="group-card-top">
                    <strong>{group.group_name || `群 ${group.group_id}`}</strong>
                    <span class="pill neutral">{formatCompactNumber(group.incoming_message_count + group.outgoing_message_count)} 条</span>
                  </div>
                  <p>{group.platform_id} / {group.group_id}</p>
                  <div class="meta-row">
                    <span>通知 {formatCompactNumber(group.notice_count)}</span>
                    <span>最近 {formatTime(Math.max(group.last_message_time, group.last_notice_time))}</span>
                  </div>
                </button>
              ))}
              {!groups.loading && groups.items.length === 0 ? <EmptyState text="没有匹配的群聊。" /> : null}
            </div>
          </div>
        </section>

        <section ref={timelinePanelRef} class="panel panel-timeline">
          <div class="panel-head">
            <div class="panel-head-main">
              <button
                class="ghost-button panel-switch mobile-only"
                onClick={() => setMobilePane("groups")}
              >
                群聊
              </button>
              <div>
                <p class="panel-title">时间线</p>
                <p class="panel-caption">{groupCaption}</p>
              </div>
            </div>
            <button class="ghost-button" onClick={() => void refreshSelectedGroup()} disabled={!selectedGroup}>
              刷新
            </button>
          </div>

          <div class="tabs">
            <button
              class={classNames("tab-button", activeTab === "messages" && "is-active")}
              onClick={() => setActiveTab("messages")}
            >
              消息
            </button>
            <button
              class={classNames("tab-button", activeTab === "notices" && "is-active")}
              onClick={() => setActiveTab("notices")}
            >
              通知
            </button>
            <button
              class={classNames("tab-button", activeTab === "profiles" && "is-active")}
              onClick={() => setActiveTab("profiles")}
            >
              画像
            </button>
          </div>

          <div class="panel-toolbar">
            {activeTab === "messages" ? (
              <>
                <select
                  value={messageDirection}
                  onChange={(event) =>
                    setMessageDirection((event.currentTarget as HTMLSelectElement).value)
                  }
                >
                  <option value="">全部方向</option>
                  <option value="incoming">入站</option>
                  <option value="outgoing">出站</option>
                </select>
                <input
                  value={messageSearch}
                  onInput={(event) =>
                    setMessageSearch((event.currentTarget as HTMLInputElement).value)
                  }
                  type="search"
                  placeholder="搜索文本、昵称、摘要"
                />
              </>
            ) : null}
            {activeTab === "notices" ? (
              <select
                value={noticeType}
                onChange={(event) =>
                  setNoticeType((event.currentTarget as HTMLSelectElement).value)
                }
              >
                <option value="">全部通知</option>
                <option value="group_recall">撤回</option>
                <option value="group_msg_emoji_like">点表情回应</option>
              </select>
            ) : null}
            {activeTab === "profiles" ? (
              <input
                value={profileSearch}
                onInput={(event) =>
                  setProfileSearch((event.currentTarget as HTMLInputElement).value)
                }
                type="search"
                placeholder="搜索成员 QQ、昵称、群名片"
              />
            ) : null}
          </div>

          <div class="panel-body panel-scroll">
            {!selectedGroup ? <EmptyState text="先选择一个群聊。" /> : null}

            {selectedGroup && activeTab === "messages" ? (
              <ListSection
                error={messages.error}
                loading={messages.loading}
                emptyText="当前没有消息。"
                itemCount={messages.items.length}
                loadMoreVisible={messages.items.length < messages.total}
                onLoadMore={() => void loadMessages(false)}
              >
                <div class="timeline-list">
                  {messages.items.map((item) => (
                    <button
                      key={item.id}
                      class={classNames(
                        "timeline-card",
                        selectedDetail?.kind === "message" &&
                          selectedDetail.messageId === item.id &&
                          "is-active",
                      )}
                      onClick={() => void showMessageDetail(item)}
                    >
                      <div class="timeline-card-top">
                        <div>
                          <strong>{displayUserLabel(item.sender_name, item.sender_card, item.sender_id)}</strong>
                          <p>{formatTime(item.event_time)}</p>
                        </div>
                        <div class="pill-row">
                          <span class={classNames("pill", item.direction === "incoming" ? "accent" : "neutral")}>
                            {item.direction === "incoming" ? "入站" : "出站"}
                          </span>
                          {item.is_recalled ? <span class="pill warn">已撤回</span> : null}
                        </div>
                      </div>
                      <p class="timeline-text">{truncateText(item.outline || item.plain_text, 180)}</p>
                    </button>
                  ))}
                </div>
              </ListSection>
            ) : null}

            {selectedGroup && activeTab === "notices" ? (
              <ListSection
                error={notices.error}
                loading={notices.loading}
                emptyText="当前没有通知事件。"
                itemCount={notices.items.length}
                loadMoreVisible={notices.items.length < notices.total}
                onLoadMore={() => void loadNotices(false)}
              >
                <div class="timeline-list">
                  {notices.items.map((item) => (
                    <button
                      key={item.id}
                      class={classNames(
                        "timeline-card",
                        selectedDetail?.kind === "notice" &&
                          selectedDetail.noticeId === item.id &&
                          "is-active",
                      )}
                      onClick={() => void showNoticeDetail(item)}
                    >
                      <div class="timeline-card-top">
                        <div>
                          <strong>{item.notice_type || "notice"}</strong>
                          <p>{formatTime(item.event_time)}</p>
                        </div>
                        <div class="pill-row">
                          {item.sub_type ? <span class="pill neutral">{item.sub_type}</span> : null}
                          {item.reaction_count ? <span class="pill accent">x{item.reaction_count}</span> : null}
                        </div>
                      </div>
                      <p class="timeline-text">
                        actor: {item.actor_user_id || "-"} · target: {item.target_user_id || "-"}
                      </p>
                    </button>
                  ))}
                </div>
              </ListSection>
            ) : null}

            {selectedGroup && activeTab === "profiles" ? (
              <ListSection
                error={profileUsers.error || groupProfileSummaryError}
                loading={profileUsers.loading}
                emptyText="当前没有画像统计。"
                itemCount={profileUsers.items.length}
                loadMoreVisible={profileUsers.items.length < profileUsers.total}
                onLoadMore={() => void loadProfiles(false)}
              >
                <div class="profile-summary-block">
                  <GroupProfileSummarySection summary={groupProfileSummary} />
                </div>
                <div class="timeline-list">
                  {profileUsers.items.map((item) => (
                    <button
                      key={item.user_id}
                      class={classNames(
                        "timeline-card",
                        selectedDetail?.kind === "profile" &&
                          selectedDetail.userId === item.user_id &&
                          "is-active",
                      )}
                      onClick={() => void showProfileDetail(item)}
                    >
                      <div class="timeline-card-top">
                        <div>
                          <strong>{displayUserLabel(item.last_sender_name, item.last_sender_card, item.user_id)}</strong>
                          <p>QQ {item.user_id}</p>
                        </div>
                        <span class="pill accent">{formatCompactNumber(item.total_message_count)} 条</span>
                      </div>
                      <div class="meta-row">
                        <span>图片 {formatCompactNumber(item.image_count)}</span>
                        <span>@ {formatCompactNumber(item.at_count)}</span>
                        <span>撤回 {formatCompactNumber(item.recalled_message_count)}</span>
                      </div>
                      <p class="timeline-text">最近活跃 {formatTime(item.last_seen_at)}</p>
                    </button>
                  ))}
                </div>
              </ListSection>
            ) : null}
          </div>
        </section>

        <section ref={detailPanelRef} class="panel panel-detail">
          <div class="panel-head">
            <div class="panel-head-main">
              <button
                class="ghost-button panel-switch mobile-only"
                onClick={() => setMobilePane("timeline")}
              >
                返回
              </button>
              <div>
                <p class="panel-title">详情</p>
                <p class="panel-caption">{detailCaption}</p>
              </div>
            </div>
          </div>
          <div class="panel-body panel-scroll detail-scroll">
            {detailLoading ? <LoadingCard /> : null}
            {detailError ? <InlineError message={detailError} /> : null}
            {!detailLoading && !detail && !detailError ? (
              <EmptyState text="点击一条消息、通知或成员画像查看详情。" />
            ) : null}
            {detail?.kind === "message" ? (
              <MessageDetailSection detail={detail.data} api={api} />
            ) : null}
            {detail?.kind === "notice" ? (
              <NoticeDetailSection detail={detail.data} />
            ) : null}
            {detail?.kind === "profile" ? (
              <ProfileDetailSection
                detail={detail.data}
                onOpenMessage={(messageId) => void showMessageDetail(messageId)}
              />
            ) : null}
          </div>
        </section>
      </main>

      <dialog
        ref={authDialogRef}
        class="auth-dialog"
        onClose={() => setAuthDialogOpen(false)}
      >
        <form
          method="dialog"
          class="auth-card"
          onSubmit={(event) => {
            event.preventDefault();
            saveToken();
          }}
        >
          <p class="panel-title">访问令牌</p>
          <p class="subtle-copy">
            如果你配置了 <code>webui_auth_token</code>，在这里填入同一个值。
          </p>
          <input
            value={draftToken}
            onInput={(event) =>
              setDraftToken((event.currentTarget as HTMLInputElement).value)
            }
            type="password"
            placeholder="X-Auth-Token"
          />
          <div class="auth-actions">
            <button
              type="button"
              class="ghost-button"
              onClick={() => setAuthDialogOpen(false)}
            >
              关闭
            </button>
            <button type="submit" class="primary-button">
              保存
            </button>
          </div>
        </form>
      </dialog>
    </div>
  );
}

function OverviewSection(props: {
  overview: OverviewPayload | null;
  error: string;
}) {
  const cards = props.overview
    ? [
        { label: "群聊", value: formatCompactNumber(props.overview.total_groups) },
        { label: "入站消息", value: formatCompactNumber(props.overview.incoming_messages) },
        { label: "通知事件", value: formatCompactNumber(props.overview.notice_events) },
        { label: "画像成员", value: formatCompactNumber(props.overview.profile_group_users) },
        { label: "Claim", value: formatCompactNumber(props.overview.profile_claims) },
        { label: "当前属性", value: formatCompactNumber(props.overview.profile_attributes) },
        { label: "互动边", value: formatCompactNumber(props.overview.interaction_edges) },
        { label: "画像更新时间", value: formatTime(props.overview.last_profile_update_time) },
      ]
    : [];

  return (
    <section class="stats-strip">
      {props.error ? <InlineError message={props.error} /> : null}
      {!props.error && cards.length === 0 ? <LoadingCard compact /> : null}
      {cards.map((card) => (
        <article key={card.label} class="stat-card">
          <span>{card.label}</span>
          <strong>{card.value}</strong>
        </article>
      ))}
    </section>
  );
}

function PipelineStatusSection(props: {
  status: ProfilePipelineStatusPayload | null;
  error: string;
  waking: boolean;
  resetting: boolean;
  onRefresh: () => void;
  onWake: () => void;
  onReset: () => void;
}) {
  const runtime = props.status?.runtime;
  const storage = props.status?.storage;
  const jobStatuses = storage?.job_statuses ?? [];
  const blockStatuses = storage?.block_statuses ?? [];
  const latestError = storage?.latest_jobs.find((job) => job.last_error)?.last_error || "";
  const llmModeEnabled = runtime?.mode === "astrbot_llm";

  return (
    <section class="pipeline-card">
      <div class="pipeline-head">
        <div>
          <p class="eyebrow">Profile Pipeline</p>
          <h2>画像分析状态</h2>
          <p class="subtle-copy">
            看这里可以确认是否真的在切 batch、跑 job、写入 claim 和 attribute。
          </p>
        </div>
        <div class="hero-actions">
          <button class="ghost-button" onClick={props.onRefresh}>
            刷新状态
          </button>
          <button
            class="primary-button"
            onClick={props.onWake}
            disabled={props.waking || !llmModeEnabled}
            title={!llmModeEnabled ? "需要先把画像流水线模式改成 astrbot_llm" : ""}
          >
            {props.waking ? "触发中…" : "立即触发分析"}
          </button>
          <button
            class="danger-button"
            onClick={props.onReset}
            disabled={props.resetting || !llmModeEnabled}
            title={!llmModeEnabled ? "需要先把画像流水线模式改成 astrbot_llm" : ""}
          >
            {props.resetting ? "重置中…" : "重置并重跑"}
          </button>
        </div>
      </div>

      {props.error ? <InlineError message={props.error} /> : null}
      {props.status && !llmModeEnabled ? (
        <InlineError message="当前不是 astrbot_llm 模式；heuristic 只验证流水线，不会提取画像 claim。请先在插件配置里把画像流水线模式改成 astrbot_llm，然后重载插件。" />
      ) : null}
      {!props.error && !props.status ? <LoadingCard compact /> : null}

      {props.status ? (
        <>
          <div class="pipeline-grid">
            <StatBlock
              label="启用状态"
              value={runtime?.enabled ? "已启用" : "未启用"}
            />
            <StatBlock
              label="LangGraph"
              value={runtime?.langgraph_available ? "可用" : "不可用"}
            />
            <StatBlock
              label="Runner"
              value={runtime?.runner_running ? "运行中" : "未运行"}
            />
            <StatBlock label="模式" value={runtime?.mode || "-"} />
            <StatBlock
              label="入站消息"
              value={formatCompactNumber(storage?.archived_incoming_messages)}
            />
            <StatBlock label="Batch" value={formatCompactNumber(storage?.total_blocks)} />
            <StatBlock label="Job" value={formatCompactNumber(storage?.total_jobs)} />
            <StatBlock label="Claim" value={formatCompactNumber(storage?.total_claims)} />
            <StatBlock
              label="Attribute"
              value={formatCompactNumber(storage?.total_attributes)}
            />
            <StatBlock
              label="最新消息"
              value={storage?.last_message ? `#${storage.last_message.id}` : "-"}
            />
            <StatBlock
              label="模型超时"
              value={`${runtime?.llm_timeout_sec ?? "-"} 秒`}
            />
            <StatBlock
              label="Running 恢复"
              value={`${runtime?.running_job_timeout_sec ?? "-"} 秒`}
            />
          </div>

          <div class="pipeline-columns">
            <SectionCard title="Job 状态">
              <StatusChips rows={jobStatuses} emptyText="还没有创建 job。" />
            </SectionCard>
            <SectionCard title="Batch 状态">
              <StatusChips rows={blockStatuses} emptyText="还没有切出 batch。" />
            </SectionCard>
          </div>

          <SectionCard title="最近任务">
            {storage?.latest_jobs.length ? (
              <div class="section-stack">
                {storage.latest_jobs.map((job) => (
                  <PipelineJobCard key={job.id} job={job} />
                ))}
              </div>
            ) : (
              <EmptyState text="还没有画像分析任务。" compact />
            )}
          </SectionCard>

          {latestError ? (
            <InlineError message={`最近错误：${latestError}`} />
          ) : null}
        </>
      ) : null}
    </section>
  );
}

type PipelineJob = ProfilePipelineStatusPayload["storage"]["latest_jobs"][number];

function PipelineJobCard(props: { job: PipelineJob }) {
  const job = props.job;
  const workflowState = job.workflow_state || {};
  const currentStage = String(workflowState.current_stage || "");
  const stageDetail = String(workflowState.stage_detail || "");
  const elapsedSeconds = job.started_at
    ? (job.finished_at || Math.floor(Date.now() / 1000)) - job.started_at
    : 0;
  const summary = job.result_summary || (workflowState.summary as Record<string, unknown> | undefined);

  return (
    <div class="job-card">
      <div class="segment-head">
        <strong>Job #{job.id}</strong>
        <span class={classNames("pill", statusPillClass(job.status))}>
          {formatJobStatus(job.status)}
        </span>
      </div>
      <div class="meta-row">
        <span>{job.platform_id}/{job.group_id}</span>
        <span>{formatCompactNumber(job.message_count)} 条消息</span>
        <span>尝试 {formatCompactNumber(job.attempt_count)} 次</span>
        <span>耗时 {formatDuration(elapsedSeconds)}</span>
      </div>
      <div class="meta-row">
        <span>消息 #{job.block_key.split(":").slice(-2).join(" - #") || "-"}</span>
        <span>阶段 {formatPipelineStage(currentStage)}</span>
        <span>更新 {formatTime(job.updated_at)}</span>
      </div>
      {stageDetail ? <p class="subtle-copy">{stageDetail}</p> : null}
      {job.status === "pending" ? (
        <p class="subtle-copy">等待前面的 running job 完成。当前 runner 按 job 顺序串行处理，避免并发调用模型打爆 Provider。</p>
      ) : null}
      {summary ? <pre>{JSON.stringify(summary, null, 2)}</pre> : null}
      {job.last_error ? <InlineError message={job.last_error} /> : null}
    </div>
  );
}

function formatJobStatus(value: string) {
  const mapping: Record<string, string> = {
    pending: "等待中",
    running: "运行中",
    completed: "已完成",
    failed: "失败待重试",
  };
  return mapping[value] || value || "未知";
}

function formatPipelineStage(value: string) {
  const mapping: Record<string, string> = {
    load_job: "读取任务",
    judge_block: "候选判断",
    judge_done: "候选判断完成",
    extract_claims: "事实抽取",
    resolve_claims: "合并消歧",
    resolve_done: "合并消歧完成",
    persist_claims: "写入数据库",
    persist_without_claims: "无候选完成",
  };
  return mapping[value] || value || "未开始";
}

function StatusChips(props: {
  rows: Array<{ status: string; count: number }>;
  emptyText: string;
}) {
  if (props.rows.length === 0) {
    return <EmptyState text={props.emptyText} compact />;
  }
  return (
    <div class="chip-cloud">
      {props.rows.map((row) => (
        <span key={row.status} class="metric-chip">
          {row.status} · {formatCompactNumber(row.count)}
        </span>
      ))}
    </div>
  );
}

function GroupProfileSummarySection(props: {
  summary: GroupProfileSummary | null;
}) {
  if (!props.summary) {
    return <LoadingCard compact />;
  }

  const summary = props.summary.summary || {};
  return (
    <div class="section-stack">
      <div class="summary-grid">
        <StatBlock label="成员数" value={formatCompactNumber(summary.tracked_users)} />
        <StatBlock
          label="消息总量"
          value={formatCompactNumber(
            (summary.incoming_message_count ?? 0) + (summary.outgoing_message_count ?? 0),
          )}
        />
        <StatBlock label="图片" value={formatCompactNumber(summary.image_count)} />
        <StatBlock label="撤回记录" value={formatCompactNumber(summary.recalled_message_count)} />
      </div>

      <SectionCard title="群画像速览">
        <div class="chip-cloud">
          {props.summary.top_attributes.map((row) => (
            <span key={row.attribute_type} class="metric-chip">
              <span title={row.attribute_type}>{row.attribute_label || formatAttributeLabel(row.attribute_type)}</span> · {formatCompactNumber(row.user_count)} 人 · 平均 {formatPercent(row.avg_confidence)}
            </span>
          ))}
          {props.summary.top_attributes.length === 0 ? <EmptyState text="还没有画像 attribute。" compact /> : null}
        </div>
      </SectionCard>

      <SectionCard title="Claim 状态">
        <div class="chip-cloud">
          {props.summary.claim_statuses.map((row) => (
            <span key={`${row.attribute_type}:${row.status}`} class="metric-chip">
              <span title={row.attribute_type}>{row.attribute_label || formatAttributeLabel(row.attribute_type)}</span> / {formatClaimStatus(row.status)} · {formatCompactNumber(row.claim_count)}
            </span>
          ))}
          {props.summary.claim_statuses.length === 0 ? <EmptyState text="还没有 claim。" compact /> : null}
        </div>
      </SectionCard>
    </div>
  );
}

function MessageDetailSection(props: {
  detail: MessageDetail;
  api: ApiClient;
}) {
  const detail = props.detail;
  return (
    <div class="section-stack">
      <SectionCard title="消息元信息">
        <MetaGrid
          rows={[
            ["平台", detail.platform_id],
            ["群号", detail.group_id],
            ["群名", detail.group_name || "-"],
            ["发送者", displayUserLabel(detail.sender_name, detail.sender_card, detail.sender_id)],
            ["方向", detail.direction],
            ["消息 ID", detail.message_id || "-"],
            ["时间", formatTime(detail.event_time)],
            ["已撤回", detail.is_recalled ? "是" : "否"],
          ]}
        />
      </SectionCard>

      <SectionCard title="消息段">
        <div class="segment-stack">
          {detail.segments.map((segment) => (
            <article key={segment.id} class="segment-card">
              <div class="segment-head">
                <strong>{segment.seg_type}</strong>
                <span class="pill neutral">#{segment.seg_index}</span>
              </div>
              {segment.text_content ? <p>{segment.text_content}</p> : null}
              {segment.seg_type === "image" && segment.local_path ? (
                <img
                  class="segment-image"
                  src={props.api.mediaUrl(segment.local_path)}
                  alt={segment.text_content || "image"}
                />
              ) : null}
              {segment.local_path && segment.seg_type !== "image" ? (
                <a class="inline-link" href={props.api.mediaUrl(segment.local_path)} target="_blank" rel="noreferrer">
                  打开本地附件
                </a>
              ) : null}
              {segment.source_url ? (
                <a class="inline-link" href={segment.source_url} target="_blank" rel="noreferrer">
                  原始链接
                </a>
              ) : null}
              {segment.seg_data ? <pre>{JSON.stringify(segment.seg_data, null, 2)}</pre> : null}
            </article>
          ))}
          {detail.segments.length === 0 ? <EmptyState text="没有规范化消息段。" compact /> : null}
        </div>
      </SectionCard>

      <SectionCard title="转发节点">
        <div class="segment-stack">
          {detail.forward_nodes.map((node) => (
            <article key={node.id} class="segment-card">
              <div class="segment-head">
                <strong>{node.sender_name || node.sender_id || `节点 ${node.node_index}`}</strong>
                <span>{formatTime(node.time)}</span>
              </div>
              <pre>{JSON.stringify(node.content, null, 2)}</pre>
            </article>
          ))}
          {detail.forward_nodes.length === 0 ? <EmptyState text="没有转发展开节点。" compact /> : null}
        </div>
      </SectionCard>

      <SectionCard title="原始事件">
        <pre>{JSON.stringify(detail.raw_event, null, 2)}</pre>
      </SectionCard>
    </div>
  );
}

function NoticeDetailSection(props: {
  detail: NoticeDetail;
}) {
  const detail = props.detail;
  return (
    <div class="section-stack">
      <SectionCard title="通知元信息">
        <MetaGrid
          rows={[
            ["类型", detail.notice_type],
            ["子类型", detail.sub_type || "-"],
            ["群号", detail.group_id],
            ["群名", detail.group_name || "-"],
            ["actor", detail.actor_user_id || "-"],
            ["operator", detail.operator_id || "-"],
            ["target", detail.target_user_id || "-"],
            ["message_id", detail.message_id || "-"],
            ["时间", formatTime(detail.event_time)],
          ]}
        />
      </SectionCard>
      <SectionCard title="原始事件">
        <pre>{JSON.stringify(detail.raw_event, null, 2)}</pre>
      </SectionCard>
    </div>
  );
}

function ProfileDetailSection(props: {
  detail: UserProfileDetail;
  onOpenMessage: (messageId: number) => void;
}) {
  const summary = props.detail.summary;
  const globalSummary = props.detail.global_summary;
  return (
    <div class="section-stack">
      <SectionCard title="成员概览">
        <div class="profile-heading">
          <div>
            <h2>{displayUserLabel(summary.last_sender_name, summary.last_sender_card, summary.user_id)}</h2>
            <p>QQ {summary.user_id}</p>
          </div>
          <div class="pill-row">
            <span class="pill accent">{formatCompactNumber(summary.total_message_count)} 条消息</span>
            <span class="pill neutral">最近 {formatTime(summary.last_seen_at)}</span>
          </div>
        </div>
        <div class="summary-grid">
          <StatBlock label="文字数" value={formatCompactNumber(summary.total_text_chars)} />
          <StatBlock label="图片" value={formatCompactNumber(summary.image_count)} />
          <StatBlock label="回复" value={formatCompactNumber(summary.reply_count)} />
          <StatBlock label="被撤回" value={formatCompactNumber(summary.recalled_message_count)} />
        </div>
        {globalSummary ? (
          <p class="subtle-copy">
            跨群累计 {formatCompactNumber(globalSummary.total_message_count)} 条，最后活跃 {formatTime(globalSummary.last_seen_at)}。
          </p>
        ) : null}
      </SectionCard>

      <SectionCard title="当前画像属性">
        <div class="attribute-grid">
          {props.detail.attributes.map((attribute) => (
            <article key={attribute.attribute_type} class="attribute-card">
              <div class="segment-head">
                <strong title={attribute.attribute_type}>
                  {attribute.attribute_label || formatAttributeLabel(attribute.attribute_type)}
                </strong>
                <span class={classNames("pill", statusPillClass(attribute.status))}>
                  {formatClaimStatus(attribute.status)}
                </span>
              </div>
              <p class="attribute-value">{attribute.normalized_value || attribute.current_value || "-"}</p>
              <div class="meta-row">
                <span>{formatSourceKind(attribute.source_kind)}</span>
                <span>置信度 {formatPercent(attribute.confidence)}</span>
              </div>
              <div class="meta-row">
                <span>证据 {formatCompactNumber(attribute.evidence_count)}</span>
                <span>更新 {formatTime(attribute.updated_at)}</span>
              </div>
              {attribute.current_claim?.resolver_note ? (
                <p class="subtle-copy">{attribute.current_claim.resolver_note}</p>
              ) : null}
            </article>
          ))}
          {props.detail.attributes.length === 0 ? <EmptyState text="当前没有聚合属性。" compact /> : null}
        </div>
      </SectionCard>

      <SectionCard title="近期 Claim">
        <div class="claim-stack">
          {props.detail.claims.map((claim) => (
            <article key={claim.id} class="claim-card">
              <div class="claim-top">
                <div>
                  <strong title={claim.attribute_type}>
                    {claim.attribute_label || formatAttributeLabel(claim.attribute_type)}
                  </strong>
                  <p>{claim.normalized_value || claim.raw_value || "-"}</p>
                </div>
                <div class="pill-row">
                  <span class={classNames("pill", statusPillClass(claim.status))}>
                    {formatClaimStatus(claim.status)}
                  </span>
                  <span class="pill neutral">{formatSourceKind(claim.source_kind)}</span>
                </div>
              </div>
              <div class="meta-row">
                <span>置信度 {formatPercent(claim.confidence)}</span>
                <span>最近证据 {formatTime(claim.last_seen_at)}</span>
              </div>
              {claim.resolver_note ? <p class="subtle-copy">{claim.resolver_note}</p> : null}
              <div class="evidence-list">
                {claim.evidence.map((evidence) => (
                  <button
                    key={`${claim.id}:${evidence.message_row_id}:${evidence.evidence_kind}`}
                    class="evidence-chip"
                    onClick={() => props.onOpenMessage(evidence.message_row_id)}
                  >
                    <strong>{evidence.sender_label}</strong>
                    <span>{truncateText(evidence.excerpt || evidence.outline || evidence.plain_text, 72)}</span>
                    <em>{formatTime(evidence.event_time)}</em>
                  </button>
                ))}
              </div>
              {claim.evidence.length === 0 ? <EmptyState text="这个 claim 还没有显式证据回链。" compact /> : null}
            </article>
          ))}
          {props.detail.claims.length === 0 ? <EmptyState text="还没有结构化 claim。" compact /> : null}
        </div>
      </SectionCard>

      <SectionCard title="互动关系">
        <div class="interaction-columns">
          <InteractionList
            title="主动互动"
            rows={props.detail.outgoing_interactions}
            nameKey="target_label"
            userKey="target_user_id"
          />
          <InteractionList
            title="收到互动"
            rows={props.detail.incoming_interactions}
            nameKey="source_label"
            userKey="source_user_id"
          />
        </div>
      </SectionCard>

      <SectionCard title="属性变更历史">
        <div class="history-list">
          {props.detail.attribute_history.map((row) => (
            <article key={row.id} class="history-row">
              <strong title={row.attribute_type}>{row.attribute_label || formatAttributeLabel(row.attribute_type)}</strong>
              <p>
                {row.previous_claim_normalized_value || "空"} → {row.claim_normalized_value || "空"}
              </p>
              <div class="meta-row">
                <span>{row.action}</span>
                <span>{formatTime(row.created_at)}</span>
              </div>
            </article>
          ))}
          {props.detail.attribute_history.length === 0 ? <EmptyState text="还没有属性切换历史。" compact /> : null}
        </div>
      </SectionCard>

      <SectionCard title="每日活跃">
        <div class="daily-list">
          {props.detail.daily_stats.map((row) => (
            <article key={String(row.stat_date)} class="daily-row">
              <strong>{String(row.stat_date)}</strong>
              <div class="meta-row">
                <span>消息 {formatCompactNumber(Number(row.incoming_message_count || 0) + Number(row.outgoing_message_count || 0))}</span>
                <span>文字 {formatCompactNumber(Number(row.total_text_chars || 0))}</span>
                <span>图片 {formatCompactNumber(Number(row.image_count || 0))}</span>
              </div>
            </article>
          ))}
          {props.detail.daily_stats.length === 0 ? <EmptyState text="还没有日活跃统计。" compact /> : null}
        </div>
      </SectionCard>
    </div>
  );
}

function InteractionList(props: {
  title: string;
  rows: Array<Record<string, number | string>>;
  nameKey: string;
  userKey: string;
}) {
  return (
    <div class="section-stack">
      <h3>{props.title}</h3>
      {props.rows.map((row, index) => (
        <article key={`${props.title}:${index}`} class="history-row">
          <strong>{String(row[props.nameKey] || row[props.userKey] || "-")}</strong>
          <p>{String(row.interaction_type || "-")}</p>
          <div class="meta-row">
            <span>{formatCompactNumber(Number(row.interaction_count || 0))} 次</span>
            <span>{formatTime(Number(row.last_seen_at || 0))}</span>
          </div>
        </article>
      ))}
      {props.rows.length === 0 ? <EmptyState text="暂无互动记录。" compact /> : null}
    </div>
  );
}

function MetaGrid(props: {
  rows: Array<[string, string]>;
}) {
  return (
    <div class="meta-grid">
      {props.rows.map(([label, value]) => (
        <div key={label} class="meta-cell">
          <span>{label}</span>
          <strong>{value}</strong>
        </div>
      ))}
    </div>
  );
}

function SectionCard(props: {
  title: string;
  children: ComponentChildren;
}) {
  return (
    <section class="section-card">
      <div class="section-card-head">
        <h3>{props.title}</h3>
      </div>
      <div class="section-card-body">{props.children}</div>
    </section>
  );
}

function ListSection(props: {
  error: string;
  loading: boolean;
  emptyText: string;
  itemCount: number;
  loadMoreVisible: boolean;
  onLoadMore: () => void;
  children: ComponentChildren;
}) {
  return (
    <div class="section-stack">
      {props.error ? <InlineError message={props.error} /> : null}
      {props.children}
      {props.loading ? <LoadingCard compact /> : null}
      {!props.loading && !props.error && props.itemCount === 0 ? (
        <EmptyState text={props.emptyText} />
      ) : null}
      {props.loadMoreVisible ? (
        <button class="wide-button" onClick={props.onLoadMore}>
          加载更多
        </button>
      ) : null}
    </div>
  );
}

function StatBlock(props: {
  label: string;
  value: string;
}) {
  return (
    <article class="mini-stat">
      <span>{props.label}</span>
      <strong>{props.value}</strong>
    </article>
  );
}

function InlineError(props: {
  message: string;
}) {
  return <div class="alert error">{props.message}</div>;
}

function EmptyState(props: {
  text: string;
  compact?: boolean;
}) {
  return <div class={classNames("empty-state", props.compact && "compact")}>{props.text}</div>;
}

function LoadingCard(props: {
  compact?: boolean;
}) {
  return (
    <div class={classNames("loading-card", props.compact && "compact")}>
      <span class="spinner" />
      <p>加载中…</p>
    </div>
  );
}

function statusPillClass(status: string) {
  if (status === "accepted") {
    return "success";
  }
  if (status === "conflicted") {
    return "warn";
  }
  if (status === "outdated") {
    return "neutral";
  }
  if (status === "rejected") {
    return "danger";
  }
  return "accent";
}

function getErrorMessage(error: unknown) {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error || "unknown error");
}

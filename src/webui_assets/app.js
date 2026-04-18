const state = {
  authToken: localStorage.getItem("qq-archive-auth-token") || "",
  groups: [],
  selectedGroup: null,
  selectedRecord: null,
  activeTab: "messages",
  mobilePane: "groups",
  messageOffset: 0,
  messageLimit: 50,
  noticeOffset: 0,
  noticeLimit: 50,
  profileOffset: 0,
  profileLimit: 50,
};

const el = {
  workspace: document.getElementById("workspace"),
  groupsPanel: document.getElementById("groups-panel"),
  timelinePanel: document.getElementById("timeline-panel"),
  detailPanel: document.getElementById("detail-panel"),
  stats: {
    groups: document.getElementById("stat-groups"),
    incoming: document.getElementById("stat-incoming"),
    outgoing: document.getElementById("stat-outgoing"),
    recalled: document.getElementById("stat-recalled"),
    notices: document.getElementById("stat-notices"),
    forwards: document.getElementById("stat-forwards"),
  },
  groupTotalCaption: document.getElementById("group-total-caption"),
  timelineCaption: document.getElementById("timeline-caption"),
  detailCaption: document.getElementById("detail-caption"),
  groupSearch: document.getElementById("group-search"),
  groupList: document.getElementById("group-list"),
  refreshOverview: document.getElementById("refresh-overview"),
  refreshGroups: document.getElementById("refresh-groups"),
  refreshActive: document.getElementById("refresh-active"),
  messageDirection: document.getElementById("message-direction"),
  messageSearch: document.getElementById("message-search"),
  noticeType: document.getElementById("notice-type"),
  profileSearch: document.getElementById("profile-search"),
  mobileShowGroups: document.getElementById("mobile-show-groups"),
  mobileBackToTimeline: document.getElementById("mobile-back-to-timeline"),
  messageList: document.getElementById("message-list"),
  noticeList: document.getElementById("notice-list"),
  profileGroupSummary: document.getElementById("profile-group-summary"),
  profileUserList: document.getElementById("profile-user-list"),
  loadMoreMessages: document.getElementById("load-more-messages"),
  loadMoreNotices: document.getElementById("load-more-notices"),
  loadMoreProfiles: document.getElementById("load-more-profiles"),
  detailView: document.getElementById("detail-view"),
  tabs: document.querySelectorAll(".tab"),
  messagesView: document.getElementById("messages-view"),
  noticesView: document.getElementById("notices-view"),
  profilesView: document.getElementById("profiles-view"),
  messagesToolbar: document.getElementById("messages-toolbar"),
  noticesToolbar: document.getElementById("notices-toolbar"),
  profilesToolbar: document.getElementById("profiles-toolbar"),
  authButton: document.getElementById("auth-button"),
  authDialog: document.getElementById("auth-dialog"),
  authTokenInput: document.getElementById("auth-token-input"),
  saveAuthToken: document.getElementById("save-auth-token"),
};

async function api(path, options = {}) {
  const headers = new Headers(options.headers || {});
  if (state.authToken) {
    headers.set("X-Auth-Token", state.authToken);
  }

  const response = await fetch(path, {
    ...options,
    headers,
  });

  if (response.status === 401) {
    showAuthDialog();
    throw new Error("unauthorized");
  }

  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `HTTP ${response.status}`);
  }

  const contentType = response.headers.get("content-type") || "";
  if (contentType.includes("application/json")) {
    return response.json();
  }
  return response.text();
}

function showAuthDialog() {
  el.authTokenInput.value = state.authToken;
  el.authDialog.showModal();
}

function saveAuthToken() {
  state.authToken = el.authTokenInput.value.trim();
  localStorage.setItem("qq-archive-auth-token", state.authToken);
}

function formatTime(value) {
  if (!value) {
    return "-";
  }
  return new Date(value * 1000).toLocaleString("zh-CN", { hour12: false });
}

function escapeHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function formatJson(value) {
  return escapeHtml(JSON.stringify(value ?? null, null, 2));
}

function formatCount(value) {
  return String(value ?? 0);
}

function renderEmpty(container, text) {
  container.innerHTML = `
    <div class="empty-state">
      <p>${escapeHtml(text)}</p>
    </div>
  `;
}

function currentGroupLabel(group) {
  if (!group) {
    return "先选择一个群";
  }
  const groupName = group.group_name ? `${group.group_name} ` : "";
  return `${groupName}(${group.platform_id} / ${group.group_id})`;
}

function mediaUrl(relativePath) {
  const encoded = String(relativePath)
    .split("/")
    .map((part) => encodeURIComponent(part))
    .join("/");
  const tokenQuery = state.authToken ? `?token=${encodeURIComponent(state.authToken)}` : "";
  return `/api/media/${encoded}${tokenQuery}`;
}

function isMobileLayout() {
  return window.matchMedia("(max-width: 860px)").matches;
}

function getMobilePaneElement(pane) {
  if (pane === "detail") {
    return el.detailPanel;
  }
  if (pane === "timeline") {
    return el.timelinePanel;
  }
  return el.groupsPanel;
}

function normalizeMobilePane() {
  if (!state.selectedGroup) {
    return "groups";
  }
  if (!["groups", "timeline", "detail"].includes(state.mobilePane)) {
    return "timeline";
  }
  return state.mobilePane;
}

function applyMobilePane(behavior = "auto") {
  state.mobilePane = normalizeMobilePane();
  if (!isMobileLayout()) {
    return;
  }

  const target = getMobilePaneElement(state.mobilePane);
  if (!target) {
    return;
  }

  el.workspace.scrollTo({
    left: target.offsetLeft,
    behavior,
  });
}

function setMobilePane(pane, behavior = "smooth") {
  state.mobilePane = pane;
  applyMobilePane(behavior);
}

function syncMobilePaneFromScroll() {
  if (!isMobileLayout()) {
    return;
  }

  const candidates = [
    { key: "groups", element: el.groupsPanel },
    { key: "timeline", element: el.timelinePanel },
    { key: "detail", element: el.detailPanel },
  ];
  const currentLeft = el.workspace.scrollLeft;
  let closest = candidates[0];
  let minDistance = Number.POSITIVE_INFINITY;

  for (const candidate of candidates) {
    const distance = Math.abs(candidate.element.offsetLeft - currentLeft);
    if (distance < minDistance) {
      minDistance = distance;
      closest = candidate;
    }
  }

  state.mobilePane = closest.key;
}

async function loadOverview() {
  const overview = await api("/api/overview");
  el.stats.groups.textContent = formatCount(overview.total_groups);
  el.stats.incoming.textContent = formatCount(overview.incoming_messages);
  el.stats.outgoing.textContent = formatCount(overview.outgoing_messages);
  el.stats.recalled.textContent = formatCount(overview.recalled_messages);
  el.stats.notices.textContent = formatCount(overview.notice_events);
  el.stats.forwards.textContent = formatCount(overview.forward_nodes);
}

async function loadGroups() {
  const search = el.groupSearch.value.trim();
  const data = await api(`/api/groups?limit=200&offset=0&search=${encodeURIComponent(search)}`);
  state.groups = data.items || [];
  el.groupTotalCaption.textContent = `${data.total || 0} 个结果`;
  renderGroups();
}

function renderGroups() {
  if (!state.groups.length) {
    renderEmpty(el.groupList, "没有找到匹配的群聊。");
    return;
  }

  el.groupList.innerHTML = state.groups
    .map((group) => {
      const active =
        state.selectedGroup &&
        state.selectedGroup.platform_id === group.platform_id &&
        state.selectedGroup.group_id === group.group_id;

      return `
        <button
          class="list-item group-item ${active ? "is-active" : ""}"
          data-platform-id="${escapeHtml(group.platform_id)}"
          data-group-id="${escapeHtml(group.group_id)}"
        >
          <span class="item-title">${escapeHtml(group.group_name || group.group_id)}</span>
          <span class="item-meta">${escapeHtml(group.platform_id)} / ${escapeHtml(group.group_id)}</span>
          <span class="item-meta">
            消息 ${formatCount(group.message_count)}
            · 通知 ${formatCount(group.notice_count)}
            · 用户 ${formatCount(group.tracked_user_count)}
          </span>
          <span class="item-meta">最近 ${formatTime(group.last_event_time)}</span>
        </button>
      `;
    })
    .join("");

  for (const node of el.groupList.querySelectorAll(".group-item")) {
    node.addEventListener("click", () => {
      const group = state.groups.find(
        (item) =>
          item.platform_id === node.dataset.platformId &&
          item.group_id === node.dataset.groupId,
      );
      void selectGroup(group || null);
    });
  }
}

async function selectGroup(group) {
  state.selectedGroup = group;
  state.selectedRecord = null;
  state.messageOffset = 0;
  state.noticeOffset = 0;
  state.profileOffset = 0;
  if (group) {
    setMobilePane("timeline", "smooth");
  } else {
    setMobilePane("groups", "smooth");
  }
  el.timelineCaption.textContent = currentGroupLabel(group);
  el.detailCaption.textContent = "点击一条消息、通知或成员画像查看详情";
  renderGroups();
  renderEmpty(el.detailView, "当前没有选中的记录。");
  await Promise.all([loadMessages(true), loadNotices(true), loadProfiles(true)]);
}

async function loadMessages(reset = false) {
  if (!state.selectedGroup) {
    renderEmpty(el.messageList, "先选择一个群聊。");
    el.loadMoreMessages.classList.add("hidden");
    return;
  }

  if (reset) {
    state.messageOffset = 0;
    el.messageList.innerHTML = "";
  }

  const query = new URLSearchParams({
    platform_id: state.selectedGroup.platform_id,
    group_id: state.selectedGroup.group_id,
    limit: String(state.messageLimit),
    offset: String(state.messageOffset),
    direction: el.messageDirection.value,
    search: el.messageSearch.value.trim(),
  });

  const data = await api(`/api/messages?${query.toString()}`);
  const items = data.items || [];

  if (!items.length && state.messageOffset === 0) {
    renderEmpty(el.messageList, "这个群当前没有匹配的消息。");
  } else {
    const fragment = document.createElement("div");
    fragment.innerHTML = items.map(renderMessageItem).join("");
    for (const child of [...fragment.children]) {
      el.messageList.appendChild(child);
    }
    bindMessageItemClicks();
  }

  state.messageOffset += items.length;
  el.loadMoreMessages.classList.toggle("hidden", state.messageOffset >= (data.total || 0));
}

function renderMessageItem(item) {
  const senderLabel = item.sender_card || item.sender_name || item.sender_id || "unknown";
  const recalledBadge = item.is_recalled ? '<span class="badge danger">已撤回</span>' : "";
  const directionBadge =
    item.direction === "outgoing"
      ? '<span class="badge accent">出站</span>'
      : '<span class="badge">入站</span>';

  return `
    <button class="list-item message-item" data-message-id="${item.id}">
      <div class="row-between">
        <span class="item-title">${escapeHtml(senderLabel)}</span>
        <span class="item-meta">${formatTime(item.event_time)}</span>
      </div>
      <div class="row-wrap">
        ${directionBadge}
        ${recalledBadge}
        <span class="badge subtle">${escapeHtml(item.post_type || "message")}</span>
        <span class="badge subtle">段 ${formatCount(item.segment_count)}</span>
        ${
          item.forward_node_count
            ? `<span class="badge subtle">转发节点 ${formatCount(item.forward_node_count)}</span>`
            : ""
        }
      </div>
      <p class="item-body">${escapeHtml(item.outline || item.plain_text || "[空消息]")}</p>
    </button>
  `;
}

function bindMessageItemClicks() {
  for (const node of el.messageList.querySelectorAll(".message-item")) {
    node.addEventListener("click", async () => {
      const detail = await api(`/api/messages/${node.dataset.messageId}`);
      state.selectedRecord = { kind: "message", id: node.dataset.messageId, detail };
      renderMessageDetail(detail);
      if (isMobileLayout()) {
        setMobilePane("detail", "smooth");
      }
    });
  }
}

async function loadNotices(reset = false) {
  if (!state.selectedGroup) {
    renderEmpty(el.noticeList, "先选择一个群聊。");
    el.loadMoreNotices.classList.add("hidden");
    return;
  }

  if (reset) {
    state.noticeOffset = 0;
    el.noticeList.innerHTML = "";
  }

  const query = new URLSearchParams({
    platform_id: state.selectedGroup.platform_id,
    group_id: state.selectedGroup.group_id,
    limit: String(state.noticeLimit),
    offset: String(state.noticeOffset),
    notice_type: el.noticeType.value,
  });

  const data = await api(`/api/notices?${query.toString()}`);
  const items = data.items || [];

  if (!items.length && state.noticeOffset === 0) {
    renderEmpty(el.noticeList, "这个群当前没有匹配的通知事件。");
  } else {
    const fragment = document.createElement("div");
    fragment.innerHTML = items.map(renderNoticeItem).join("");
    for (const child of [...fragment.children]) {
      el.noticeList.appendChild(child);
    }
    bindNoticeItemClicks();
  }

  state.noticeOffset += items.length;
  el.loadMoreNotices.classList.toggle("hidden", state.noticeOffset >= (data.total || 0));
}

function renderNoticeItem(item) {
  const actor = item.actor_user_id || item.operator_id || item.target_id || "unknown";
  return `
    <button class="list-item notice-item" data-notice-id="${item.id}">
      <div class="row-between">
        <span class="item-title">${escapeHtml(item.notice_type)}</span>
        <span class="item-meta">${formatTime(item.event_time)}</span>
      </div>
      <div class="row-wrap">
        ${item.sub_type ? `<span class="badge subtle">${escapeHtml(item.sub_type)}</span>` : ""}
        ${item.message_id ? `<span class="badge subtle">消息 ${escapeHtml(item.message_id)}</span>` : ""}
      </div>
      <p class="item-body">actor/operator: ${escapeHtml(actor)}</p>
    </button>
  `;
}

function bindNoticeItemClicks() {
  for (const node of el.noticeList.querySelectorAll(".notice-item")) {
    node.addEventListener("click", async () => {
      const detail = await api(`/api/notices/${node.dataset.noticeId}`);
      state.selectedRecord = { kind: "notice", id: node.dataset.noticeId, detail };
      renderNoticeDetail(detail);
      if (isMobileLayout()) {
        setMobilePane("detail", "smooth");
      }
    });
  }
}

async function loadProfiles(reset = false) {
  if (!state.selectedGroup) {
    renderEmpty(el.profileGroupSummary, "先选择一个群聊。");
    renderEmpty(el.profileUserList, "先选择一个群聊。");
    el.loadMoreProfiles.classList.add("hidden");
    return;
  }

  if (reset) {
    state.profileOffset = 0;
    el.profileUserList.innerHTML = "";
  }

  await loadGroupProfileSummary();

  const query = new URLSearchParams({
    platform_id: state.selectedGroup.platform_id,
    group_id: state.selectedGroup.group_id,
    limit: String(state.profileLimit),
    offset: String(state.profileOffset),
    search: el.profileSearch.value.trim(),
  });
  const data = await api(`/api/profiles/users?${query.toString()}`);
  const items = data.items || [];

  if (!items.length && state.profileOffset === 0) {
    renderEmpty(el.profileUserList, "这个群当前还没有可用的成员画像。");
  } else {
    const fragment = document.createElement("div");
    fragment.innerHTML = items.map(renderProfileUserItem).join("");
    for (const child of [...fragment.children]) {
      el.profileUserList.appendChild(child);
    }
    bindProfileUserClicks();
  }

  state.profileOffset += items.length;
  el.loadMoreProfiles.classList.toggle("hidden", state.profileOffset >= (data.total || 0));
}

async function loadGroupProfileSummary() {
  const query = new URLSearchParams({
    platform_id: state.selectedGroup.platform_id,
    group_id: state.selectedGroup.group_id,
  });
  const data = await api(`/api/profiles/group?${query.toString()}`);
  renderGroupProfileSummary(data);
}

function renderGroupProfileSummary(data) {
  const summary = data.summary || {};
  const totalMessages =
    (summary.incoming_message_count || 0) + (summary.outgoing_message_count || 0);
  const interactions = data.top_interactions || [];

  if (!summary.tracked_users) {
    renderEmpty(el.profileGroupSummary, "这个群当前还没有画像汇总。");
    return;
  }

  el.profileGroupSummary.innerHTML = `
    <article class="detail-card">
      <div class="section-head">
        <h3>群画像概览</h3>
        <span class="item-meta">最近 ${formatTime(summary.last_seen_at)}</span>
      </div>
      <div class="kv-grid compact">
        <div><span>成员数</span><strong>${formatCount(summary.tracked_users)}</strong></div>
        <div><span>消息数</span><strong>${formatCount(totalMessages)}</strong></div>
        <div><span>文本字数</span><strong>${formatCount(summary.total_text_chars)}</strong></div>
        <div><span>图片</span><strong>${formatCount(summary.image_count)}</strong></div>
        <div><span>回复</span><strong>${formatCount(summary.reply_count)}</strong></div>
        <div><span>@</span><strong>${formatCount(summary.at_count)}</strong></div>
        <div><span>撤回动作</span><strong>${formatCount(summary.recall_action_count)}</strong></div>
        <div><span>点表情回应</span><strong>${formatCount(summary.emoji_notice_count)}</strong></div>
      </div>
      ${
        interactions.length
          ? `
            <div class="detail-section tight">
              <div class="section-head">
                <h3>高频互动</h3>
              </div>
              <div class="simple-list">
                ${interactions
                  .map(
                    (item) => `
                      <div class="simple-list-item">
                        <strong>${escapeHtml(item.source_label)}</strong>
                        <span class="item-meta">→</span>
                        <strong>${escapeHtml(item.target_label)}</strong>
                        <span class="badge subtle">${escapeHtml(item.interaction_type)}</span>
                        <span class="item-meta">${formatCount(item.interaction_count)}</span>
                      </div>
                    `,
                  )
                  .join("")}
              </div>
            </div>
          `
          : ""
      }
    </article>
  `;
}

function renderProfileUserItem(item) {
  const label = item.last_sender_card || item.last_sender_name || item.user_id;
  return `
    <button class="list-item profile-user-item" data-user-id="${escapeHtml(item.user_id)}">
      <div class="row-between">
        <span class="item-title">${escapeHtml(label)}</span>
        <span class="item-meta">${formatCount(item.total_message_count)} 条</span>
      </div>
      <div class="row-wrap">
        <span class="badge subtle">字数 ${formatCount(item.total_text_chars)}</span>
        <span class="badge subtle">图 ${formatCount(item.image_count)}</span>
        <span class="badge subtle">回复 ${formatCount(item.reply_count)}</span>
        <span class="badge subtle">@ ${formatCount(item.at_count)}</span>
      </div>
      <p class="item-body">最近活跃 ${formatTime(item.last_seen_at)}</p>
    </button>
  `;
}

function bindProfileUserClicks() {
  for (const node of el.profileUserList.querySelectorAll(".profile-user-item")) {
    node.addEventListener("click", async () => {
      const query = new URLSearchParams({
        platform_id: state.selectedGroup.platform_id,
        group_id: state.selectedGroup.group_id,
      });
      const detail = await api(
        `/api/profiles/users/${encodeURIComponent(node.dataset.userId)}?${query.toString()}`,
      );
      state.selectedRecord = { kind: "profile", id: node.dataset.userId, detail };
      renderProfileDetail(detail);
      if (isMobileLayout()) {
        setMobilePane("detail", "smooth");
      }
    });
  }
}

function renderSegments(segments) {
  if (!segments?.length) {
    return '<div class="empty-state"><p>没有消息段记录。</p></div>';
  }

  return segments
    .map((segment) => {
      const badges = [`<span class="badge subtle">${escapeHtml(segment.seg_type)}</span>`];
      if (segment.raw_type && segment.raw_type !== segment.seg_type) {
        badges.push(`<span class="badge subtle">raw:${escapeHtml(segment.raw_type)}</span>`);
      }
      if (segment.media_status) {
        badges.push(`<span class="badge subtle">${escapeHtml(segment.media_status)}</span>`);
      }

      let preview = "";
      if (segment.local_path) {
        const localUrl = mediaUrl(segment.local_path);
        if (segment.seg_type === "image") {
          preview = `<a href="${localUrl}" target="_blank" rel="noreferrer"><img class="segment-image" src="${localUrl}" alt="image" /></a>`;
        } else {
          preview = `<a class="file-link" href="${localUrl}" target="_blank" rel="noreferrer">打开本地附件</a>`;
        }
      } else if (segment.source_url) {
        preview = `<a class="file-link" href="${escapeHtml(segment.source_url)}" target="_blank" rel="noreferrer">打开源地址</a>`;
      }

      return `
        <article class="detail-card">
          <div class="row-between">
            <strong>#${segment.seg_index}</strong>
            <div class="row-wrap">${badges.join("")}</div>
          </div>
          ${segment.seg_text ? `<p>${escapeHtml(segment.seg_text)}</p>` : ""}
          ${preview}
          <pre>${formatJson(segment.seg_data)}</pre>
        </article>
      `;
    })
    .join("");
}

function renderForwardNodes(nodes) {
  if (!nodes?.length) {
    return "";
  }

  return `
    <section class="detail-section">
      <div class="section-head">
        <h3>转发节点</h3>
      </div>
      ${nodes
        .map(
          (node) => `
            <article class="detail-card">
              <div class="row-between">
                <strong>#${node.node_index}</strong>
                <span class="item-meta">${formatTime(node.sent_time)}</span>
              </div>
              <p>${escapeHtml(node.sender_name || node.sender_id || "unknown")}</p>
              <p>${escapeHtml(node.content_text || "")}</p>
              <pre>${formatJson(node.content)}</pre>
            </article>
          `,
        )
        .join("")}
    </section>
  `;
}

function renderMessageDetail(detail) {
  el.detailCaption.textContent = `消息 #${detail.id}`;
  el.detailView.innerHTML = `
    <section class="detail-section">
      <div class="section-head">
        <h3>消息元数据</h3>
      </div>
      <article class="detail-card">
        <div class="kv-grid">
          <div><span>platform</span><strong>${escapeHtml(detail.platform_id)}</strong></div>
          <div><span>group</span><strong>${escapeHtml(detail.group_id)}</strong></div>
          <div><span>sender</span><strong>${escapeHtml(detail.sender_card || detail.sender_name || detail.sender_id || "-")}</strong></div>
          <div><span>direction</span><strong>${escapeHtml(detail.direction)}</strong></div>
          <div><span>message_id</span><strong>${escapeHtml(detail.message_id || "-")}</strong></div>
          <div><span>event_time</span><strong>${escapeHtml(formatTime(detail.event_time))}</strong></div>
          <div><span>recalled</span><strong>${detail.is_recalled ? "yes" : "no"}</strong></div>
          <div><span>outline</span><strong>${escapeHtml(detail.outline || "-")}</strong></div>
        </div>
      </article>
    </section>
    <section class="detail-section">
      <div class="section-head">
        <h3>消息段</h3>
      </div>
      ${renderSegments(detail.segments)}
    </section>
    ${renderForwardNodes(detail.forward_nodes)}
    <section class="detail-section">
      <div class="section-head">
        <h3>原始事件</h3>
      </div>
      <article class="detail-card">
        <pre>${formatJson(detail.raw_event)}</pre>
      </article>
    </section>
  `;
}

function renderNoticeDetail(detail) {
  el.detailCaption.textContent = `通知 #${detail.id}`;
  el.detailView.innerHTML = `
    <section class="detail-section">
      <div class="section-head">
        <h3>通知元数据</h3>
      </div>
      <article class="detail-card">
        <div class="kv-grid">
          <div><span>notice_type</span><strong>${escapeHtml(detail.notice_type)}</strong></div>
          <div><span>sub_type</span><strong>${escapeHtml(detail.sub_type || "-")}</strong></div>
          <div><span>group</span><strong>${escapeHtml(detail.group_id)}</strong></div>
          <div><span>message_id</span><strong>${escapeHtml(detail.message_id || "-")}</strong></div>
          <div><span>actor</span><strong>${escapeHtml(detail.actor_user_id || "-")}</strong></div>
          <div><span>operator</span><strong>${escapeHtml(detail.operator_id || "-")}</strong></div>
          <div><span>target</span><strong>${escapeHtml(detail.target_id || "-")}</strong></div>
          <div><span>event_time</span><strong>${escapeHtml(formatTime(detail.event_time))}</strong></div>
        </div>
      </article>
    </section>
    <section class="detail-section">
      <div class="section-head">
        <h3>原始事件</h3>
      </div>
      <article class="detail-card">
        <pre>${formatJson(detail.raw_event)}</pre>
      </article>
    </section>
  `;
}

function renderProfileInteractions(title, rows, labelKey) {
  return `
    <section class="detail-section">
      <div class="section-head">
        <h3>${title}</h3>
      </div>
      ${
        rows?.length
          ? `
            <div class="simple-list">
              ${rows
                .map(
                  (item) => `
                    <div class="simple-list-item">
                      <strong>${escapeHtml(item[labelKey])}</strong>
                      <span class="badge subtle">${escapeHtml(item.interaction_type)}</span>
                      <span class="item-meta">${formatCount(item.interaction_count)}</span>
                      <span class="item-meta">最近 ${formatTime(item.last_seen_at)}</span>
                    </div>
                  `,
                )
                .join("")}
            </div>
          `
          : '<div class="empty-state"><p>暂无互动记录。</p></div>'
      }
    </section>
  `;
}

function renderProfileDailyStats(rows) {
  if (!rows?.length) {
    return '<div class="empty-state"><p>暂无按日统计。</p></div>';
  }

  return `
    <div class="simple-list">
      ${rows
        .map(
          (item) => `
            <div class="simple-list-item multi-line">
              <strong>${escapeHtml(item.stat_date)}</strong>
              <span class="item-meta">
                消息 ${formatCount((item.incoming_message_count || 0) + (item.outgoing_message_count || 0))}
                · 字数 ${formatCount(item.total_text_chars)}
                · 图 ${formatCount(item.image_count)}
                · 回复 ${formatCount(item.reply_count)}
                · @ ${formatCount(item.at_count)}
              </span>
            </div>
          `,
        )
        .join("")}
    </div>
  `;
}

function renderProfileDetail(detail) {
  const summary = detail.summary || {};
  const globalSummary = detail.global_summary || {};
  const title = summary.last_sender_card || summary.last_sender_name || summary.user_id;
  const groupTotal =
    (summary.incoming_message_count || 0) + (summary.outgoing_message_count || 0);
  const globalTotal =
    (globalSummary.incoming_message_count || 0) + (globalSummary.outgoing_message_count || 0);

  el.detailCaption.textContent = `画像: ${title}`;
  el.detailView.innerHTML = `
    <section class="detail-section">
      <div class="section-head">
        <h3>群内画像</h3>
      </div>
      <article class="detail-card">
        <div class="kv-grid">
          <div><span>user_id</span><strong>${escapeHtml(summary.user_id || "-")}</strong></div>
          <div><span>display</span><strong>${escapeHtml(title || "-")}</strong></div>
          <div><span>群内消息数</span><strong>${formatCount(groupTotal)}</strong></div>
          <div><span>文本字数</span><strong>${formatCount(summary.total_text_chars)}</strong></div>
          <div><span>图片</span><strong>${formatCount(summary.image_count)}</strong></div>
          <div><span>文件</span><strong>${formatCount(summary.file_count)}</strong></div>
          <div><span>回复</span><strong>${formatCount(summary.reply_count)}</strong></div>
          <div><span>@</span><strong>${formatCount(summary.at_count)}</strong></div>
          <div><span>撤回动作</span><strong>${formatCount(summary.recall_action_count)}</strong></div>
          <div><span>被撤回消息</span><strong>${formatCount(summary.recalled_message_count)}</strong></div>
          <div><span>点表情回应</span><strong>${formatCount(summary.emoji_notice_count)}</strong></div>
          <div><span>最近活跃</span><strong>${formatTime(summary.last_seen_at)}</strong></div>
        </div>
      </article>
    </section>
    ${
      globalSummary.user_id
        ? `
          <section class="detail-section">
            <div class="section-head">
              <h3>跨群汇总</h3>
            </div>
            <article class="detail-card">
              <div class="kv-grid compact">
                <div><span>总消息数</span><strong>${formatCount(globalTotal)}</strong></div>
                <div><span>总文本字数</span><strong>${formatCount(globalSummary.total_text_chars)}</strong></div>
                <div><span>最近群</span><strong>${escapeHtml(globalSummary.last_group_name || globalSummary.last_group_id || "-")}</strong></div>
                <div><span>最后活跃</span><strong>${formatTime(globalSummary.last_seen_at)}</strong></div>
              </div>
            </article>
          </section>
        `
        : ""
    }
    <section class="detail-section">
      <div class="section-head">
        <h3>近 30 天</h3>
      </div>
      ${renderProfileDailyStats(detail.daily_stats)}
    </section>
    ${renderProfileInteractions("发起互动", detail.outgoing_interactions, "target_label")}
    ${renderProfileInteractions("收到互动", detail.incoming_interactions, "source_label")}
  `;
}

function switchTab(nextTab) {
  state.activeTab = nextTab;
  for (const node of el.tabs) {
    node.classList.toggle("is-active", node.dataset.tab === nextTab);
  }

  const isMessages = nextTab === "messages";
  const isNotices = nextTab === "notices";
  const isProfiles = nextTab === "profiles";

  el.messagesView.classList.toggle("hidden", !isMessages);
  el.noticesView.classList.toggle("hidden", !isNotices);
  el.profilesView.classList.toggle("hidden", !isProfiles);
  el.messagesToolbar.classList.toggle("hidden", !isMessages);
  el.noticesToolbar.classList.toggle("hidden", !isNotices);
  el.profilesToolbar.classList.toggle("hidden", !isProfiles);
}

function bindEvents() {
  el.refreshOverview.addEventListener("click", () => void loadOverview().catch(reportError));
  el.refreshGroups.addEventListener("click", () => void loadGroups().catch(reportError));
  el.refreshActive.addEventListener("click", async () => {
    try {
      await loadOverview();
      await Promise.all([loadMessages(true), loadNotices(true), loadProfiles(true)]);
    } catch (error) {
      reportError(error);
    }
  });

  el.groupSearch.addEventListener("change", () => void loadGroups().catch(reportError));
  el.messageDirection.addEventListener("change", () => void loadMessages(true).catch(reportError));
  el.messageSearch.addEventListener("change", () => void loadMessages(true).catch(reportError));
  el.noticeType.addEventListener("change", () => void loadNotices(true).catch(reportError));
  el.profileSearch.addEventListener("change", () => void loadProfiles(true).catch(reportError));
  el.loadMoreMessages.addEventListener("click", () => void loadMessages(false).catch(reportError));
  el.loadMoreNotices.addEventListener("click", () => void loadNotices(false).catch(reportError));
  el.loadMoreProfiles.addEventListener("click", () => void loadProfiles(false).catch(reportError));
  el.mobileShowGroups.addEventListener("click", () => setMobilePane("groups", "smooth"));
  el.mobileBackToTimeline.addEventListener("click", () => setMobilePane("timeline", "smooth"));
  el.authButton.addEventListener("click", showAuthDialog);

  el.saveAuthToken.addEventListener("click", async (event) => {
    event.preventDefault();
    saveAuthToken();
    el.authDialog.close();
    try {
      await bootstrap();
    } catch (error) {
      reportError(error);
    }
  });

  for (const node of el.tabs) {
    node.addEventListener("click", () => switchTab(node.dataset.tab));
  }

  el.workspace.addEventListener("scroll", syncMobilePaneFromScroll, { passive: true });
  window.addEventListener("resize", () => applyMobilePane("auto"));
}

function reportError(error) {
  console.error(error);
  const text = error instanceof Error ? error.message : String(error);
  el.detailCaption.textContent = "请求失败";
  el.detailView.innerHTML = `
    <div class="empty-state">
      <p>${escapeHtml(text)}</p>
    </div>
  `;
}

async function bootstrap() {
  await Promise.all([loadOverview(), loadGroups()]);
  if (state.selectedGroup) {
    await Promise.all([loadMessages(true), loadNotices(true), loadProfiles(true)]);
    setMobilePane("timeline", "auto");
  } else {
    renderEmpty(el.messageList, "先选择一个群聊。");
    renderEmpty(el.noticeList, "先选择一个群聊。");
    renderEmpty(el.profileGroupSummary, "先选择一个群聊。");
    renderEmpty(el.profileUserList, "先选择一个群聊。");
    setMobilePane("groups", "auto");
  }
}

bindEvents();
switchTab("messages");
applyMobilePane("auto");
bootstrap().catch(reportError);

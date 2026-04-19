export type DetailKind = "message" | "notice" | "profile";
export type ActiveTab = "messages" | "notices" | "profiles";
export type MobilePane = "groups" | "timeline" | "detail";

export interface OverviewPayload {
  total_groups: number;
  incoming_messages: number;
  outgoing_messages: number;
  recalled_messages: number;
  notice_events: number;
  emoji_reactions: number;
  forward_nodes: number;
  profile_users: number;
  profile_group_users: number;
  interaction_edges: number;
  profile_claims: number;
  profile_attributes: number;
  profile_jobs_completed: number;
  last_event_time: number | null;
  last_profile_update_time: number | null;
  db_path: string;
}

export interface ProfilePipelineStatusPayload {
  runtime: {
    enabled: boolean;
    mode: string;
    langgraph_available: boolean;
    supported: boolean;
    runner_running: boolean;
    llm_mode: string;
    poll_interval_sec: number;
    batch_message_limit: number;
    min_batch_messages: number;
    batch_overlap: number;
    max_jobs_per_tick: number;
    llm_timeout_sec: number;
    running_job_timeout_sec: number;
  } | null;
  storage: {
    archived_incoming_messages: number;
    archived_groups: number;
    total_blocks: number;
    total_jobs: number;
    total_claims: number;
    total_attributes: number;
    block_statuses: Array<{ status: string; count: number }>;
    job_statuses: Array<{ status: string; count: number }>;
    latest_jobs: Array<{
      id: number;
      block_id: number;
      status: string;
      attempt_count: number;
      scheduled_at: number;
      started_at: number | null;
      finished_at: number | null;
      updated_at: number;
      last_error: string;
      workflow_state_json: string;
      workflow_state: Record<string, unknown> | null;
      result_summary_json: string;
      result_summary: Record<string, unknown> | null;
      block_key: string;
      platform_id: string;
      group_id: string;
      group_name: string;
      message_count: number;
      approx_text_chars: number;
      first_event_at: number;
      last_event_at: number;
    }>;
    latest_claims: Array<{
      id: number;
      platform_id: string;
      group_id: string;
      group_name: string;
      subject_user_id: string;
      attribute_type: string;
      attribute_label: string;
      normalized_value: string;
      source_kind: string;
      confidence: number;
      status: string;
      updated_at: number;
    }>;
    cursors: Array<{
      state_key: string;
      state_value: string;
      updated_at: number;
    }>;
    last_message: {
      id: number;
      platform_id: string;
      group_id: string;
      event_time: number;
    } | null;
  };
}

export interface PagedResponse<T> {
  items: T[];
  total: number;
}

export interface GroupRow {
  platform_id: string;
  group_id: string;
  group_name: string;
  unified_origin: string;
  last_message_time: number;
  last_notice_time: number;
  incoming_message_count: number;
  outgoing_message_count: number;
  notice_count: number;
}

export interface MessageListItem {
  id: number;
  platform_id: string;
  group_id: string;
  group_name: string;
  sender_id: string;
  sender_name: string;
  sender_card: string;
  direction: string;
  outline: string;
  plain_text: string;
  event_time: number;
  is_recalled: number;
  message_id: string | null;
}

export interface NoticeListItem {
  id: number;
  platform_id: string;
  group_id: string;
  notice_type: string;
  sub_type: string;
  actor_user_id: string;
  operator_id: string;
  target_user_id: string;
  message_id: string | null;
  reaction_code: string;
  reaction_count: number;
  event_time: number;
}

export interface ArchivedSegment {
  id: number;
  seg_index: number;
  seg_type: string;
  raw_type: string;
  text_content: string;
  seg_data: Record<string, unknown> | null;
  source_url: string;
  local_path: string;
  media_status: string;
  file_size: number | null;
  mime_type: string;
  sha256: string;
}

export interface ForwardNodeRow {
  id: number;
  node_index: number;
  sender_id: string;
  sender_name: string;
  time: number;
  content: unknown;
}

export interface MessageDetail extends MessageListItem {
  bot_self_id: string;
  session_id: string;
  sender_card: string;
  post_type: string;
  message_sub_type: string;
  archived_at: number;
  raw_event: Record<string, unknown> | null;
  segments: ArchivedSegment[];
  forward_nodes: ForwardNodeRow[];
}

export interface NoticeDetail extends NoticeListItem {
  group_name: string;
  target_message_row_id: number | null;
  raw_event: Record<string, unknown> | null;
}

export interface GroupProfileSummary {
  summary: {
    tracked_users?: number;
    group_name?: string;
    incoming_message_count?: number;
    outgoing_message_count?: number;
    text_message_count?: number;
    total_text_chars?: number;
    image_count?: number;
    record_count?: number;
    video_count?: number;
    file_count?: number;
    forward_count?: number;
    reply_count?: number;
    at_count?: number;
    raw_segment_count?: number;
    media_message_count?: number;
    recall_action_count?: number;
    recalled_message_count?: number;
    emoji_notice_count?: number;
    last_seen_at?: number;
  };
  daily_stats: Array<Record<string, number | string>>;
  top_interactions: Array<Record<string, number | string>>;
  top_attributes: Array<{
    attribute_type: string;
    attribute_label: string;
    user_count: number;
    avg_confidence: number;
    last_updated_at: number;
  }>;
  claim_statuses: Array<{
    attribute_type: string;
    attribute_label: string;
    status: string;
    claim_count: number;
    last_updated_at: number;
  }>;
}

export interface GroupProfileUser {
  platform_id: string;
  group_id: string;
  group_name: string;
  user_id: string;
  last_sender_name: string;
  last_sender_card: string;
  first_seen_at: number;
  last_seen_at: number;
  incoming_message_count: number;
  outgoing_message_count: number;
  text_message_count: number;
  total_text_chars: number;
  image_count: number;
  record_count: number;
  video_count: number;
  file_count: number;
  forward_count: number;
  reply_count: number;
  at_count: number;
  raw_segment_count: number;
  media_message_count: number;
  recall_action_count: number;
  recalled_message_count: number;
  emoji_notice_count: number;
  total_message_count: number;
}

export interface ProfileAttribute {
  platform_id: string;
  group_id: string;
  group_name: string;
  subject_user_id: string;
  attribute_type: string;
  attribute_label: string;
  current_claim_id: number | null;
  current_value: string;
  normalized_value: string;
  confidence: number;
  source_kind: string;
  first_seen_at: number;
  last_seen_at: number;
  evidence_count: number;
  updated_at: number;
  status: string;
  current_claim: {
    id: number;
    raw_value: string;
    normalized_value: string;
    source_kind: string;
    tense: string;
    polarity: string;
    confidence: number;
    status: string;
    resolver_note: string;
    first_seen_at: number;
    last_seen_at: number;
    updated_at: number;
    payload: Record<string, unknown> | null;
  } | null;
}

export interface ClaimEvidence {
  claim_id: number;
  message_row_id: number;
  excerpt: string;
  evidence_kind: string;
  created_at: number;
  event_time: number;
  sender_id: string;
  sender_name: string;
  sender_card: string;
  sender_label: string;
  direction: string;
  plain_text: string;
  outline: string;
}

export interface ProfileClaim {
  id: number;
  platform_id: string;
  group_id: string;
  group_name: string;
  subject_user_id: string;
  source_message_row_id: number | null;
  attribute_type: string;
  attribute_label: string;
  raw_value: string;
  normalized_value: string;
  source_kind: string;
  tense: string;
  polarity: string;
  confidence: number;
  status: string;
  resolver_note: string;
  first_seen_at: number;
  last_seen_at: number;
  created_at: number;
  updated_at: number;
  payload: Record<string, unknown> | null;
  evidence_count: number;
  evidence: ClaimEvidence[];
}

export interface AttributeHistoryRow {
  id: number;
  platform_id: string;
  group_id: string;
  subject_user_id: string;
  attribute_type: string;
  attribute_label: string;
  claim_id: number | null;
  previous_claim_id: number | null;
  action: string;
  created_at: number;
  payload: Record<string, unknown> | null;
  claim_normalized_value: string;
  claim_status: string;
  previous_claim_normalized_value: string;
  previous_claim_status: string;
}

export interface UserProfileDetail {
  summary: GroupProfileUser;
  global_summary: (GroupProfileUser & { total_message_count: number }) | null;
  daily_stats: Array<Record<string, number | string>>;
  outgoing_interactions: Array<Record<string, number | string>>;
  incoming_interactions: Array<Record<string, number | string>>;
  attributes: ProfileAttribute[];
  claims: ProfileClaim[];
  attribute_history: AttributeHistoryRow[];
}

export interface SelectedDetail {
  kind: DetailKind;
  title: string;
  messageId?: number;
  noticeId?: number;
  userId?: string;
}

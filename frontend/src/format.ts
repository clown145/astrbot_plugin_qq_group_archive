export function formatTime(value: number | null | undefined) {
  if (!value) {
    return "-";
  }
  return new Date(value * 1000).toLocaleString("zh-CN", { hour12: false });
}

export function formatCompactNumber(value: number | null | undefined) {
  return new Intl.NumberFormat("zh-CN").format(value ?? 0);
}

export function formatDuration(seconds: number | null | undefined) {
  const total = Math.max(Math.floor(seconds ?? 0), 0);
  const minutes = Math.floor(total / 60);
  const remainingSeconds = total % 60;
  if (minutes <= 0) {
    return `${remainingSeconds} 秒`;
  }
  if (minutes < 60) {
    return `${minutes} 分 ${remainingSeconds} 秒`;
  }
  const hours = Math.floor(minutes / 60);
  return `${hours} 小时 ${minutes % 60} 分`;
}

export function formatPercent(value: number | null | undefined) {
  return `${Math.round((value ?? 0) * 100)}%`;
}

export function displayUserLabel(name?: string | null, card?: string | null, userId?: string | null) {
  return String(card || "").trim() || String(name || "").trim() || String(userId || "").trim() || "未知成员";
}

export function truncateText(value: string | null | undefined, maxLength = 180) {
  const text = String(value || "").trim();
  if (text.length <= maxLength) {
    return text || "-";
  }
  return `${text.slice(0, maxLength)}...`;
}

export function classNames(...values: Array<string | false | null | undefined>) {
  return values.filter(Boolean).join(" ");
}

export function formatAttributeLabel(value: string) {
  const key = String(value || "").trim();
  const mapping: Record<string, string> = {
    education_university: "学校",
    education_major: "专业",
    device_phone: "手机",
    device_computer_model: "电脑型号",
    device_tablet: "平板设备",
    device_peripheral: "外设",
    device_environment: "设备环境",
    os_preference: "系统偏好",
    ide_preference: "编辑器偏好",
    programming_language: "编程语言",
    programming_framework: "技术框架",
    programming_habit: "编程习惯",
    project_experience: "项目经历",
    appearance_hair: "发型",
    clothing_style: "穿衣风格",
    appearance_style: "外貌风格",
    schedule_status: "状态",
    course_or_class: "课程上课",
    location_hint: "地点",
    occupation_or_role: "身份",
    romantic_partner_status: "感情状态",
    relationship_status: "关系状态",
    advisor_or_supervisor: "导师/上级",
    mentor_or_teacher: "导师/老师",
    interest_preference: "兴趣偏好",
    food_preference: "饮食偏好",
    canteen_dining_habit: "食堂用餐习惯",
    behavioral_style: "行为风格",
    communication_response_style: "沟通回应风格",
    desired_ai_application: "想做的 AI 应用",
  };
  if (mapping[key]) {
    return mapping[key];
  }
  return humanizeOpenAttributeKey(key);
}

function humanizeOpenAttributeKey(value: string) {
  const key = value.trim();
  if (!key) {
    return "未知属性";
  }

  const phraseMap: Record<string, string> = {
    ai: "AI",
    api: "API",
    app: "应用",
    application: "应用",
    applications: "应用",
    advisor: "导师",
    supervisor: "上级",
    mentor: "导师",
    teacher: "老师",
    partner: "对象",
    romantic: "感情",
    relationship: "关系",
    clothing: "穿衣",
    clothes: "穿衣",
    fashion: "穿搭",
    style: "风格",
    appearance: "外貌",
    hair: "头发",
    device: "设备",
    computer: "电脑",
    laptop: "笔记本",
    model: "型号",
    phone: "手机",
    tablet: "平板",
    os: "系统",
    ide: "编辑器",
    programming: "编程",
    coding: "写代码",
    code: "代码",
    language: "语言",
    framework: "框架",
    habit: "习惯",
    habits: "习惯",
    preference: "偏好",
    preferences: "偏好",
    preferred: "偏好",
    project: "项目",
    experience: "经历",
    education: "教育",
    university: "大学",
    college: "学院",
    major: "专业",
    course: "课程",
    class: "上课",
    schedule: "日程",
    status: "状态",
    location: "地点",
    home: "家",
    dorm: "宿舍",
    canteen: "食堂",
    dining: "用餐",
    food: "饮食",
    behavior: "行为",
    behavioral: "行为",
    communication: "沟通",
    response: "回应",
    desired: "想要的",
    goal: "目标",
    plan: "计划",
    role: "身份",
    occupation: "职业",
    work: "工作",
    internship: "实习",
    personality: "性格",
    emotion: "情绪",
    emotional: "情绪",
    hobby: "爱好",
    interest: "兴趣",
  };

  const translated = key
    .split(/[_\-\s]+/)
    .filter(Boolean)
    .map((part) => phraseMap[part.toLowerCase()] || part)
    .join("");
  return translated || key;
}

export function formatSourceKind(value: string) {
  const mapping: Record<string, string> = {
    self_report: "自述",
    other_report: "他述",
    inferred: "推断",
    direct_observation: "直接观察",
    unknown: "未知来源",
  };
  return mapping[value] || value || "未知来源";
}

export function formatClaimStatus(value: string) {
  const mapping: Record<string, string> = {
    accepted: "已采纳",
    candidate: "候选",
    conflicted: "冲突",
    outdated: "过期",
    rejected: "已拒绝",
  };
  return mapping[value] || value || "未知状态";
}

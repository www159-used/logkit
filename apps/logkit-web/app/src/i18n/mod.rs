use leptos::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Locale {
    Zh,
    En,
}

impl Locale {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Zh => "zh",
            Self::En => "en",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "zh" | "zh-Hans" => Some(Self::Zh),
            "en" => Some(Self::En),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Msg {
    PageNotFound,
    AddConnection,
    LoadingConnections,
    NoConnections,
    Edit,
    Delete,
    PingFailed,
    Workers,
    Connections,
    Back,
    LoadingWorkers,
    NoWorkers,
    ColStatus,
    ColEvents,
    Stop,
    StatusStopped,
    StatusHealthy,
    StatusUnhealthy,
    StartWorker,
    NewWorker,
    FlowBody,
    FlowSink,
    FlowOptions,
    BodyPreset,
    SinkType,
    SinkStdout,
    SinkFile,
    SinkKafka,
    FileOutput,
    FileMaxSize,
    KafkaTopic,
    KafkaBrokers,
    Rate,
    ThreadsOptional,
    ScriptLabel,
    NewConnection,
    EditConnection,
    Name,
    Kind,
    LocalUnix,
    RemoteTcp,
    SocketOptional,
    Host,
    Port,
    Notes,
    Save,
    Cancel,
    ConnectionSaved,
    KindLocal,
    KindRemote,
    ThemeLight,
    ThemeDark,
    ThemeSystem,
    Settings,
    Appearance,
    Language,
    LocaleZh,
    LocaleEn,
    SinkFileRequiresMinLogend,
    LogendServerVersion,
    About,
    AboutVersion,
    AboutLicense,
    AboutDocs,
    AboutSource,
    AboutRuntime,
    AboutRuntimeDesktop,
    AboutRuntimeWeb,
    ViewWorkerStatus,
    LoadingWorker,
    WorkerEpsChart,
    StatEps,
    StatEpsInterval,
    StatEventsEst,
    StatRetry,
    StatHeartbeat,
    StatHeartbeatInterval,
    StatHeartbeatTimeout,
    RefreshInterval,
    RefreshSec1,
    RefreshSec2,
    RefreshSec5,
    RefreshSec10,
    RefreshSec30,
    KafkaMode,
    KafkaModeCommon,
    KafkaModeAgent,
    KafkaAgentFormat,
    KafkaAgentFormatJson,
    KafkaAgentFormatPb,
    KafkaAgentSourceId,
    KafkaAgentAppname,
    KafkaAgentTag,
    KafkaAgentDomain,
}

pub fn translate(locale: Locale, key: Msg) -> &'static str {
    match (locale, key) {
        (Locale::Zh, Msg::PageNotFound) => "未找到页面",
        (Locale::En, Msg::PageNotFound) => "Page not found",
        (Locale::Zh, Msg::AddConnection) => "添加连接",
        (Locale::En, Msg::AddConnection) => "Add connection",
        (Locale::Zh, Msg::LoadingConnections) => "加载连接…",
        (Locale::En, Msg::LoadingConnections) => "Loading connections…",
        (Locale::Zh, Msg::NoConnections) => "还没有连接。",
        (Locale::En, Msg::NoConnections) => "No connections yet.",
        (Locale::Zh, Msg::Edit) => "编辑",
        (Locale::En, Msg::Edit) => "Edit",
        (Locale::Zh, Msg::Delete) => "删除",
        (Locale::En, Msg::Delete) => "Delete",
        (Locale::Zh, Msg::PingFailed) => "Ping 失败",
        (Locale::En, Msg::PingFailed) => "Ping failed",
        (Locale::Zh, Msg::Workers) => "Workers",
        (Locale::En, Msg::Workers) => "Workers",
        (Locale::Zh, Msg::Connections) => "连接",
        (Locale::En, Msg::Connections) => "Connections",
        (Locale::Zh, Msg::Back) => "返回连接",
        (Locale::En, Msg::Back) => "Back to connections",
        (Locale::Zh, Msg::LoadingWorkers) => "加载 worker…",
        (Locale::En, Msg::LoadingWorkers) => "Loading workers…",
        (Locale::Zh, Msg::NoWorkers) => "当前 logend 上没有 worker。",
        (Locale::En, Msg::NoWorkers) => "No workers on this logend.",
        (Locale::Zh, Msg::StartWorker) => "启动 worker",
        (Locale::En, Msg::StartWorker) => "Start worker",
        (Locale::Zh, Msg::NewWorker) => "启动 worker",
        (Locale::En, Msg::NewWorker) => "Start worker",
        (Locale::Zh, Msg::FlowBody) => "Body",
        (Locale::En, Msg::FlowBody) => "Body",
        (Locale::Zh, Msg::FlowSink) => "Sink",
        (Locale::En, Msg::FlowSink) => "Sink",
        (Locale::Zh, Msg::FlowOptions) => "选项",
        (Locale::En, Msg::FlowOptions) => "Options",
        (Locale::Zh, Msg::BodyPreset) => "预设",
        (Locale::En, Msg::BodyPreset) => "Preset",
        (Locale::Zh, Msg::SinkType) => "类型",
        (Locale::En, Msg::SinkType) => "Type",
        (Locale::Zh, Msg::SinkStdout) => "stdout",
        (Locale::En, Msg::SinkStdout) => "stdout",
        (Locale::Zh, Msg::SinkFile) => "file",
        (Locale::En, Msg::SinkFile) => "file",
        (Locale::Zh, Msg::SinkKafka) => "Kafka",
        (Locale::En, Msg::SinkKafka) => "Kafka",
        (Locale::Zh, Msg::FileOutput) => "输出路径",
        (Locale::En, Msg::FileOutput) => "Output path",
        (Locale::Zh, Msg::FileMaxSize) => "单文件上限",
        (Locale::En, Msg::FileMaxSize) => "Max file size",
        (Locale::Zh, Msg::KafkaTopic) => "Topic",
        (Locale::En, Msg::KafkaTopic) => "Topic",
        (Locale::Zh, Msg::KafkaBrokers) => "Brokers",
        (Locale::En, Msg::KafkaBrokers) => "Brokers",
        (Locale::Zh, Msg::Rate) => "速率",
        (Locale::En, Msg::Rate) => "Rate",
        (Locale::Zh, Msg::ThreadsOptional) => "线程",
        (Locale::En, Msg::ThreadsOptional) => "Threads",
        (Locale::Zh, Msg::ScriptLabel) => "标签",
        (Locale::En, Msg::ScriptLabel) => "Label",
        (Locale::Zh, Msg::ColStatus) => "状态",
        (Locale::En, Msg::ColStatus) => "Status",
        (Locale::Zh, Msg::ColEvents) => "事件",
        (Locale::En, Msg::ColEvents) => "Events",
        (Locale::Zh, Msg::Stop) => "停止",
        (Locale::En, Msg::Stop) => "Stop",
        (Locale::Zh, Msg::StatusStopped) => "已停止",
        (Locale::En, Msg::StatusStopped) => "Stopped",
        (Locale::Zh, Msg::StatusHealthy) => "健康",
        (Locale::En, Msg::StatusHealthy) => "Healthy",
        (Locale::Zh, Msg::StatusUnhealthy) => "异常",
        (Locale::En, Msg::StatusUnhealthy) => "Unhealthy",
        (Locale::Zh, Msg::NewConnection) => "新连接",
        (Locale::En, Msg::NewConnection) => "New connection",
        (Locale::Zh, Msg::EditConnection) => "编辑连接",
        (Locale::En, Msg::EditConnection) => "Edit connection",
        (Locale::Zh, Msg::Name) => "名称",
        (Locale::En, Msg::Name) => "Name",
        (Locale::Zh, Msg::Kind) => "类型",
        (Locale::En, Msg::Kind) => "Type",
        (Locale::Zh, Msg::LocalUnix) => "本机 Unix",
        (Locale::En, Msg::LocalUnix) => "Local Unix",
        (Locale::Zh, Msg::RemoteTcp) => "远端 TCP",
        (Locale::En, Msg::RemoteTcp) => "Remote TCP",
        (Locale::Zh, Msg::SocketOptional) => "套接字",
        (Locale::En, Msg::SocketOptional) => "Socket",
        (Locale::Zh, Msg::Host) => "主机",
        (Locale::En, Msg::Host) => "Host",
        (Locale::Zh, Msg::Port) => "端口",
        (Locale::En, Msg::Port) => "Port",
        (Locale::Zh, Msg::Notes) => "备注",
        (Locale::En, Msg::Notes) => "Notes",
        (Locale::Zh, Msg::Save) => "保存",
        (Locale::En, Msg::Save) => "Save",
        (Locale::Zh, Msg::Cancel) => "取消",
        (Locale::En, Msg::Cancel) => "Cancel",
        (Locale::Zh, Msg::ConnectionSaved) => "连接已保存",
        (Locale::En, Msg::ConnectionSaved) => "Connection saved",
        (Locale::Zh, Msg::KindLocal) => "本机",
        (Locale::En, Msg::KindLocal) => "Local",
        (Locale::Zh, Msg::KindRemote) => "远端",
        (Locale::En, Msg::KindRemote) => "Remote",
        (Locale::Zh, Msg::ThemeLight) => "浅色",
        (Locale::En, Msg::ThemeLight) => "Light",
        (Locale::Zh, Msg::ThemeDark) => "深色",
        (Locale::En, Msg::ThemeDark) => "Dark",
        (Locale::Zh, Msg::ThemeSystem) => "跟随系统",
        (Locale::En, Msg::ThemeSystem) => "System",
        (Locale::Zh, Msg::Settings) => "设置",
        (Locale::En, Msg::Settings) => "Settings",
        (Locale::Zh, Msg::Appearance) => "外观",
        (Locale::En, Msg::Appearance) => "Appearance",
        (Locale::Zh, Msg::Language) => "语言",
        (Locale::En, Msg::Language) => "Language",
        (Locale::Zh, Msg::LocaleZh) => "简体中文",
        (Locale::En, Msg::LocaleZh) => "简体中文",
        (Locale::Zh, Msg::LocaleEn) => "English",
        (Locale::En, Msg::LocaleEn) => "English",
        (Locale::Zh, Msg::SinkFileRequiresMinLogend) => {
            "当前 logend 过旧，不支持文件 sink。请升级 logend 至 2.1.0 及以上后刷新。"
        }
        (Locale::En, Msg::SinkFileRequiresMinLogend) => {
            "File sink requires logend 2.1.0+. Upgrade logend and refresh."
        }
        (Locale::Zh, Msg::LogendServerVersion) => "服务端",
        (Locale::En, Msg::LogendServerVersion) => "Server",
        (Locale::Zh, Msg::About) => "关于",
        (Locale::En, Msg::About) => "About",
        (Locale::Zh, Msg::AboutVersion) => "版本",
        (Locale::En, Msg::AboutVersion) => "Version",
        (Locale::Zh, Msg::AboutLicense) => "许可",
        (Locale::En, Msg::AboutLicense) => "License",
        (Locale::Zh, Msg::AboutDocs) => "文档",
        (Locale::En, Msg::AboutDocs) => "Documentation",
        (Locale::Zh, Msg::AboutSource) => "源代码",
        (Locale::En, Msg::AboutSource) => "Source code",
        (Locale::Zh, Msg::AboutRuntime) => "运行环境",
        (Locale::En, Msg::AboutRuntime) => "Runtime",
        (Locale::Zh, Msg::AboutRuntimeDesktop) => "桌面应用",
        (Locale::En, Msg::AboutRuntimeDesktop) => "Desktop app",
        (Locale::Zh, Msg::AboutRuntimeWeb) => "浏览器",
        (Locale::En, Msg::AboutRuntimeWeb) => "Web browser",
        (Locale::Zh, Msg::ViewWorkerStatus) => "查看状态",
        (Locale::En, Msg::ViewWorkerStatus) => "View status",
        (Locale::Zh, Msg::LoadingWorker) => "加载 worker…",
        (Locale::En, Msg::LoadingWorker) => "Loading worker…",
        (Locale::Zh, Msg::WorkerEpsChart) => "EPS 趋势",
        (Locale::En, Msg::WorkerEpsChart) => "EPS trend",
        (Locale::Zh, Msg::StatEps) => "EPS",
        (Locale::En, Msg::StatEps) => "EPS",
        (Locale::Zh, Msg::StatEpsInterval) => "EPS 间隔",
        (Locale::En, Msg::StatEpsInterval) => "EPS interval",
        (Locale::Zh, Msg::StatEventsEst) => "事件估算",
        (Locale::En, Msg::StatEventsEst) => "Events est.",
        (Locale::Zh, Msg::StatRetry) => "重试",
        (Locale::En, Msg::StatRetry) => "Retries",
        (Locale::Zh, Msg::StatHeartbeat) => "距上次心跳",
        (Locale::En, Msg::StatHeartbeat) => "Since heartbeat",
        (Locale::Zh, Msg::StatHeartbeatInterval) => "心跳间隔",
        (Locale::En, Msg::StatHeartbeatInterval) => "Heartbeat interval",
        (Locale::Zh, Msg::StatHeartbeatTimeout) => "心跳超时",
        (Locale::En, Msg::StatHeartbeatTimeout) => "Heartbeat timeout",
        (Locale::Zh, Msg::RefreshInterval) => "刷新间隔",
        (Locale::En, Msg::RefreshInterval) => "Refresh interval",
        (Locale::Zh, Msg::RefreshSec1) => "1 秒",
        (Locale::En, Msg::RefreshSec1) => "1 second",
        (Locale::Zh, Msg::RefreshSec2) => "2 秒",
        (Locale::En, Msg::RefreshSec2) => "2 seconds",
        (Locale::Zh, Msg::RefreshSec5) => "5 秒",
        (Locale::En, Msg::RefreshSec5) => "5 seconds",
        (Locale::Zh, Msg::RefreshSec10) => "10 秒",
        (Locale::En, Msg::RefreshSec10) => "10 seconds",
        (Locale::Zh, Msg::RefreshSec30) => "30 秒",
        (Locale::En, Msg::RefreshSec30) => "30 seconds",
        (Locale::Zh, Msg::KafkaMode) => "模式",
        (Locale::En, Msg::KafkaMode) => "Mode",
        (Locale::Zh, Msg::KafkaModeCommon) => "Common",
        (Locale::En, Msg::KafkaModeCommon) => "Common",
        (Locale::Zh, Msg::KafkaModeAgent) => "Agent",
        (Locale::En, Msg::KafkaModeAgent) => "Agent",
        (Locale::Zh, Msg::KafkaAgentFormat) => "编码",
        (Locale::En, Msg::KafkaAgentFormat) => "Format",
        (Locale::Zh, Msg::KafkaAgentFormatJson) => "JSON",
        (Locale::En, Msg::KafkaAgentFormatJson) => "JSON",
        (Locale::Zh, Msg::KafkaAgentFormatPb) => "PB",
        (Locale::En, Msg::KafkaAgentFormatPb) => "PB",
        (Locale::Zh, Msg::KafkaAgentSourceId) => "source_id",
        (Locale::En, Msg::KafkaAgentSourceId) => "source_id",
        (Locale::Zh, Msg::KafkaAgentAppname) => "appname",
        (Locale::En, Msg::KafkaAgentAppname) => "appname",
        (Locale::Zh, Msg::KafkaAgentTag) => "tag",
        (Locale::En, Msg::KafkaAgentTag) => "tag",
        (Locale::Zh, Msg::KafkaAgentDomain) => "domain",
        (Locale::En, Msg::KafkaAgentDomain) => "domain",
    }
}

#[derive(Clone, Copy)]
pub struct I18n {
    pub locale: ReadSignal<Locale>,
    pub set_locale: WriteSignal<Locale>,
}

impl I18n {
    /// 取当前语言的文案；在 view 里请写 `move || i18n.t(Msg::…)` 以订阅语言切换。
    pub fn t(&self, key: Msg) -> &'static str {
        translate(self.locale.get(), key)
    }

    pub fn worker_status_label(&self, key: crate::model::WorkerStatusKey) -> &'static str {
        use crate::model::WorkerStatusKey;
        match key {
            WorkerStatusKey::Stopped => self.t(Msg::StatusStopped),
            WorkerStatusKey::Healthy => self.t(Msg::StatusHealthy),
            WorkerStatusKey::Unhealthy => self.t(Msg::StatusUnhealthy),
        }
    }
}

pub fn provide_i18n() -> I18n {
    let (locale, set_locale) = signal(initial_locale());

    Effect::new(move |_| {
        let value = locale.get();
        apply_locale_to_dom(value);
        persist_locale(value);
    });

    let ctx = I18n {
        locale,
        set_locale,
    };
    provide_context(ctx);
    ctx
}

pub fn use_i18n() -> I18n {
    expect_context::<I18n>()
}

fn initial_locale() -> Locale {
    read_stored_locale().unwrap_or(Locale::Zh)
}

const LOCALE_KEY: &str = "logkit-locale";

fn read_stored_locale() -> Option<Locale> {
    crate::browser_storage::read(LOCALE_KEY).and_then(|v| Locale::parse(&v))
}

fn persist_locale(locale: Locale) {
    crate::browser_storage::write(LOCALE_KEY, locale.as_str());
}

fn apply_locale_to_dom(locale: Locale) {
    #[cfg(feature = "hydrate")]
    {
        if let Some(html) = web_sys::window()
            .and_then(|w| w.document())
            .and_then(|d| d.document_element())
        {
            let lang = match locale {
                Locale::Zh => "zh-Hans",
                Locale::En => "en",
            };
            let _ = html.set_attribute("lang", lang);
        }
    }
    #[cfg(not(feature = "hydrate"))]
    {
        let _ = locale;
    }
}


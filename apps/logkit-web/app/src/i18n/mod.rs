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
    AppSubtitle,
    AddConnection,
    LoadingConnections,
    NoConnections,
    NoConnectionsHint,
    Edit,
    Delete,
    PingFailed,
    Workers,
    Connections,
    Refresh,
    LoadingWorkers,
    NoWorkers,
    NoWorkersHint,
    ColLabel,
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
    KindLocal,
    KindRemote,
    ThemeLight,
    ThemeDark,
    ThemeSystem,
    Settings,
    SettingsSubtitle,
    Appearance,
    Language,
    LocaleZh,
    LocaleEn,
    SinkFileRequiresMinLogend,
    LogendServerVersion,
}

pub fn translate(locale: Locale, key: Msg) -> &'static str {
    match (locale, key) {
        (Locale::Zh, Msg::PageNotFound) => "未找到页面",
        (Locale::En, Msg::PageNotFound) => "Page not found",
        (Locale::Zh, Msg::AppSubtitle) => "管理本机或远端 logend 连接",
        (Locale::En, Msg::AppSubtitle) => "Manage local or remote logend connections",
        (Locale::Zh, Msg::AddConnection) => "添加连接",
        (Locale::En, Msg::AddConnection) => "Add connection",
        (Locale::Zh, Msg::LoadingConnections) => "加载连接…",
        (Locale::En, Msg::LoadingConnections) => "Loading connections…",
        (Locale::Zh, Msg::NoConnections) => "还没有连接。",
        (Locale::En, Msg::NoConnections) => "No connections yet.",
        (Locale::Zh, Msg::NoConnectionsHint) => {
            "点击「添加连接」保存本机 UDS 或远端 TCP logend。"
        }
        (Locale::En, Msg::NoConnectionsHint) => {
            "Click \"Add connection\" to save a local UDS or remote TCP logend."
        }
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
        (Locale::Zh, Msg::Refresh) => "刷新",
        (Locale::En, Msg::Refresh) => "Refresh",
        (Locale::Zh, Msg::LoadingWorkers) => "加载 worker…",
        (Locale::En, Msg::LoadingWorkers) => "Loading workers…",
        (Locale::Zh, Msg::NoWorkers) => "当前 logend 上没有 worker。",
        (Locale::En, Msg::NoWorkers) => "No workers on this logend.",
        (Locale::Zh, Msg::NoWorkersHint) => "点击「启动 worker」，按 Body → Sink → 选项填写并提交。",
        (Locale::En, Msg::NoWorkersHint) => {
            "Click \"Start worker\" and fill in Body → Sink → Options."
        }
        (Locale::Zh, Msg::StartWorker) => "启动 worker",
        (Locale::En, Msg::StartWorker) => "Start worker",
        (Locale::Zh, Msg::NewWorker) => "启动 worker",
        (Locale::En, Msg::NewWorker) => "Start worker",
        (Locale::Zh, Msg::FlowBody) => "1. Body",
        (Locale::En, Msg::FlowBody) => "1. Body",
        (Locale::Zh, Msg::FlowSink) => "2. Sink",
        (Locale::En, Msg::FlowSink) => "2. Sink",
        (Locale::Zh, Msg::FlowOptions) => "3. 其他",
        (Locale::En, Msg::FlowOptions) => "3. Options",
        (Locale::Zh, Msg::BodyPreset) => "Body 预设",
        (Locale::En, Msg::BodyPreset) => "Body preset",
        (Locale::Zh, Msg::SinkType) => "Sink 类型",
        (Locale::En, Msg::SinkType) => "Sink type",
        (Locale::Zh, Msg::SinkStdout) => "标准输出 (stdout)",
        (Locale::En, Msg::SinkStdout) => "Standard output (stdout)",
        (Locale::Zh, Msg::SinkFile) => "文件 (file)",
        (Locale::En, Msg::SinkFile) => "File",
        (Locale::Zh, Msg::SinkKafka) => "Kafka",
        (Locale::En, Msg::SinkKafka) => "Kafka",
        (Locale::Zh, Msg::FileOutput) => "输出路径（可留空自动生成）",
        (Locale::En, Msg::FileOutput) => "Output path (optional, auto if empty)",
        (Locale::Zh, Msg::FileMaxSize) => "单文件上限（可选，如 64MiB）",
        (Locale::En, Msg::FileMaxSize) => "Max file size (optional, e.g. 64MiB)",
        (Locale::Zh, Msg::KafkaTopic) => "Topic",
        (Locale::En, Msg::KafkaTopic) => "Topic",
        (Locale::Zh, Msg::KafkaBrokers) => "Brokers（可留空用默认）",
        (Locale::En, Msg::KafkaBrokers) => "Brokers (optional)",
        (Locale::Zh, Msg::Rate) => "速率 (min-interval)",
        (Locale::En, Msg::Rate) => "Rate (min-interval)",
        (Locale::Zh, Msg::ThreadsOptional) => "并发线程（可选）",
        (Locale::En, Msg::ThreadsOptional) => "Threads (optional)",
        (Locale::Zh, Msg::ScriptLabel) => "标签（可选）",
        (Locale::En, Msg::ScriptLabel) => "Label (optional)",
        (Locale::Zh, Msg::ColLabel) => "标签",
        (Locale::En, Msg::ColLabel) => "Label",
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
        (Locale::Zh, Msg::SocketOptional) => "套接字（可留空）",
        (Locale::En, Msg::SocketOptional) => "Socket (optional)",
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
        (Locale::Zh, Msg::SettingsSubtitle) => "界面语言与主题偏好保存在本机浏览器。",
        (Locale::En, Msg::SettingsSubtitle) => "Language and theme preferences are stored in this browser.",
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


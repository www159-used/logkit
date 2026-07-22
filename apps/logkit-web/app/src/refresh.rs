use leptos::prelude::*;

pub const DEFAULT_REFRESH_MS: u32 = 1000;

const REFRESH_KEY: &str = "logkit-refresh-ms";

/// 可选的 worker 列表 / 详情轮询间隔。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshPeriod {
    Sec1,
    Sec2,
    Sec5,
    Sec10,
    Sec30,
}

impl RefreshPeriod {
    pub const ALL: [Self; 5] = [
        Self::Sec1,
        Self::Sec2,
        Self::Sec5,
        Self::Sec10,
        Self::Sec30,
    ];

    pub fn as_ms(self) -> u32 {
        match self {
            Self::Sec1 => 1_000,
            Self::Sec2 => 2_000,
            Self::Sec5 => 5_000,
            Self::Sec10 => 10_000,
            Self::Sec30 => 30_000,
        }
    }

    pub fn from_ms(ms: u32) -> Self {
        Self::ALL
            .into_iter()
            .min_by_key(|p| p.as_ms().abs_diff(ms))
            .unwrap_or(Self::Sec1)
    }
}

#[derive(Clone, Copy)]
pub struct RefreshCtx {
    pub interval_ms: ReadSignal<u32>,
    pub set_interval_ms: WriteSignal<u32>,
}

pub fn provide_refresh_interval() -> RefreshCtx {
    let (interval_ms, set_interval_ms) = signal(initial_refresh_ms());

    Effect::new(move |_| {
        let ms = RefreshPeriod::from_ms(interval_ms.get()).as_ms();
        if interval_ms.get() != ms {
            set_interval_ms.set(ms);
        }
        persist_refresh_ms(ms);
    });

    let ctx = RefreshCtx {
        interval_ms,
        set_interval_ms,
    };
    provide_context(ctx);
    ctx
}

pub fn use_refresh_interval() -> RefreshCtx {
    expect_context::<RefreshCtx>()
}

fn initial_refresh_ms() -> u32 {
    read_stored_refresh_ms()
        .map(RefreshPeriod::from_ms)
        .map(RefreshPeriod::as_ms)
        .unwrap_or(DEFAULT_REFRESH_MS)
}

fn read_stored_refresh_ms() -> Option<u32> {
    crate::browser_storage::read(REFRESH_KEY)
        .and_then(|v| v.parse().ok())
        .filter(|&ms| ms >= 500 && ms <= 60_000)
}

fn persist_refresh_ms(ms: u32) {
    crate::browser_storage::write(REFRESH_KEY, &ms.to_string());
}

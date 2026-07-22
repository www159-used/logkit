use leptos::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThemePreference {
    System,
    Light,
    Dark,
}

impl ThemePreference {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::Light => "light",
            Self::Dark => "dark",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "system" => Some(Self::System),
            "light" => Some(Self::Light),
            "dark" => Some(Self::Dark),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
pub struct ThemeCtx {
    pub preference: ReadSignal<ThemePreference>,
    pub set_preference: WriteSignal<ThemePreference>,
}

pub fn provide_theme() -> ThemeCtx {
    let (preference, set_preference) = signal(initial_theme());

    Effect::new(move |_| {
        let value = preference.get();
        apply_theme_to_dom(value);
        persist_theme(value);
    });

    let ctx = ThemeCtx {
        preference,
        set_preference,
    };
    provide_context(ctx);
    ctx
}

pub fn use_theme() -> ThemeCtx {
    expect_context::<ThemeCtx>()
}

fn initial_theme() -> ThemePreference {
    read_stored_theme().unwrap_or(ThemePreference::System)
}

const THEME_KEY: &str = "logkit-theme";

fn read_stored_theme() -> Option<ThemePreference> {
    crate::browser_storage::read(THEME_KEY).and_then(|v| ThemePreference::parse(&v))
}

fn persist_theme(theme: ThemePreference) {
    crate::browser_storage::write(THEME_KEY, theme.as_str());
}

fn apply_theme_to_dom(preference: ThemePreference) {
    #[cfg(feature = "hydrate")]
    {
        if let Some(html) = web_sys::window()
            .and_then(|w| w.document())
            .and_then(|d| d.document_element())
        {
            let _ = html.set_attribute("data-theme", preference.as_str());
        }
    }
    #[cfg(not(feature = "hydrate"))]
    {
        let _ = preference;
    }
}


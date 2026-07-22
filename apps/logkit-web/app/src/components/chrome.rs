use crate::components::connection_new_href;
use crate::i18n::{use_i18n, Msg};

use leptos::prelude::*;
use leptos_router::components::A;
use leptos_router::hooks::use_location;

#[component]
fn IconBack() -> impl IntoView {
    view! {
        <svg
            class="chrome-icon"
            xmlns="http://www.w3.org/2000/svg"
            width="18"
            height="18"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            stroke-width="2"
            stroke-linecap="round"
            stroke-linejoin="round"
            aria-hidden="true"
        >
            <path d="M15 18l-6-6 6-6"/>
        </svg>
    }
}

#[component]
fn IconPlus() -> impl IntoView {
    view! {
        <svg
            class="chrome-icon"
            xmlns="http://www.w3.org/2000/svg"
            width="18"
            height="18"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            stroke-width="2"
            stroke-linecap="round"
            aria-hidden="true"
        >
            <path d="M5 12h14"/>
            <path d="M12 5v14"/>
        </svg>
    }
}

#[component]
fn IconSettings() -> impl IntoView {
    view! {
        <svg
            class="chrome-icon"
            xmlns="http://www.w3.org/2000/svg"
            width="18"
            height="18"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            stroke-width="2"
            stroke-linecap="round"
            stroke-linejoin="round"
            aria-hidden="true"
        >
            <path d="M12.22 2h-.44a2 2 0 0 0-2 2v.18a2 2 0 0 1-1 1.73l-.43.25a2 2 0 0 1-2 0l-.15-.08a2 2 0 0 0-2.73.73l-.22.38a2 2 0 0 0 .73 2.73l.15.1a2 2 0 0 1 1 1.72v.51a2 2 0 0 1-1 1.74l-.15.09a2 2 0 0 0-.73 2.73l.22.38a2 2 0 0 0 2.73.73l.15-.08a2 2 0 0 1 2 0l.43.25a2 2 0 0 1 1 1.73V20a2 2 0 0 0 2 2h.44a2 2 0 0 0 2-2v-.18a2 2 0 0 1 1-1.73l.43-.25a2 2 0 0 1 2 0l.15.08a2 2 0 0 0 2.73-.73l.22-.39a2 2 0 0 0-.73-2.73l-.15-.08a2 2 0 0 1-1-1.74v-.5a2 2 0 0 1 1-1.74l.15-.09a2 2 0 0 0 .73-2.73l-.22-.38a2 2 0 0 0-2.73-.73l-.15.08a2 2 0 0 1-2 0l-.43-.25a2 2 0 0 1-1-1.73V4a2 2 0 0 0-2-2z"/>
            <circle cx="12" cy="12" r="3"/>
        </svg>
    }
}

#[component]
pub fn AppChrome() -> impl IntoView {
    let i18n = use_i18n();
    let location = use_location();

    let on_home = move || {
        let path = location.pathname.get();
        path == "/" || path.is_empty()
    };
    let show_back = move || !on_home();
    let on_settings = move || location.pathname.get().starts_with("/settings");
    let back_href = move || chrome_back_href(&location.pathname.get());

    view! {
        <header class="chrome" data-tauri-drag-region>
            <div class="chrome-drag" data-tauri-drag-region></div>
            <div class="chrome-actions">
                <Show when=show_back>
                    <A
                        href=back_href
                        attr:class="chrome-icon-btn"
                        attr:aria-label=move || i18n.t(Msg::Back)
                        prop:title=move || i18n.t(Msg::Back)
                    >
                        <IconBack/>
                    </A>
                </Show>
                <Show when=on_home>
                    <A
                        href=connection_new_href()
                        attr:class="chrome-icon-btn chrome-icon-btn-primary"
                        attr:aria-label=move || i18n.t(Msg::AddConnection)
                        prop:title=move || i18n.t(Msg::AddConnection)
                    >
                        <IconPlus/>
                    </A>
                </Show>
                <A
                    href="/settings"
                    attr:class=move || {
                        if on_settings() {
                            "chrome-icon-btn chrome-icon-btn-active"
                        } else {
                            "chrome-icon-btn"
                        }
                    }
                    attr:aria-label=move || i18n.t(Msg::Settings)
                    prop:title=move || i18n.t(Msg::Settings)
                >
                    <IconSettings/>
                </A>
            </div>
        </header>
    }
}

fn chrome_back_href(path: &str) -> String {
    let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
    if parts.first() == Some(&"connections") {
        return "/".into();
    }
    if parts.len() >= 4 && parts.first() == Some(&"c") && parts.get(2) == Some(&"workers") {
        return format!("/c/{}/workers", parts[1]);
    }
    if parts.len() >= 3 && parts.first() == Some(&"c") && parts.get(2) == Some(&"workers") {
        return "/".into();
    }
    "/".into()
}

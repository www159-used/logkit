use crate::i18n::{use_i18n, Msg};

use leptos::prelude::*;
use leptos_router::components::A;

#[component]
pub fn AppChrome() -> impl IntoView {
    let i18n = use_i18n();

    view! {
        <header class="chrome" data-tauri-drag-region>
            <A href="/" attr:class="chrome-brand">"Logkit"</A>
            <nav class="chrome-nav">
                <A href="/" attr:class="chrome-link">
                    {move || i18n.t(Msg::Connections)}
                </A>
                <A href="/settings" attr:class="chrome-link">
                    {move || i18n.t(Msg::Settings)}
                </A>
            </nav>
        </header>
    }
}

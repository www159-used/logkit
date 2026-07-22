use crate::i18n::{use_i18n, Locale, Msg};
use crate::theme::{use_theme, ThemePreference};

use leptos::prelude::*;
use leptos_router::components::A;

#[component]
pub fn SettingsPage() -> impl IntoView {
    let i18n = use_i18n();
    let theme_ctx = use_theme();

    view! {
        <div class="page">
            <header class="header">
                <div>
                    <p class="breadcrumb">
                        <A href="/">{move || i18n.t(Msg::Connections)}</A>
                        " / "
                        {move || i18n.t(Msg::Settings)}
                    </p>
                    <h1 class="title">{move || i18n.t(Msg::Settings)}</h1>
                    <p class="subtitle">{move || i18n.t(Msg::SettingsSubtitle)}</p>
                </div>
            </header>

            <section class="card settings-section">
                <h2 class="settings-heading">{move || i18n.t(Msg::Appearance)}</h2>
                <fieldset class="settings-fieldset">
                    <label class="settings-option">
                        <input
                            type="radio"
                            name="theme"
                            prop:checked=move || theme_ctx.preference.get() == ThemePreference::System
                            on:change=move |_| theme_ctx.set_preference.set(ThemePreference::System)
                        />
                        <span>{move || i18n.t(Msg::ThemeSystem)}</span>
                    </label>
                    <label class="settings-option">
                        <input
                            type="radio"
                            name="theme"
                            prop:checked=move || theme_ctx.preference.get() == ThemePreference::Light
                            on:change=move |_| theme_ctx.set_preference.set(ThemePreference::Light)
                        />
                        <span>{move || i18n.t(Msg::ThemeLight)}</span>
                    </label>
                    <label class="settings-option">
                        <input
                            type="radio"
                            name="theme"
                            prop:checked=move || theme_ctx.preference.get() == ThemePreference::Dark
                            on:change=move |_| theme_ctx.set_preference.set(ThemePreference::Dark)
                        />
                        <span>{move || i18n.t(Msg::ThemeDark)}</span>
                    </label>
                </fieldset>
            </section>

            <section class="card settings-section">
                <h2 class="settings-heading">{move || i18n.t(Msg::Language)}</h2>
                <fieldset class="settings-fieldset">
                    <label class="settings-option">
                        <input
                            type="radio"
                            name="locale"
                            prop:checked=move || i18n.locale.get() == Locale::Zh
                            on:change=move |_| i18n.set_locale.set(Locale::Zh)
                        />
                        <span>{move || i18n.t(Msg::LocaleZh)}</span>
                    </label>
                    <label class="settings-option">
                        <input
                            type="radio"
                            name="locale"
                            prop:checked=move || i18n.locale.get() == Locale::En
                            on:change=move |_| i18n.set_locale.set(Locale::En)
                        />
                        <span>{move || i18n.t(Msg::LocaleEn)}</span>
                    </label>
                </fieldset>
            </section>
        </div>
    }
}

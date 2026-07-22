use crate::app_info::{APP_LICENSE, APP_NAME, APP_VERSION, DOCS_URL, LICENSE_URL, REPO_URL};
use crate::i18n::{use_i18n, Locale, Msg};
use crate::theme::{use_theme, ThemePreference};

use crate::refresh::{RefreshPeriod, use_refresh_interval};

use crate::components::{PageHeader, PageHeaderMain, PageShell, PageTitle, SectionHeading};

use leptos::prelude::*;

#[component]
pub fn SettingsPage() -> impl IntoView {
    let i18n = use_i18n();
    let theme_ctx = use_theme();
    let refresh_ctx = use_refresh_interval();

    view! {
        <PageShell>
            <PageHeader>
                <PageHeaderMain>
                    <PageTitle>{move || i18n.t(Msg::Settings)}</PageTitle>
                </PageHeaderMain>
            </PageHeader>

            <section class="card settings-section">
                <SectionHeading>{move || i18n.t(Msg::Appearance)}</SectionHeading>
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
                <SectionHeading>{move || i18n.t(Msg::RefreshInterval)}</SectionHeading>
                <fieldset class="settings-fieldset">
                    {RefreshPeriod::ALL.into_iter().map(|period| {
                        let ms = period.as_ms();
                        view! {
                            <label class="settings-option">
                                <input
                                    type="radio"
                                    name="refresh"
                                    prop:checked=move || refresh_ctx.interval_ms.get() == ms
                                    on:change=move |_| refresh_ctx.set_interval_ms.set(ms)
                                />
                                <span>{refresh_period_label(i18n, period)}</span>
                            </label>
                        }
                    }).collect_view()}
                </fieldset>
            </section>

            <section class="card settings-section">
                <SectionHeading>{move || i18n.t(Msg::Language)}</SectionHeading>
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

            <section class="card settings-section about-section">
                <SectionHeading>{move || i18n.t(Msg::About)}</SectionHeading>
                <p class="about-name">{APP_NAME}</p>
                <dl class="about-list">
                    <dt>{move || i18n.t(Msg::AboutVersion)}</dt>
                    <dd class="mono">{APP_VERSION}</dd>
                    <dt>{move || i18n.t(Msg::AboutLicense)}</dt>
                    <dd>
                        <a href=LICENSE_URL target="_blank" rel="noopener noreferrer">
                            {APP_LICENSE}
                        </a>
                    </dd>
                    <dt>{move || i18n.t(Msg::AboutDocs)}</dt>
                    <dd>
                        <a href=DOCS_URL target="_blank" rel="noopener noreferrer">
                            "logkit book"
                        </a>
                    </dd>
                    <dt>{move || i18n.t(Msg::AboutSource)}</dt>
                    <dd>
                        <a href=REPO_URL target="_blank" rel="noopener noreferrer">
                            "github.com/www159-used/logkit"
                        </a>
                    </dd>
                    <dt>{move || i18n.t(Msg::AboutRuntime)}</dt>
                    <dd>
                        <span class="about-runtime-desktop">{move || i18n.t(Msg::AboutRuntimeDesktop)}</span>
                        <span class="about-runtime-web">{move || i18n.t(Msg::AboutRuntimeWeb)}</span>
                    </dd>
                </dl>
            </section>
        </PageShell>
    }
}

fn refresh_period_label(i18n: crate::i18n::I18n, period: RefreshPeriod) -> &'static str {
    use crate::i18n::Msg;
    match period {
        RefreshPeriod::Sec1 => i18n.t(Msg::RefreshSec1),
        RefreshPeriod::Sec2 => i18n.t(Msg::RefreshSec2),
        RefreshPeriod::Sec5 => i18n.t(Msg::RefreshSec5),
        RefreshPeriod::Sec10 => i18n.t(Msg::RefreshSec10),
        RefreshPeriod::Sec30 => i18n.t(Msg::RefreshSec30),
    }
}

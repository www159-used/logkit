#![recursion_limit = "256"]

mod api;
mod browser_storage;
mod components;
mod i18n;
mod model;
mod pages;
mod theme;

pub use pages::{ConnectionsPage, SettingsPage, WorkersPage};

use components::AppChrome;
use i18n::provide_i18n;
use leptos::hydration::{AutoReload, HydrationScripts};
use leptos::prelude::*;
use leptos_meta::{provide_meta_context, MetaTags, Stylesheet, Title};
use leptos_router::{
    components::{Route, Router, Routes},
    path,
};
use theme::provide_theme;

pub fn shell(options: LeptosOptions) -> impl IntoView {
    view! {
        <!DOCTYPE html>
        <html lang="zh-Hans">
            <head>
                <meta charset="utf-8"/>
                <meta name="viewport" content="width=device-width, initial-scale=1"/>
                <Title text="Logkit"/>
                <script>
                    "(function(){var k='logkit-theme',s=localStorage.getItem(k),d=document.documentElement;"
                    "if(s==='light'||s==='dark'||s==='system'){d.setAttribute('data-theme',s);}else{d.setAttribute('data-theme','system');}"
                    "if(typeof window.__TAURI__!=='undefined'||typeof window.__TAURI_INTERNALS__!=='undefined'){"
                    "d.classList.add('tauri');"
                    "if(/Mac|iPhone|iPod|iPad/.test(navigator.platform)||/Mac OS X/.test(navigator.userAgent)){"
                    "d.classList.add('platform-macos');}}})();"
                </script>
                <Stylesheet id="leptos" href="/pkg/logkit-web.css"/>
                <AutoReload options=options.clone()/>
                <HydrationScripts options=options.clone()/>
                <MetaTags/>
            </head>
            <body>
                <App/>
            </body>
        </html>
    }
}

#[component]
pub fn App() -> impl IntoView {
    provide_meta_context();
    provide_i18n();
    provide_theme();

    view! {
        <Router>
            <AppChrome/>
            <main class="shell">
                <Routes fallback=|| {
                    let i18n = i18n::use_i18n();
                    view! {
                        <p class="page muted">
                            {move || i18n.t(i18n::Msg::PageNotFound)}
                        </p>
                    }
                }>
                    <Route path=path!("settings") view=SettingsPage/>
                    <Route path=path!("c/:id/workers") view=WorkersPage/>
                    <Route path=path!("") view=ConnectionsPage/>
                </Routes>
            </main>
        </Router>
    }
}

#[cfg(feature = "ssr")]
pub fn build_router(leptos_options: LeptosOptions) -> axum::Router {
    use axum::Router;
    use leptos_axum::{generate_route_list, LeptosRoutes};
    use tower_http::cors::{AllowOrigin, CorsLayer};

    let routes = generate_route_list(App);
    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::predicate(|origin, _parts| {
            let origin_bytes = origin.as_bytes();
            origin_bytes == b"tauri://localhost"
                || origin_bytes.starts_with(b"http://localhost:")
                || origin_bytes.starts_with(b"http://127.0.0.1:")
        }))
        .allow_methods([
            axum::http::Method::GET,
            axum::http::Method::POST,
            axum::http::Method::OPTIONS,
        ])
        .allow_headers([
            axum::http::header::CONTENT_TYPE,
            axum::http::header::ACCEPT,
        ]);

    Router::new()
        .leptos_routes(&leptos_options, routes, {
            let leptos_options = leptos_options.clone();
            move || shell(leptos_options.clone())
        })
        .layer(cors)
        .fallback(leptos_axum::file_and_error_handler(shell))
        .with_state(leptos_options)
}

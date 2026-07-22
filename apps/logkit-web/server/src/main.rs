use leptos::logging::log;
use leptos::prelude::*;
use logkit_web_app::build_router;

#[tokio::main]
async fn main() {
    simple_logger::init_with_level(log::Level::Info).expect("logger");
    let conf = get_configuration(None).expect("leptos configuration");
    let addr = conf.leptos_options.site_addr;
    let leptos_options = conf.leptos_options;
    let app = build_router(leptos_options);

    log!("logkit-web listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app.into_make_service())
        .await
        .unwrap();
}

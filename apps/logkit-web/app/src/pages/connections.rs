use crate::api::{
    delete_connection, list_connections, ping_saved_connection,
};
use crate::components::{ConnectionsList, EmptyState, PageShell, use_toast};
use crate::i18n::{use_i18n, Msg};
use crate::model::{ConnectionId, LogendConnection};

use leptos::prelude::*;
use std::sync::Arc;

#[component]
pub fn ConnectionsPage() -> impl IntoView {
    let i18n = use_i18n();
    let toast = use_toast();
    let (connections, set_connections) = signal(Vec::<LogendConnection>::new());

    let connections_res = Resource::new(
        || (),
        |_| async move { list_connections().await },
    );

    let toast_for_effect = toast.clone();
    Effect::new(move |_| {
        if let Some(Ok(list)) = connections_res.get() {
            set_connections.set(list.clone());
        } else if let Some(Err(e)) = connections_res.get() {
            toast_for_effect.error(e.to_string());
        }
    });

    let on_ping: Arc<dyn Fn(ConnectionId) + Send + Sync> = Arc::new({
        let toast = toast.clone();
        let failed = i18n.t(Msg::PingFailed);
        move |id: ConnectionId| {
            let toast = toast.clone();
            leptos::task::spawn_local(async move {
                match ping_saved_connection(id).await {
                    Ok(r) => toast.success(format!(
                        "{} → {} [{}]",
                        r.endpoint,
                        r.pong,
                        r.server_version.display_short()
                    )),
                    Err(e) => toast.error(format!("{failed}: {e}")),
                }
            });
        }
    });

    let on_delete: Arc<dyn Fn(ConnectionId) + Send + Sync> = Arc::new({
        let toast = toast.clone();
        move |id: ConnectionId| {
            let toast = toast.clone();
            leptos::task::spawn_local(async move {
                match delete_connection(id).await {
                    Ok(deleted) => {
                        set_connections.update(|list| list.retain(|c| c.id != deleted));
                    }
                    Err(e) => toast.error(e.to_string()),
                }
            });
        }
    });

    view! {
        <PageShell>
            <Suspense fallback=move || view! {
                <p class="muted">{i18n.t(Msg::LoadingConnections)}</p>
            }>
                {move || {
                    let list = connections.get();
                    if list.is_empty() {
                        view! {
                            <EmptyState>
                                <p>{i18n.t(Msg::NoConnections)}</p>
                            </EmptyState>
                        }.into_any()
                    } else {
                        view! {
                            <ConnectionsList
                                connections=list
                                i18n=i18n
                                on_ping=on_ping.clone()
                                on_delete=on_delete.clone()
                            />
                        }.into_any()
                    }
                }}
            </Suspense>
        </PageShell>
    }
}

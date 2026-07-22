use crate::api::{
    delete_connection, list_connections, ping_saved_connection,
};
use crate::components::{kind_class, kind_label, workers_href, ConnectionForm};
use crate::i18n::{use_i18n, Msg};
use crate::model::{upsert_sorted, ConnectionId, LogendConnection};

use leptos::prelude::*;
use leptos_router::components::A;

#[component]
pub fn ConnectionsPage() -> impl IntoView {
    let i18n = use_i18n();
    let (connections, set_connections) = signal(Vec::<LogendConnection>::new());
    let (ping_msg, set_ping_msg) = signal(String::new());
    let (show_form, set_show_form) = signal(false);
    let (edit, set_edit) = signal(None::<LogendConnection>);

    let connections_res = Resource::new(
        || (),
        |_| async move { list_connections().await },
    );

    Effect::new(move |_| {
        if let Some(Ok(list)) = connections_res.get() {
            set_connections.set(list.clone());
        } else if let Some(Err(e)) = connections_res.get() {
            set_ping_msg.set(e.to_string());
        }
    });

    let on_ping = move |id: ConnectionId| {
        leptos::task::spawn_local(async move {
            set_ping_msg.set(String::new());
            match ping_saved_connection(id).await {
                Ok(r) => set_ping_msg.set(format!(
                    "{} → {} [{}]",
                    r.endpoint,
                    r.pong,
                    r.server_version.display_short()
                )),
                Err(e) => {
                    set_ping_msg.set(format!("{}: {e}", i18n.t(Msg::PingFailed)));
                }
            }
        });
    };

    let on_delete = move |id: ConnectionId| {
        leptos::task::spawn_local(async move {
            match delete_connection(id).await {
                Ok(deleted) => {
                    set_connections.update(|list| list.retain(|c| c.id != deleted));
                }
                Err(e) => set_ping_msg.set(e.to_string()),
            }
        });
    };

    view! {
        <div class="page">
            <header class="header">
                <div>
                    <h1 class="title">"Logkit"</h1>
                    <p class="subtitle">{move || i18n.t(Msg::AppSubtitle)}</p>
                </div>
                <div class="header-actions">
                    <button
                        type="button"
                        class="btn btn-primary"
                        on:click=move |_| {
                            set_edit.set(None);
                            set_show_form.set(true);
                        }
                    >
                        {move || i18n.t(Msg::AddConnection)}
                    </button>
                </div>
            </header>

            <Show when=move || show_form.get()>
                <ConnectionForm
                    initial=edit
                    on_saved=move |conn| {
                        set_connections.update(|list| upsert_sorted(list, conn));
                        set_show_form.set(false);
                    }
                    on_cancel=move || set_show_form.set(false)
                />
            </Show>

            <Show when=move || !ping_msg.get().is_empty()>
                <p class="banner">{move || ping_msg.get()}</p>
            </Show>

            <Suspense fallback=move || view! {
                <p class="muted">{i18n.t(Msg::LoadingConnections)}</p>
            }>
                {move || {
                    let list = connections.get();
                    if list.is_empty() {
                        view! {
                            <div class="empty">
                                <p>{i18n.t(Msg::NoConnections)}</p>
                                <p class="muted">{i18n.t(Msg::NoConnectionsHint)}</p>
                            </div>
                        }.into_any()
                    } else {
                        view! {
                            <ul class="grid">
                                {list.into_iter().map(|c| {
                                    let id = c.id;
                                    let kind = c.kind;
                                    let name = c.name.clone();
                                    let endpoint = c.endpoint_display();
                                    let workers_link = workers_href(id);
                                    let notes = c.notes.clone();
                                    let has_notes = !notes.is_empty();
                                    view! {
                                        <li class="card">
                                            <div class="card-head">
                                                <h2>
                                                    <A href=workers_link.clone() attr:class="card-title-link">
                                                        {name.clone()}
                                                    </A>
                                                </h2>
                                                <span class=kind_class(kind)>
                                                    {kind_label(i18n, kind)}
                                                </span>
                                            </div>
                                            <p class="endpoint">{endpoint}</p>
                                            <Show when=move || has_notes>
                                                <p class="notes">{notes.clone()}</p>
                                            </Show>
                                            <div class="actions">
                                                <A href=workers_link attr:class="btn btn-primary">
                                                    {i18n.t(Msg::Workers)}
                                                </A>
                                                <button
                                                    type="button"
                                                    class="btn"
                                                    on:click=move |_| on_ping(id)
                                                >
                                                    "Ping"
                                                </button>
                                                <button
                                                    type="button"
                                                    class="btn"
                                                    on:click=move |_| {
                                                        set_edit.set(Some(c.clone()));
                                                        set_show_form.set(true);
                                                    }
                                                >
                                                    {i18n.t(Msg::Edit)}
                                                </button>
                                                <button
                                                    type="button"
                                                    class="btn btn-danger"
                                                    on:click=move |_| on_delete(id)
                                                >
                                                    {i18n.t(Msg::Delete)}
                                                </button>
                                            </div>
                                        </li>
                                    }
                                }).collect_view()}
                            </ul>
                        }.into_any()
                    }
                }}
            </Suspense>
        </div>
    }
}

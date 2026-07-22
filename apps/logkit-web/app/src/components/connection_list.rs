use crate::components::{connection_edit_href, kind_class, kind_label, workers_href};
use crate::i18n::{I18n, Msg};
use crate::model::{ConnectionId, LogendConnection};

use leptos::prelude::*;
use leptos_router::components::A;
use std::sync::Arc;

#[component]
pub fn ConnectionsList(
    connections: Vec<LogendConnection>,
    i18n: I18n,
    on_ping: Arc<dyn Fn(ConnectionId) + Send + Sync>,
    on_delete: Arc<dyn Fn(ConnectionId) + Send + Sync>,
) -> impl IntoView {
    view! {
        <ul class="grid">
            {connections.into_iter().map(|c| {
                let id = c.id;
                let kind = c.kind;
                let name = c.name.clone();
                let endpoint = c.endpoint_display();
                let workers_link = workers_href(id);
                let edit_link = connection_edit_href(id);
                let notes = c.notes.clone();
                let has_notes = !notes.is_empty();
                let ping = on_ping.clone();
                let delete = on_delete.clone();
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
                                on:click=move |_| ping(id)
                            >
                                "Ping"
                            </button>
                            <A href=edit_link attr:class="btn">
                                {i18n.t(Msg::Edit)}
                            </A>
                            <button
                                type="button"
                                class="btn btn-danger"
                                on:click=move |_| delete(id)
                            >
                                {i18n.t(Msg::Delete)}
                            </button>
                        </div>
                    </li>
                }
            }).collect_view()}
        </ul>
    }
}

use crate::i18n::{I18n, Msg};
use crate::model::{ConnectionId, WorkerSummary};

use leptos::prelude::*;
use leptos_router::components::A;

use super::connection::worker_detail_href;

#[component]
pub fn WorkersList(
    connection_id: ConnectionId,
    workers: Vec<WorkerSummary>,
    i18n: I18n,
    on_stop: impl Fn(String) + 'static + Clone,
) -> impl IntoView {
    let on_stop = on_stop.clone();
    view! {
        <ul class="grid worker-list">
            {workers.into_iter().map(|w| {
                let worker_id = w.id.clone();
                let detail_href = worker_detail_href(connection_id, &w.id);
                let status_class = w.status_class().to_string();
                let status_key = w.status_label_key();
                let status_label = i18n.worker_status_label(status_key);
                let label = w.config_label.clone();
                let id = w.id.clone();
                let sink = w.sink_summary.clone();
                let stop = on_stop.clone();
                view! {
                    <li class="card worker-card">
                        <div class="card-head">
                            <h2>
                                <A href=detail_href.clone() attr:class="card-title-link">
                                    {label}
                                </A>
                            </h2>
                            <span class=status_class>{status_label}</span>
                        </div>
                        <p class="endpoint mono">{id}</p>
                        <p class="sink">{sink}</p>
                        <div class="actions">
                            <A href=detail_href attr:class="btn">
                                {i18n.t(Msg::ViewWorkerStatus)}
                            </A>
                            <button
                                type="button"
                                class="btn btn-danger"
                                on:click=move |ev| {
                                    ev.stop_propagation();
                                    stop(worker_id.clone());
                                }
                            >
                                {i18n.t(Msg::Stop)}
                            </button>
                        </div>
                    </li>
                }
            }).collect_view()}
        </ul>
    }
}

use crate::i18n::{I18n, Msg};
use crate::model::WorkerSummary;

use leptos::prelude::*;

#[component]
pub fn WorkersTable(
    workers: Vec<WorkerSummary>,
    i18n: I18n,
    on_stop: impl Fn(String) + 'static + Clone,
) -> impl IntoView {
    let on_stop = on_stop.clone();
    view! {
        <div class="table-wrap card">
            <table class="table">
                <thead>
                    <tr>
                        <th>"ID"</th>
                        <th>{i18n.t(Msg::ColLabel)}</th>
                        <th>{i18n.t(Msg::ColStatus)}</th>
                        <th>"EPS"</th>
                        <th>{i18n.t(Msg::ColEvents)}</th>
                        <th>"Sink"</th>
                        <th></th>
                    </tr>
                </thead>
                <tbody>
                    {workers.into_iter().map(|w| {
                        let worker_id = w.id.clone();
                        let status_class = worker_status_class(&w);
                        let status_label = worker_status_label(i18n, &w);
                        let stop = on_stop.clone();
                        view! {
                            <tr>
                                <td class="mono">{w.id.clone()}</td>
                                <td>{w.config_label.clone()}</td>
                                <td>
                                    <span class=status_class>{status_label}</span>
                                </td>
                                <td class="mono">{format_eps(w.eps)}</td>
                                <td class="mono">{w.log_events_total}</td>
                                <td class="sink">{w.sink_summary.clone()}</td>
                                <td class="row-actions">
                                    <button
                                        type="button"
                                        class="btn btn-danger"
                                        on:click=move |_| stop(worker_id.clone())
                                    >
                                        {i18n.t(Msg::Stop)}
                                    </button>
                                </td>
                            </tr>
                        }
                    }).collect_view()}
                </tbody>
            </table>
        </div>
    }
}

fn worker_status_label(i18n: I18n, w: &WorkerSummary) -> &'static str {
    if !w.alive {
        i18n.t(Msg::StatusStopped)
    } else if w.healthy {
        i18n.t(Msg::StatusHealthy)
    } else {
        i18n.t(Msg::StatusUnhealthy)
    }
}

fn worker_status_class(w: &WorkerSummary) -> &'static str {
    if !w.alive {
        "badge badge-stopped"
    } else if w.healthy {
        "badge badge-healthy"
    } else {
        "badge badge-unhealthy"
    }
}

fn format_eps(eps: f64) -> String {
    if eps <= 0.0 {
        "—".into()
    } else {
        format!("{eps:.1}")
    }
}

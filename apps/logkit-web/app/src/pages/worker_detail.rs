use crate::api::{load_worker_detail};
use crate::components::{
    clear_toast_resource_error, toast_resource_error, use_toast, workers_href, Breadcrumb, EpsChart,
    PageHeader, PageHeaderMain, PageShell, PageSubtitle, PageTitle, SectionHeading, push_eps_sample,
};
use crate::i18n::{use_i18n, Msg};
use crate::model::{ConnectionId, LogendConnection, WorkerSummary};
use crate::poll::use_poll_tick;
use crate::refresh::use_refresh_interval;

use leptos::prelude::*;
use leptos_router::components::A;
use leptos_router::hooks::use_params_map;

#[component]
pub fn WorkerDetailPage() -> impl IntoView {
    let i18n = use_i18n();
    let toast = use_toast();
    let params = use_params_map();
    let refresh = use_refresh_interval();
    let poll_tick = use_poll_tick(refresh.interval_ms);

    let connection_id = move || {
        params.with(|p| p.get("id").as_ref().and_then(|raw| ConnectionId::parse(raw)))
    };
    let worker_id = move || params.with(|p| p.get("worker_id").clone());

    let (connection, set_connection) = signal(None::<LogendConnection>);
    let (worker, set_worker) = signal(None::<WorkerSummary>);
    let (eps_history, set_eps_history) = signal(Vec::<f64>::new());

    let detail_res = Resource::new(
        move || (connection_id(), worker_id(), poll_tick.get()),
        |(conn_id, wid, _)| async move {
            let (Some(conn_id), Some(wid)) = (conn_id, wid) else {
                return Err(leptos::prelude::ServerFnError::ServerError(
                    "missing route params".into(),
                ));
            };
            load_worker_detail(conn_id, wid).await
        },
    );

    Effect::new(move |_| {
        worker_id();
        set_eps_history.set(Vec::new());
    });

    let toast_for_detail = toast.clone();
    let last_err = StoredValue::new(None::<String>);
    Effect::new(move |_| {
        match detail_res.get() {
            Some(Ok((conn, w))) => {
                clear_toast_resource_error(&last_err);
                set_connection.set(Some(conn));
                set_eps_history.update(|h| push_eps_sample(h, w.eps));
                set_worker.set(Some(w));
            }
            Some(Err(e)) => toast_resource_error(&toast_for_detail, &last_err, e),
            None => {}
        }
    });

    view! {
        <PageShell>
            <PageHeader>
                <PageHeaderMain>
                    {move || {
                        let conn = connection.get();
                        let w = worker.get();
                        match (conn, w) {
                            (Some(c), Some(w)) => {
                                let conn_name = c.name.clone();
                                let label = w.config_label.clone();
                                let crumb_label = label.clone();
                                let worker_id = w.id.clone();
                                view! {
                                    <Breadcrumb>
                                        <A href="/">{move || i18n.t(Msg::Connections)}</A>
                                        " / "
                                        <A href=workers_href(c.id)>{conn_name}</A>
                                        " / "
                                        {crumb_label}
                                    </Breadcrumb>
                                    <PageTitle>{label}</PageTitle>
                                    <PageSubtitle class="endpoint mono">{worker_id}</PageSubtitle>
                                }.into_any()
                            }
                            _ => view! {
                                <PageTitle>{move || i18n.t(Msg::Workers)}</PageTitle>
                            }.into_any(),
                        }
                    }}
                </PageHeaderMain>
            </PageHeader>

            <Suspense fallback=move || view! {
                <p class="muted">{move || i18n.t(Msg::LoadingWorker)}</p>
            }>
                {move || worker.get().map(|w| {
                    let status_label = i18n.worker_status_label(w.status_label_key());
                    view! {
                        <section class="card worker-status-card">
                            <div class="card-head">
                                <SectionHeading>{move || i18n.t(Msg::ColStatus)}</SectionHeading>
                                <span class=w.status_class().to_string()>{status_label}</span>
                            </div>
                            <dl class="desc-list worker-metrics">
                                <dt>{move || i18n.t(Msg::StatEps)}</dt>
                                <dd class="mono">{format!("{:.3}", w.eps)}</dd>
                                <dt>{move || i18n.t(Msg::StatEpsInterval)}</dt>
                                <dd class="mono">{format!("{:.3}", w.eps_interval)}</dd>
                                <dt>{move || i18n.t(Msg::ColEvents)}</dt>
                                <dd class="mono">{w.log_events_total}</dd>
                                <dt>{move || i18n.t(Msg::StatEventsEst)}</dt>
                                <dd class="mono">{format!("{:.1}", w.log_events_estimated)}</dd>
                                <dt>{move || i18n.t(Msg::StatRetry)}</dt>
                                <dd class="mono">{w.retry_total}</dd>
                                <dt>{move || i18n.t(Msg::StatHeartbeat)}</dt>
                                <dd class="mono">{format!("{:.1}s", w.seconds_since_heartbeat)}</dd>
                                <dt>{move || i18n.t(Msg::StatHeartbeatInterval)}</dt>
                                <dd class="mono">{format!("{}s", w.heartbeat_interval_secs)}</dd>
                                <dt>{move || i18n.t(Msg::StatHeartbeatTimeout)}</dt>
                                <dd class="mono">{format!("{}s", w.heartbeat_timeout_secs)}</dd>
                                <dt>"Sink"</dt>
                                <dd class="sink">{w.sink_summary.clone()}</dd>
                            </dl>
                        </section>

                        <section class="worker-chart-section">
                            <SectionHeading>{move || i18n.t(Msg::WorkerEpsChart)}</SectionHeading>
                            <EpsChart samples=eps_history/>
                        </section>
                    }
                })}
            </Suspense>
        </PageShell>
    }
}

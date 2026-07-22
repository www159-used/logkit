use crate::api::{load_workers_page, stop_connection_worker};
use crate::components::{
    clear_toast_resource_error, toast_resource_error, use_toast, worker_new_href, EmptyState,
    PageHeader, PageHeaderActions, PageHeaderMain, PageShell, PageSubtitle, PageTitle, WorkersList,
};
use crate::i18n::{use_i18n, Msg};
use crate::model::{ConnectionId, LogendConnection, LogendServerVersion, WorkerSummary};
use crate::poll::use_poll_tick;
use crate::refresh::use_refresh_interval;

use leptos::prelude::*;
use leptos_router::components::A;
use leptos_router::hooks::use_params_map;

#[component]
pub fn WorkersPage() -> impl IntoView {
    let i18n = use_i18n();
    let toast = use_toast();
    let params = use_params_map();
    let refresh = use_refresh_interval();
    let poll_tick = use_poll_tick(refresh.interval_ms);
    let connection_id = move || {
        params.with(|p| p.get("id").as_ref().and_then(|raw| ConnectionId::parse(raw)))
    };

    let (connection, set_connection) = signal(None::<LogendConnection>);
    let (workers, set_workers) = signal(Vec::<WorkerSummary>::new());
    let (server_version, set_server_version) = signal(LogendServerVersion::default());

    let workers_res = Resource::new(
        move || (connection_id(), poll_tick.get()),
        |(id, _)| async move {
            let Some(id) = id else {
                return Err(leptos::prelude::ServerFnError::ServerError(
                    "missing connection id".into(),
                ));
            };
            load_workers_page(id).await
        },
    );

    let toast_for_effect = toast.clone();
    let last_err = StoredValue::new(None::<String>);
    Effect::new(move |_| {
        match workers_res.get() {
            Some(Ok((conn, list, version))) => {
                clear_toast_resource_error(&last_err);
                set_connection.set(Some(conn));
                set_workers.set(list);
                set_server_version.set(version);
            }
            Some(Err(e)) => toast_resource_error(&toast_for_effect, &last_err, e),
            None => {}
        }
    });

    let reload = move || {
        workers_res.refetch();
    };

    let on_stop = move |worker_id: String| {
        let Some(id) = connection_id() else {
            return;
        };
        let toast = toast.clone();
        leptos::task::spawn_local(async move {
            match stop_connection_worker(id, worker_id).await {
                Ok(status) => toast.success(status),
                Err(e) => toast.error(e.to_string()),
            }
            reload();
        });
    };

    view! {
        <PageShell>
            <PageHeader>
                <PageHeaderMain>
                    {move || match connection.get() {
                        Some(c) => {
                            let name = c.name.clone();
                            let endpoint = c.endpoint_display();
                            view! {
                                <PageTitle>{name.clone()}</PageTitle>
                                <PageSubtitle class="endpoint">{endpoint}</PageSubtitle>
                                <PageSubtitle class="muted">
                                    {move || i18n.t(Msg::LogendServerVersion)}
                                    ": "
                                    {move || server_version.get().display_short()}
                                </PageSubtitle>
                            }.into_any()
                        }
                        None => view! {
                            <PageTitle>{move || i18n.t(Msg::Workers)}</PageTitle>
                        }.into_any(),
                    }}
                </PageHeaderMain>
                <PageHeaderActions>
                    {move || connection_id().map(|id| view! {
                        <A href=worker_new_href(id) attr:class="btn btn-primary">
                            {move || i18n.t(Msg::StartWorker)}
                        </A>
                    })}
                </PageHeaderActions>
            </PageHeader>

            <Suspense fallback=move || view! {
                <p class="muted">{i18n.t(Msg::LoadingWorkers)}</p>
            }>
                {move || {
                    let list = workers.get();
                    if list.is_empty() {
                        view! {
                            <EmptyState>
                                <p>{i18n.t(Msg::NoWorkers)}</p>
                            </EmptyState>
                        }.into_any()
                    } else if let Some(id) = connection_id() {
                        view! {
                            <WorkersList
                                connection_id=id
                                workers=list
                                i18n=i18n
                                on_stop=on_stop.clone()
                            />
                        }.into_any()
                    } else {
                        view! { <p class="muted">"—"</p> }.into_any()
                    }
                }}
            </Suspense>
        </PageShell>
    }
}

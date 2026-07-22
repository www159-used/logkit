use crate::api::load_workers_page;
use crate::components::{
    persist_flash, workers_href, Breadcrumb, PageHeader, PageHeaderMain, PageShell, PageSubtitle,
    PageTitle, WorkerStartForm,
};
use crate::i18n::{use_i18n, Msg};
use crate::model::{ConnectionId, LogendConnection, StartWorkerResult};

use leptos::prelude::*;
use leptos_router::components::A;
use leptos_router::hooks::{use_navigate, use_params_map};

#[component]
pub fn WorkerNewPage() -> impl IntoView {
    let i18n = use_i18n();
    let navigate = use_navigate();
    let params = use_params_map();
    let connection_id = move || {
        params.with(|p| p.get("id").as_ref().and_then(|raw| ConnectionId::parse(raw)))
    };

    let (connection, set_connection) = signal(None::<LogendConnection>);

    let page_res = Resource::new(connection_id, |id| async move {
        let Some(id) = id else {
            return Err(leptos::prelude::ServerFnError::ServerError(
                "missing connection id".into(),
            ));
        };
        load_workers_page(id).await.map(|(conn, _, version)| (conn, version))
    });

    Effect::new(move |_| {
        if let Some(Ok((conn, _))) = page_res.get() {
            set_connection.set(Some(conn));
        }
    });

    let go_back = StoredValue::new({
        let navigate = navigate.clone();
        move || {
            if let Some(id) = connection_id() {
                navigate(&workers_href(id), Default::default());
            }
        }
    });

    let on_started = StoredValue::new({
        let navigate = navigate.clone();
        move |result: StartWorkerResult| {
            let Some(id) = connection_id() else {
                return;
            };
            let msg = format_start_message(result);
            persist_flash(&msg);
            navigate(&workers_href(id), Default::default());
        }
    });

    view! {
        <PageShell>
            <PageHeader>
                <PageHeaderMain>
                    {move || match connection.get() {
                        Some(c) => {
                            let name = c.name.clone();
                            let endpoint = c.endpoint_display();
                            view! {
                                <Breadcrumb>
                                    <A href="/">{move || i18n.t(Msg::Connections)}</A>
                                    " / "
                                    <A href=workers_href(c.id)>{name.clone()}</A>
                                    " / "
                                    {move || i18n.t(Msg::NewWorker)}
                                </Breadcrumb>
                                <PageTitle>{move || i18n.t(Msg::NewWorker)}</PageTitle>
                                <PageSubtitle class="endpoint">{endpoint}</PageSubtitle>
                            }.into_any()
                        }
                        None => view! {
                            <PageTitle>{move || i18n.t(Msg::NewWorker)}</PageTitle>
                        }.into_any(),
                    }}
                </PageHeaderMain>
            </PageHeader>

            <Suspense fallback=move || view! {
                <p class="muted">{move || i18n.t(Msg::LoadingWorkers)}</p>
            }>
                {move || {
                    let Some(id) = connection_id() else {
                        return view! { <p class="muted">"—"</p> }.into_any();
                    };
                    match page_res.get() {
                        Some(Ok((_, version))) => {
                            let supports_file = version.supports_file_sink();
                            view! {
                                <WorkerStartForm
                                    connection_id=id
                                    supports_file_sink=move || supports_file
                                    on_started=move |result| on_started.with_value(|f| f(result))
                                    on_cancel=move || go_back.with_value(|f| f())
                                />
                            }.into_any()
                        }
                        Some(Err(e)) => view! {
                            <p class="error">{e.to_string()}</p>
                        }.into_any(),
                        None => view! {
                            <p class="muted">{move || i18n.t(Msg::LoadingWorkers)}</p>
                        }.into_any(),
                    }
                }}
            </Suspense>
        </PageShell>
    }
}

fn format_start_message(result: StartWorkerResult) -> String {
    if result.worker_id.is_empty() {
        if result.output.trim().is_empty() {
            result.status
        } else {
            format!("{}\n{}", result.output.trim(), result.status)
        }
    } else {
        format!(
            "{} → {} ({})",
            result.worker_id,
            result.status,
            result.output.trim()
        )
    }
}

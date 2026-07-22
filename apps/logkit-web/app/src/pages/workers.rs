use crate::api::{load_workers_page, stop_connection_worker};
use crate::components::{WorkerStartForm, WorkersTable};
use crate::i18n::{use_i18n, Msg};
use crate::model::{ConnectionId, LogendConnection, LogendServerVersion, StartWorkerResult, WorkerSummary};

use leptos::prelude::*;
use leptos_router::components::A;
use leptos_router::hooks::use_params_map;

#[component]
pub fn WorkersPage() -> impl IntoView {
    let i18n = use_i18n();
    let params = use_params_map();
    let connection_id = move || {
        params.with(|p| p.get("id").as_ref().and_then(|raw| ConnectionId::parse(raw)))
    };

    let (connection, set_connection) = signal(None::<LogendConnection>);
    let (workers, set_workers) = signal(Vec::<WorkerSummary>::new());
    let (server_version, set_server_version) = signal(LogendServerVersion::default());
    let (ping_msg, set_ping_msg) = signal(String::new());
    let (show_form, set_show_form) = signal(false);

    let workers_res = Resource::new(
        connection_id,
        |id| async move {
            let Some(id) = id else {
                return Err(leptos::prelude::ServerFnError::ServerError(
                    "missing connection id".into(),
                ));
            };
            load_workers_page(id).await
        },
    );

    Effect::new(move |_| {
        match workers_res.get() {
            Some(Ok((conn, list, version))) => {
                set_connection.set(Some(conn));
                set_workers.set(list);
                set_server_version.set(version);
            }
            Some(Err(e)) => set_ping_msg.set(e.to_string()),
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
        leptos::task::spawn_local(async move {
            match stop_connection_worker(id, worker_id).await {
                Ok(status) => set_ping_msg.set(status),
                Err(e) => set_ping_msg.set(e.to_string()),
            }
            reload();
        });
    };

    let format_start_message = |result: StartWorkerResult| -> String {
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
    };

    view! {
        <div class="page">
            <header class="header">
                <div>
                    <p class="breadcrumb">
                        <A href="/">{move || i18n.t(Msg::Connections)}</A>
                        " / "
                        {move || i18n.t(Msg::Workers)}
                    </p>
                    {move || match connection.get() {
                        Some(c) => view! {
                            <h1 class="title">{c.name.clone()}</h1>
                            <p class="subtitle endpoint">{c.endpoint_display()}</p>
                            <p class="subtitle muted">
                                {move || i18n.t(Msg::LogendServerVersion)}
                                ": "
                                {move || server_version.get().display_short()}
                            </p>
                        }.into_any(),
                        None => view! {
                            <h1 class="title">{move || i18n.t(Msg::Workers)}</h1>
                        }.into_any(),
                    }}
                </div>
                <div class="header-actions">
                    <Show when=move || connection_id().is_some()>
                        <button
                            type="button"
                            class="btn btn-primary"
                            on:click=move |_| set_show_form.set(true)
                        >
                            {move || i18n.t(Msg::StartWorker)}
                        </button>
                    </Show>
                    <button type="button" class="btn" on:click=move |_| reload()>
                        {move || i18n.t(Msg::Refresh)}
                    </button>
                </div>
            </header>

            <Show when=move || show_form.get() && connection_id().is_some()>
                {move || {
                    connection_id().map(|id| {
                        view! {
                            <WorkerStartForm
                                connection_id=id
                                supports_file_sink=move || server_version.get().supports_file_sink()
                                on_started=move |result| {
                                    set_ping_msg.set(format_start_message(result));
                                    set_show_form.set(false);
                                    reload();
                                }
                                on_cancel=move || set_show_form.set(false)
                            />
                        }
                    })
                }}
            </Show>

            <Show when=move || !ping_msg.get().is_empty()>
                <p class="banner">{move || ping_msg.get()}</p>
            </Show>

            <Suspense fallback=move || view! {
                <p class="muted">{i18n.t(Msg::LoadingWorkers)}</p>
            }>
                {move || {
                    let list = workers.get();
                    if list.is_empty() {
                        view! {
                            <div class="empty">
                                <p>{i18n.t(Msg::NoWorkers)}</p>
                                <p class="muted">{i18n.t(Msg::NoWorkersHint)}</p>
                            </div>
                        }.into_any()
                    } else {
                        view! { <WorkersTable workers=list i18n=i18n on_stop=on_stop.clone() /> }.into_any()
                    }
                }}
            </Suspense>
        </div>
    }
}

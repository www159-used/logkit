use crate::api::get_connection;
use crate::components::{
    persist_flash, Breadcrumb, ConnectionForm, PageHeader, PageHeaderMain, PageShell, PageTitle,
};
use crate::i18n::{use_i18n, Msg};
use crate::model::{ConnectionId, LogendConnection};

use leptos::prelude::*;
use leptos_router::components::A;
use leptos_router::hooks::{use_navigate, use_params_map};

#[component]
pub fn ConnectionEditPage() -> impl IntoView {
    let i18n = use_i18n();
    let navigate = use_navigate();
    let params = use_params_map();
    let connection_id = move || {
        params.with(|p| p.get("id").as_ref().and_then(|raw| ConnectionId::parse(raw)))
    };

    let (initial, set_initial) = signal(None::<LogendConnection>);

    let conn_res = Resource::new(connection_id, |id| async move {
        let Some(id) = id else {
            return Err(leptos::prelude::ServerFnError::ServerError(
                "missing connection id".into(),
            ));
        };
        get_connection(id).await
    });

    Effect::new(move |_| {
        if let Some(Ok(conn)) = conn_res.get() {
            set_initial.set(Some(conn));
        }
    });

    let on_saved = StoredValue::new({
        let navigate = navigate.clone();
        move |_conn: LogendConnection| {
            persist_flash(i18n.t(Msg::ConnectionSaved));
            navigate("/", Default::default());
        }
    });

    let on_cancel = StoredValue::new({
        let navigate = navigate.clone();
        move || navigate("/", Default::default())
    });

    view! {
        <PageShell>
            <PageHeader>
                <PageHeaderMain>
                    <Breadcrumb>
                        <A href="/">{move || i18n.t(Msg::Connections)}</A>
                        " / "
                        {move || i18n.t(Msg::EditConnection)}
                    </Breadcrumb>
                    <PageTitle>{move || i18n.t(Msg::EditConnection)}</PageTitle>
                </PageHeaderMain>
            </PageHeader>

            <Suspense fallback=move || view! {
                <p class="muted">{move || i18n.t(Msg::LoadingConnections)}</p>
            }>
                {move || match conn_res.get() {
                    Some(Ok(_)) => view! {
                        <ConnectionForm
                            initial=initial
                            on_saved=move |conn| on_saved.with_value(|f| f(conn))
                            on_cancel=move || on_cancel.with_value(|f| f())
                        />
                    }.into_any(),
                    Some(Err(e)) => view! {
                        <p class="error">{e.to_string()}</p>
                    }.into_any(),
                    None => view! {
                        <p class="muted">{move || i18n.t(Msg::LoadingConnections)}</p>
                    }.into_any(),
                }}
            </Suspense>
        </PageShell>
    }
}

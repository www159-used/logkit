use crate::components::{persist_flash, Breadcrumb, ConnectionForm, PageHeader, PageHeaderMain, PageShell, PageTitle};
use crate::i18n::{use_i18n, Msg};
use crate::model::LogendConnection;

use leptos::prelude::*;
use leptos_router::components::A;
use leptos_router::hooks::use_navigate;

#[component]
pub fn ConnectionNewPage() -> impl IntoView {
    let i18n = use_i18n();
    let navigate = use_navigate();
    let (initial, _) = signal(None::<LogendConnection>);

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
                        {move || i18n.t(Msg::NewConnection)}
                    </Breadcrumb>
                    <PageTitle>{move || i18n.t(Msg::NewConnection)}</PageTitle>
                </PageHeaderMain>
            </PageHeader>

            <ConnectionForm
                initial=initial
                on_saved=move |conn| on_saved.with_value(|f| f(conn))
                on_cancel=move || on_cancel.with_value(|f| f())
            />
        </PageShell>
    }
}

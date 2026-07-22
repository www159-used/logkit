use crate::api::{
    default_local_socket, new_local_connection, new_remote_connection, save_connection,
};
use crate::i18n::{use_i18n, Msg};
use crate::model::{ConnectionKind, LogendConnection, DEFAULT_LOGEND_PORT};

use leptos::prelude::*;

#[component]
pub fn ConnectionForm(
    initial: ReadSignal<Option<LogendConnection>>,
    on_saved: impl Fn(LogendConnection) + 'static + Clone + Send,
    on_cancel: impl Fn() + 'static + Clone + Send,
) -> impl IntoView {
    let i18n = use_i18n();
    let (name, set_name) = signal(String::new());
    let (kind, set_kind) = signal(ConnectionKind::Local);
    let (socket, set_socket) = signal(String::new());
    let (host, set_host) = signal(String::new());
    let (port, set_port) = signal(DEFAULT_LOGEND_PORT);
    let (notes, set_notes) = signal(String::new());
    let (id, set_id) = signal(None::<crate::model::ConnectionId>);
    let (error, set_error) = signal(String::new());
    let socket_placeholder = Resource::new(|| (), |_| default_local_socket());

    Effect::new(move |_| {
        if let Some(c) = initial.get() {
            set_id.set(Some(c.id));
            set_name.set(c.name.clone());
            set_kind.set(c.kind);
            set_socket.set(c.socket.clone());
            set_host.set(c.host.clone());
            set_port.set(if c.port == 0 {
                DEFAULT_LOGEND_PORT
            } else {
                c.port
            });
            set_notes.set(c.notes.clone());
        } else {
            set_id.set(None);
            set_name.set(String::new());
            set_kind.set(ConnectionKind::Local);
            set_socket.set(String::new());
            set_host.set(String::new());
            set_port.set(DEFAULT_LOGEND_PORT);
            set_notes.set(String::new());
        }
    });

    let on_submit = {
        let on_saved = on_saved.clone();
        move |ev: leptos::ev::SubmitEvent| {
            ev.prevent_default();
            set_error.set(String::new());
            let on_saved = on_saved.clone();
            leptos::task::spawn_local(async move {
                let conn = if id.get().is_none() {
                    match kind.get() {
                        ConnectionKind::Local => match new_local_connection(name.get()).await {
                            Ok(c) => c,
                            Err(e) => {
                                set_error.set(e.to_string());
                                return;
                            }
                        },
                        ConnectionKind::Remote => {
                            match new_remote_connection(name.get(), host.get(), port.get()).await
                            {
                                Ok(c) => c,
                                Err(e) => {
                                    set_error.set(e.to_string());
                                    return;
                                }
                            }
                        }
                    }
                } else {
                    let existing = initial.get();
                    LogendConnection {
                        id: id.get().expect("edit mode"),
                        name: name.get(),
                        kind: kind.get(),
                        socket: socket.get(),
                        host: host.get(),
                        port: port.get(),
                        defaults_file: existing
                            .as_ref()
                            .map(|c| c.defaults_file.clone())
                            .unwrap_or_default(),
                        auto_kafka_protocol: existing.and_then(|c| c.auto_kafka_protocol),
                        notes: notes.get(),
                    }
                };

                match save_connection(conn).await {
                    Ok(saved) => on_saved(saved),
                    Err(e) => set_error.set(e.to_string()),
                }
            });
        }
    };

    view! {
        <form class="form card" on:submit=on_submit>
            <h2 class="form-title">
                {move || {
                    if id.get().is_none() {
                        i18n.t(Msg::NewConnection)
                    } else {
                        i18n.t(Msg::EditConnection)
                    }
                }}
            </h2>

            <label class="field">
                <span>{move || i18n.t(Msg::Name)}</span>
                <input
                    type="text"
                    prop:value=move || name.get()
                    on:input=move |ev| set_name.set(event_target_value(&ev))
                    required
                    placeholder="dev / root_132"
                />
            </label>

            <fieldset class="field">
                <span>{move || i18n.t(Msg::Kind)}</span>
                <label class="radio">
                    <input
                        type="radio"
                        name="kind"
                        prop:checked=move || kind.get() == ConnectionKind::Local
                        on:change=move |_| set_kind.set(ConnectionKind::Local)
                    />
                    {move || i18n.t(Msg::LocalUnix)}
                </label>
                <label class="radio">
                    <input
                        type="radio"
                        name="kind"
                        prop:checked=move || kind.get() == ConnectionKind::Remote
                        on:change=move |_| set_kind.set(ConnectionKind::Remote)
                    />
                    {move || i18n.t(Msg::RemoteTcp)}
                </label>
            </fieldset>

            <Show when=move || kind.get() == ConnectionKind::Local>
                <label class="field">
                    <span>{move || i18n.t(Msg::SocketOptional)}</span>
                    <input
                        type="text"
                        prop:value=move || socket.get()
                        on:input=move |ev| set_socket.set(event_target_value(&ev))
                        prop:placeholder=move || {
                            socket_placeholder
                                .get()
                                .and_then(|r| r.ok())
                                .unwrap_or_else(|| "$HOME/.logkit/logend.sock".into())
                        }
                    />
                </label>
            </Show>

            <Show when=move || kind.get() == ConnectionKind::Remote>
                <label class="field">
                    <span>{move || i18n.t(Msg::Host)}</span>
                    <input
                        type="text"
                        prop:value=move || host.get()
                        on:input=move |ev| set_host.set(event_target_value(&ev))
                        placeholder="192.168.1.132"
                        required=move || kind.get() == ConnectionKind::Remote
                    />
                </label>
                <label class="field">
                    <span>{move || i18n.t(Msg::Port)}</span>
                    <input
                        type="number"
                        prop:value=move || port.get().to_string()
                        on:input=move |ev| {
                            if let Ok(n) = event_target_value(&ev).parse() {
                                set_port.set(n);
                            }
                        }
                    />
                </label>
            </Show>

            <label class="field">
                <span>{move || i18n.t(Msg::Notes)}</span>
                <input
                    type="text"
                    prop:value=move || notes.get()
                    on:input=move |ev| set_notes.set(event_target_value(&ev))
                />
            </label>

            <Show when=move || !error.get().is_empty()>
                <p class="error">{move || error.get()}</p>
            </Show>

            <div class="actions">
                <button type="submit" class="btn btn-primary">
                    {move || i18n.t(Msg::Save)}
                </button>
                <button type="button" class="btn" on:click=move |_| on_cancel()>
                    {move || i18n.t(Msg::Cancel)}
                </button>
            </div>
        </form>
    }
}

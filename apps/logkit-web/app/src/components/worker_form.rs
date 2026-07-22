use crate::api::start_connection_worker;
use crate::i18n::{use_i18n, Msg};
use crate::model::{ConnectionId, StartWorkerResult, WorkerSinkKind, WorkerStartForm, BODY_PRESETS};

use leptos::prelude::*;

fn format_worker_start_error(raw: &str) -> String {
    if raw.contains("unknown builtin `file_sink`") {
        "当前 logend 版本过旧，不支持 file_sink。请在 logend 所在机器用本仓库重新编译并重启 logend（cargo build -p logend --release）。".into()
    } else {
        raw.to_string()
    }
}

#[component]
pub fn WorkerStartForm(
    connection_id: ConnectionId,
    #[prop(into)] supports_file_sink: Signal<bool>,
    on_started: impl Fn(StartWorkerResult) + 'static + Clone + Send,
    on_cancel: impl Fn() + 'static + Clone + Send,
) -> impl IntoView {
    let i18n = use_i18n();
    let (body_preset, set_body_preset) = signal(BODY_PRESETS[0].to_string());
    let (sink_kind, set_sink_kind) = signal(WorkerSinkKind::Stdout);
    let (file_output, set_file_output) = signal(String::new());
    let (file_max_size, set_file_max_size) = signal(String::new());
    let (kafka_topic, set_kafka_topic) = signal(String::new());
    let (kafka_brokers, set_kafka_brokers) = signal(String::new());
    let (rate, set_rate) = signal("1ms".to_string());
    let (threads, set_threads) = signal(String::new());
    let (label, set_label) = signal(String::new());
    let (error, set_error) = signal(String::new());
    let (submitting, set_submitting) = signal(false);

    let on_submit = {
        let on_started = on_started.clone();
        move |ev: leptos::ev::SubmitEvent| {
            ev.prevent_default();
            if submitting.get() {
                return;
            }
            set_error.set(String::new());
            set_submitting.set(true);
            let on_started = on_started.clone();
            let form = WorkerStartForm {
                body_preset: body_preset.get(),
                sink_kind: sink_kind.get(),
                rate: rate.get(),
                threads: if threads.get().trim().is_empty() {
                    None
                } else {
                    match threads.get().trim().parse() {
                        Ok(n) => Some(n),
                        Err(_) => {
                            set_error.set("threads must be a positive integer".into());
                            set_submitting.set(false);
                            return;
                        }
                    }
                },
                label: label.get(),
            };
            leptos::task::spawn_local(async move {
                match start_connection_worker(connection_id, form).await {
                    Ok(result) => on_started(result),
                    Err(e) => set_error.set(format_worker_start_error(&e.to_string())),
                }
                set_submitting.set(false);
            });
        }
    };

    view! {
        <form class="form card worker-flow" on:submit=on_submit>
            <h2 class="form-title">{move || i18n.t(Msg::NewWorker)}</h2>

            <section class="flow-section">
                <h3 class="flow-heading">{move || i18n.t(Msg::FlowBody)}</h3>
                <label class="field">
                    <span>{move || i18n.t(Msg::BodyPreset)}</span>
                    <select
                        prop:value=move || body_preset.get()
                        on:change=move |ev| set_body_preset.set(event_target_value(&ev))
                    >
                        {BODY_PRESETS.iter().map(|name| {
                            let name = (*name).to_string();
                            view! { <option value=name.clone()>{name.clone()}</option> }
                        }).collect_view()}
                    </select>
                </label>
            </section>

            <section class="flow-section">
                <h3 class="flow-heading">{move || i18n.t(Msg::FlowSink)}</h3>
                <fieldset class="field">
                    <span>{move || i18n.t(Msg::SinkType)}</span>
                    <label class="radio">
                        <input
                            type="radio"
                            name="sink"
                            prop:checked=move || matches!(sink_kind.get(), WorkerSinkKind::Stdout)
                            on:change=move |_| set_sink_kind.set(WorkerSinkKind::Stdout)
                        />
                        {move || i18n.t(Msg::SinkStdout)}
                    </label>
                    <label class="radio">
                        <input
                            type="radio"
                            name="sink"
                            prop:disabled=move || !supports_file_sink.get()
                            prop:checked=move || matches!(sink_kind.get(), WorkerSinkKind::File { .. })
                            on:change=move |_| {
                                if !supports_file_sink.get() {
                                    return;
                                }
                                set_sink_kind.set(WorkerSinkKind::File {
                                    output: file_output.get(),
                                    max_size: file_max_size.get(),
                                })
                            }
                        />
                        {move || i18n.t(Msg::SinkFile)}
                    </label>
                    <Show when=move || !supports_file_sink.get()>
                        <p class="muted">{move || i18n.t(Msg::SinkFileRequiresMinLogend)}</p>
                    </Show>
                    <label class="radio">
                        <input
                            type="radio"
                            name="sink"
                            prop:checked=move || matches!(sink_kind.get(), WorkerSinkKind::Kafka { .. })
                            on:change=move |_| {
                                set_sink_kind.set(WorkerSinkKind::Kafka {
                                    topic: kafka_topic.get(),
                                    brokers: kafka_brokers.get(),
                                })
                            }
                        />
                        {move || i18n.t(Msg::SinkKafka)}
                    </label>
                </fieldset>
                <Show when=move || matches!(sink_kind.get(), WorkerSinkKind::File { .. })>
                    <label class="field">
                        <span>{move || i18n.t(Msg::FileOutput)}</span>
                        <input
                            type="text"
                            prop:value=move || file_output.get()
                            on:input=move |ev| {
                                let output = event_target_value(&ev);
                                set_file_output.set(output.clone());
                                set_sink_kind.set(WorkerSinkKind::File {
                                    output,
                                    max_size: file_max_size.get(),
                                });
                            }
                            placeholder="/var/log/logkit/out.log"
                        />
                    </label>
                    <label class="field">
                        <span>{move || i18n.t(Msg::FileMaxSize)}</span>
                        <input
                            type="text"
                            prop:value=move || file_max_size.get()
                            on:input=move |ev| {
                                let max_size = event_target_value(&ev);
                                set_file_max_size.set(max_size.clone());
                                set_sink_kind.set(WorkerSinkKind::File {
                                    output: file_output.get(),
                                    max_size,
                                });
                            }
                            placeholder="64MiB"
                        />
                    </label>
                </Show>
                <Show when=move || matches!(sink_kind.get(), WorkerSinkKind::Kafka { .. })>
                    <label class="field">
                        <span>{move || i18n.t(Msg::KafkaTopic)}</span>
                        <input
                            type="text"
                            prop:value=move || kafka_topic.get()
                            on:input=move |ev| {
                                let topic = event_target_value(&ev);
                                set_kafka_topic.set(topic.clone());
                                set_sink_kind.set(WorkerSinkKind::Kafka {
                                    topic,
                                    brokers: kafka_brokers.get(),
                                });
                            }
                            required=move || matches!(sink_kind.get(), WorkerSinkKind::Kafka { .. })
                            placeholder="logs"
                        />
                    </label>
                    <label class="field">
                        <span>{move || i18n.t(Msg::KafkaBrokers)}</span>
                        <input
                            type="text"
                            prop:value=move || kafka_brokers.get()
                            on:input=move |ev| {
                                let brokers = event_target_value(&ev);
                                set_kafka_brokers.set(brokers.clone());
                                set_sink_kind.set(WorkerSinkKind::Kafka {
                                    topic: kafka_topic.get(),
                                    brokers,
                                });
                            }
                            placeholder="127.0.0.1:9092"
                        />
                    </label>
                </Show>
            </section>

            <section class="flow-section">
                <h3 class="flow-heading">{move || i18n.t(Msg::FlowOptions)}</h3>
                <label class="field">
                    <span>{move || i18n.t(Msg::Rate)}</span>
                    <input
                        type="text"
                        prop:value=move || rate.get()
                        on:input=move |ev| set_rate.set(event_target_value(&ev))
                        placeholder="1ms"
                    />
                </label>
                <label class="field">
                    <span>{move || i18n.t(Msg::ThreadsOptional)}</span>
                    <input
                        type="number"
                        prop:value=move || threads.get()
                        on:input=move |ev| set_threads.set(event_target_value(&ev))
                        min="1"
                        placeholder="1"
                    />
                </label>
                <label class="field">
                    <span>{move || i18n.t(Msg::ScriptLabel)}</span>
                    <input
                        type="text"
                        prop:value=move || label.get()
                        on:input=move |ev| set_label.set(event_target_value(&ev))
                        placeholder="dev / stdout"
                    />
                </label>
            </section>

            <Show when=move || !error.get().is_empty()>
                <p class="error">{move || error.get()}</p>
            </Show>

            <div class="actions">
                <button type="submit" class="btn btn-primary" disabled=move || submitting.get()>
                    {move || i18n.t(Msg::StartWorker)}
                </button>
                <button type="button" class="btn" on:click=move |_| on_cancel()>
                    {move || i18n.t(Msg::Cancel)}
                </button>
            </div>
        </form>
    }
}

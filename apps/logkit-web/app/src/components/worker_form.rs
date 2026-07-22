use crate::api::start_connection_worker;
use crate::i18n::{use_i18n, Msg};
use crate::model::{
    ConnectionId, KafkaWebAgentFormat, KafkaWebMode, StartWorkerResult, WorkerSinkKind,
    WorkerStartForm, BODY_PRESETS, default_kafka_sink,
};

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
    let (kafka_mode, set_kafka_mode) = signal(KafkaWebMode::Common);
    let (kafka_topic, set_kafka_topic) = signal(String::new());
    let (kafka_brokers, set_kafka_brokers) = signal(String::new());
    let (kafka_agent_format, set_kafka_agent_format) = signal(KafkaWebAgentFormat::Json);
    let (kafka_source_id, set_kafka_source_id) = signal(String::new());
    let (kafka_appname, set_kafka_appname) = signal(String::new());
    let (kafka_tag, set_kafka_tag) = signal("ww".to_string());
    let (kafka_domain, set_kafka_domain) = signal(String::new());
    let (rate, set_rate) = signal("1s".to_string());
    let (threads, set_threads) = signal(String::new());
    let (label, set_label) = signal(String::new());
    let (error, set_error) = signal(String::new());
    let (submitting, set_submitting) = signal(false);

    let kafka_sink_from_fields = move || WorkerSinkKind::Kafka {
        mode: kafka_mode.get(),
        topic: kafka_topic.get(),
        brokers: kafka_brokers.get(),
        agent_format: kafka_agent_format.get(),
        source_id: kafka_source_id.get(),
        appname: kafka_appname.get(),
        tag: kafka_tag.get(),
        domain: kafka_domain.get(),
    };

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
            let sink_kind = match sink_kind.get() {
                WorkerSinkKind::Kafka { .. } => kafka_sink_from_fields(),
                other => other,
            };
            let form = WorkerStartForm {
                body_preset: body_preset.get(),
                sink_kind,
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
                                set_sink_kind.set(default_kafka_sink());
                                set_kafka_mode.set(KafkaWebMode::Common);
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
                    <fieldset class="field">
                        <span>{move || i18n.t(Msg::KafkaMode)}</span>
                        <label class="radio">
                            <input
                                type="radio"
                                name="kafka_mode"
                                prop:checked=move || kafka_mode.get() == KafkaWebMode::Common
                                on:change=move |_| {
                                    set_kafka_mode.set(KafkaWebMode::Common);
                                    set_sink_kind.set(kafka_sink_from_fields());
                                }
                            />
                            {move || i18n.t(Msg::KafkaModeCommon)}
                        </label>
                        <label class="radio">
                            <input
                                type="radio"
                                name="kafka_mode"
                                prop:checked=move || kafka_mode.get() == KafkaWebMode::Agent
                                on:change=move |_| {
                                    set_kafka_mode.set(KafkaWebMode::Agent);
                                    set_sink_kind.set(kafka_sink_from_fields());
                                }
                            />
                            {move || i18n.t(Msg::KafkaModeAgent)}
                        </label>
                    </fieldset>

                    <Show when=move || kafka_mode.get() == KafkaWebMode::Common>
                        <label class="field">
                            <span>{move || i18n.t(Msg::KafkaTopic)}</span>
                            <input
                                type="text"
                                prop:value=move || kafka_topic.get()
                                on:input=move |ev| {
                                    set_kafka_topic.set(event_target_value(&ev));
                                    set_sink_kind.set(kafka_sink_from_fields());
                                }
                                placeholder="logs"
                            />
                        </label>
                    </Show>

                    <label class="field">
                        <span>{move || i18n.t(Msg::KafkaBrokers)}</span>
                        <input
                            type="text"
                            prop:value=move || kafka_brokers.get()
                            on:input=move |ev| {
                                set_kafka_brokers.set(event_target_value(&ev));
                                set_sink_kind.set(kafka_sink_from_fields());
                            }
                            required=move || matches!(sink_kind.get(), WorkerSinkKind::Kafka { .. })
                            placeholder="127.0.0.1:9092"
                        />
                    </label>

                    <Show when=move || kafka_mode.get() == KafkaWebMode::Agent>
                        <label class="field">
                            <span>{move || i18n.t(Msg::KafkaAgentFormat)}</span>
                            <select
                                prop:value=move || match kafka_agent_format.get() {
                                    KafkaWebAgentFormat::Json => "json",
                                    KafkaWebAgentFormat::Pb => "pb",
                                }
                                on:change=move |ev| {
                                    set_kafka_agent_format.set(match event_target_value(&ev).as_str() {
                                        "pb" => KafkaWebAgentFormat::Pb,
                                        _ => KafkaWebAgentFormat::Json,
                                    });
                                    set_sink_kind.set(kafka_sink_from_fields());
                                }
                            >
                                <option value="json">{move || i18n.t(Msg::KafkaAgentFormatJson)}</option>
                                <option value="pb">{move || i18n.t(Msg::KafkaAgentFormatPb)}</option>
                            </select>
                        </label>
                        <label class="field">
                            <span>{move || i18n.t(Msg::KafkaAgentSourceId)}</span>
                            <input
                                type="text"
                                prop:value=move || kafka_source_id.get()
                                on:input=move |ev| {
                                    set_kafka_source_id.set(event_target_value(&ev));
                                    set_sink_kind.set(kafka_sink_from_fields());
                                }
                                placeholder="550e8400-e29b-41d4-a716-446655440000"
                            />
                        </label>
                        <label class="field">
                            <span>{move || i18n.t(Msg::KafkaAgentAppname)}</span>
                            <input
                                type="text"
                                prop:value=move || kafka_appname.get()
                                on:input=move |ev| {
                                    set_kafka_appname.set(event_target_value(&ev));
                                    set_sink_kind.set(kafka_sink_from_fields());
                                }
                            />
                        </label>
                        <label class="field">
                            <span>{move || i18n.t(Msg::KafkaAgentTag)}</span>
                            <input
                                type="text"
                                prop:value=move || kafka_tag.get()
                                on:input=move |ev| {
                                    set_kafka_tag.set(event_target_value(&ev));
                                    set_sink_kind.set(kafka_sink_from_fields());
                                }
                            />
                        </label>
                        <label class="field">
                            <span>{move || i18n.t(Msg::KafkaAgentDomain)}</span>
                            <input
                                type="text"
                                prop:value=move || kafka_domain.get()
                                on:input=move |ev| {
                                    set_kafka_domain.set(event_target_value(&ev));
                                    set_sink_kind.set(kafka_sink_from_fields());
                                }
                            />
                        </label>
                    </Show>
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
                        placeholder="1s"
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

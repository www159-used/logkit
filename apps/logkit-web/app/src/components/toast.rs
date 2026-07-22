use leptos::prelude::*;

pub const FLASH_STORAGE_KEY: &str = "logkit-flash";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToastKind {
    Success,
    Error,
}

#[derive(Debug, Clone, PartialEq)]
struct ToastMessage {
    text: String,
    kind: ToastKind,
}

#[derive(Clone)]
pub struct Toast {
    active: ReadSignal<Option<ToastMessage>>,
    set_active: WriteSignal<Option<ToastMessage>>,
}

impl Toast {
    pub fn show(&self, text: impl Into<String>, kind: ToastKind) {
        self.set_active.set(Some(ToastMessage {
            text: text.into(),
            kind,
        }));
    }

    pub fn success(&self, text: impl Into<String>) {
        self.show(text, ToastKind::Success);
    }

    pub fn error(&self, text: impl Into<String>) {
        self.show(text, ToastKind::Error);
    }

    pub fn dismiss(&self) {
        self.set_active.set(None);
    }
}

pub fn provide_toast() -> Toast {
    let (active, set_active) = signal(None::<ToastMessage>);
    let toast = Toast {
        active,
        set_active,
    };
    provide_context(toast.clone());
    toast
}

pub fn use_toast() -> Toast {
    expect_context::<Toast>()
}

pub fn persist_flash(text: &str) {
    crate::browser_storage::write(FLASH_STORAGE_KEY, text);
}

/// 轮询/Resource 失败时避免相同错误文案重复弹出 toast。
pub fn toast_resource_error(
    toast: &Toast,
    last: &StoredValue<Option<String>>,
    err: impl ToString,
) {
    let msg = err.to_string();
    if last.with_value(|s| s.as_deref() == Some(msg.as_str())) {
        return;
    }
    last.set_value(Some(msg.clone()));
    toast.error(msg);
}

pub fn clear_toast_resource_error(last: &StoredValue<Option<String>>) {
    last.set_value(None);
}

#[component]
pub fn ToastHost() -> impl IntoView {
    let toast = use_toast();
    let (active, set_active) = (toast.active, toast.set_active);

    Effect::new(move |_| {
        if let Some(msg) = crate::browser_storage::read(FLASH_STORAGE_KEY) {
            if !msg.is_empty() {
                set_active.set(Some(ToastMessage {
                    text: msg,
                    kind: ToastKind::Success,
                }));
                crate::browser_storage::write(FLASH_STORAGE_KEY, "");
            }
        }
    });

    let dismiss_epoch = StoredValue::new(0u64);

    Effect::new(move |prev: Option<Option<ToastMessage>>| {
        let current = active.get();
        let prev_msg = prev.flatten();
        let is_new = match (&prev_msg, &current) {
            (_, None) => false,
            (None, Some(_)) => true,
            (Some(old), Some(new)) => old.text != new.text || old.kind != new.kind,
        };
        if is_new {
            dismiss_epoch.update_value(|e| *e += 1);
            #[cfg(feature = "hydrate")]
            {
                let epoch = dismiss_epoch.get_value();
                let set_active = set_active.clone();
                leptos::leptos_dom::helpers::set_timeout(
                    move || {
                        if dismiss_epoch.get_value() == epoch {
                            set_active.set(None);
                        }
                    },
                    std::time::Duration::from_secs(5),
                );
            }
        }
        current
    });

    view! {
        <div class="toast-host" role="status" aria-live="polite">
            <Show when=move || active.get().is_some()>
                {move || {
                    let msg = active.get().unwrap();
                    let kind_class = match msg.kind {
                        ToastKind::Success => "toast toast-success",
                        ToastKind::Error => "toast toast-error",
                    };
                    view! {
                        <div class=kind_class>
                            <p class="toast-text">{msg.text}</p>
                            <button
                                type="button"
                                class="toast-close"
                                aria-label="Close"
                                on:click=move |_| set_active.set(None)
                            >
                                "×"
                            </button>
                        </div>
                    }
                }}
            </Show>
        </div>
    }
}

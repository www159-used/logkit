//! hydrate 下周期性 tick，间隔随设置变化。

use leptos::prelude::*;

#[cfg(feature = "hydrate")]
pub fn use_poll_tick(interval_ms: ReadSignal<u32>) -> ReadSignal<u32> {
    use wasm_bindgen::prelude::*;
    use wasm_bindgen::JsCast;

    let (tick, set_tick) = signal(0u32);
    Effect::new(move |_| {
        let ms = interval_ms.get().max(500);
        let Some(window) = web_sys::window() else {
            return;
        };
        let closure = Closure::<dyn FnMut()>::wrap(Box::new(move || {
            set_tick.update(|n| *n += 1);
        }));
        let id = window
            .set_interval_with_callback_and_timeout_and_arguments_0(
                closure.as_ref().unchecked_ref(),
                ms as i32,
            )
            .expect("setInterval");
        closure.forget();
        on_cleanup(move || {
            window.clear_interval_with_handle(id);
        });
    });
    tick
}

#[cfg(not(feature = "hydrate"))]
pub fn use_poll_tick(_interval_ms: ReadSignal<u32>) -> ReadSignal<u32> {
    signal(0u32).0
}

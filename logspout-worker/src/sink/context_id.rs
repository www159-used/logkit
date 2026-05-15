//! 与 Java `ContextIDGenerator`（Yottabyte collector）等价的 **`context_id` 生成器**。
//!
//! **进程级单例**：全进程只应通过 [`next_context_id`] 取号；不要为每条连接或每个 sink 单独 `new` 一份，否则会破坏与 Java 侧一致的单调语义。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

const SEQ_MASK: u64 = 0xeffff;

static GLOBAL: OnceLock<ContextIdGenerator> = OnceLock::new();

/// `timestamp_ms * 1_000_000 + (seq & 0xeffff)`，单调不减，防时钟回拨。
struct ContextIdGenerator {
    seq: AtomicU64,
    last_ts: Mutex<i64>,
}

impl ContextIdGenerator {
    fn new() -> Self {
        let now = now_ms_i64();
        Self {
            seq: AtomicU64::new(0),
            last_ts: Mutex::new(now),
        }
    }

    fn next(&self) -> i64 {
        let inc = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let sequence_id = (inc & SEQ_MASK) as i64;

        let mut ts = now_ms_i64();
        let mut guard = match self.last_ts.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::warn!("context_id last_ts mutex poisoned; recovering inner state");
                poisoned.into_inner()
            }
        };
        ts = ts.max(*guard);
        if sequence_id == 0 {
            ts = ts.saturating_add(1);
        }
        *guard = ts;
        drop(guard);

        ts.saturating_mul(1_000_000).saturating_add(sequence_id)
    }
}

fn global() -> &'static ContextIdGenerator {
    GLOBAL.get_or_init(ContextIdGenerator::new)
}

/// 下一个 `context_id`（**全进程唯一序列**，与 Java `ContextIDGenerator` 对齐）。
pub(crate) fn next_context_id() -> i64 {
    global().next()
}

fn now_ms_i64() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_ids_monotonic_for_many_calls() {
        let mut prev = next_context_id();
        for _ in 0..10_000 {
            let n = next_context_id();
            assert!(n >= prev, "not monotonic: {prev} -> {n}");
            prev = n;
        }
    }
}

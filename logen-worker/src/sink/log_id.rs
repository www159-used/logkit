//! 时间有序 `log_id` 生成（仿插件里的 time-based base64 UUID 形态）。

use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::STANDARD_NO_PAD;
use base64::Engine as _;
use uuid::Uuid;

#[derive(Debug)]
struct TimeBasedLogIdGenerator {
    sequence_number: u32,
    last_timestamp_ms: u64,
    munged_node: [u8; 6],
}

pub(crate) fn wall_clock_ms_u64() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis() as u64,
        Err(e) => {
            tracing::warn!("system clock before unix epoch while generating log_id: {e}");
            0
        }
    }
}

fn put_lower_bytes_be(out: &mut [u8], value: u64) {
    for (i, b) in out.iter_mut().rev().enumerate() {
        *b = (value >> (i * 8)) as u8;
    }
}

impl TimeBasedLogIdGenerator {
    fn new() -> Self {
        let seed = Uuid::new_v4();
        let s = seed.as_bytes();
        let mut munged_node = [0u8; 6];
        munged_node.copy_from_slice(&s[..6]);
        for (i, b) in munged_node.iter_mut().enumerate() {
            *b ^= s[10 + i];
        }
        let sequence_number = u32::from_be_bytes([s[12], s[13], s[14], s[15]]) & 0x00ff_ffff;
        Self {
            sequence_number,
            last_timestamp_ms: 0,
            munged_node,
        }
    }

    fn next_log_id(&mut self) -> String {
        self.sequence_number = self.sequence_number.wrapping_add(1) & 0x00ff_ffff;
        let mut timestamp = wall_clock_ms_u64();
        timestamp = timestamp.max(self.last_timestamp_ms);
        if self.sequence_number == 0 {
            timestamp = timestamp.saturating_add(1);
        }
        self.last_timestamp_ms = timestamp;

        let mut bytes = [0u8; 15];
        put_lower_bytes_be(&mut bytes[..6], timestamp);
        bytes[6..12].copy_from_slice(&self.munged_node);
        put_lower_bytes_be(&mut bytes[12..15], self.sequence_number as u64);
        STANDARD_NO_PAD.encode(bytes)
    }
}

pub(crate) fn next_log_id() -> String {
    static GEN: OnceLock<Mutex<TimeBasedLogIdGenerator>> = OnceLock::new();
    let g = GEN.get_or_init(|| Mutex::new(TimeBasedLogIdGenerator::new()));
    let mut guard = match g.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!("time-based log_id generator mutex poisoned; recovering inner state");
            poisoned.into_inner()
        }
    };
    guard.next_log_id()
}

#[cfg(test)]
mod tests {
    use super::next_log_id;

    /// 测试内容：时间有序 `log_id` 生成器应产出固定长度、无 padding 的标准 Base64 风格字符串。
    /// 输入：连续调用两次 `next_log_id()`。
    /// 预期：两次结果长度均为 `20`、值不相同，且字符集仅包含标准 Base64 字符。
    #[test]
    fn generated_log_id_is_20_char_base64ish() {
        let a = next_log_id();
        let b = next_log_id();
        assert_eq!(a.len(), 20);
        assert_eq!(b.len(), 20);
        assert_ne!(a, b);
        assert!(a
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '/'));
    }
}

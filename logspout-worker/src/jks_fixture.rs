//! 仓库内 **`logspout-worker/assets/keystore.jks`**、**`truststore.jks`** 的口令与默认 bootstrap。
//! 仅由 [`crate::kafka_smoke`] 与单测引用；与 `tests/fixtures/kafka_asset_broker.yaml` 中口令保持一致。

/// 与 `logspout-worker/assets/keystore.jks` 匹配的 fixture 口令。
pub const FIXTURE_KEYSTORE_PASSWORD: &str =
    "8c4804e1504aa139bd827c9c016f11d4cc7174a95352f5068a3cb2c1f4849e91";
/// 与 `logspout-worker/assets/truststore.jks` 匹配的 fixture 口令。
pub const FIXTURE_TRUSTSTORE_PASSWORD: &str = "vKFoWrbf_El1pCtcUVHZn0ygI5Mu8izQ";
/// 默认 fixture 连的 Kafka bootstrap（单测 `#[ignore]` 与 probe 二进制默认一致）。
pub const FIXTURE_BOOTSTRAP_BROKER: &str = "192.168.1.60:9092";

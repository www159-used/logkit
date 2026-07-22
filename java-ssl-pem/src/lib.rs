//! **`.jks`** truststore / keystore → **内存 PEM 文本**。
//!
//! 供支持 `ssl.ca.pem` / `ssl.certificate.pem` / `ssl.key.pem` 的 TLS 栈使用（**不依赖 Kafka / librdkafka**）。
//!
//! 唯一入口：[`materialize_java_ssl_pem`]。
//!
//! - 自研 JKS 解析，不调用本机 `openssl`。
//! - **不支持** PEM/DER 输入，也不支持 `.p12` / `.pfx`。
//! - 多私钥 keystore：显式 `key_alias` 优先；否则自动选 `agent`；无 `agent` 则报错。
//! - 仅一条私钥时可省略 alias。

use std::path::Path;

mod error;
mod jks;
mod keystore;
mod pem;

pub use error::JavaSslPemError;

/// JKS truststore（信任锚）。
#[derive(Debug, Clone, Copy)]
pub struct JksTruststore<'a> {
    pub path: &'a str,
    pub password: &'a str,
}

/// JKS client keystore（mTLS 身份）。
#[derive(Debug, Clone, Copy)]
pub struct JksKeystore<'a> {
    pub path: &'a str,
    pub password: &'a str,
    pub key_alias: Option<&'a str>,
}

/// TLS 材料：trust / identity 各自可选，均为 JKS。
#[derive(Debug, Clone, Copy, Default)]
pub struct JavaSslMaterial<'a> {
    pub trust: Option<JksTruststore<'a>>,
    pub identity: Option<JksKeystore<'a>>,
}

/// [`materialize_java_ssl_pem`] 的输出（UTF-8 PEM）。
#[derive(Debug, Default)]
pub struct MaterializedSslPem {
    pub ca_pem: Option<String>,
    pub certificate_pem: Option<String>,
    pub key_pem: Option<String>,
}

/// 解析 JKS 私钥别名：显式 `key_alias` → 单私钥 → 多私钥优先 `agent` → 否则报错。
pub fn resolve_jks_key_alias(
    path: &Path,
    password: &str,
    key_alias: Option<&str>,
) -> Result<String, JavaSslPemError> {
    keystore::resolve_key_alias(path, password, key_alias)
}

/// 将 [`JavaSslMaterial`]（JKS）解析为 PEM 文本。
pub fn materialize_java_ssl_pem(
    m: &JavaSslMaterial<'_>,
) -> Result<MaterializedSslPem, JavaSslPemError> {
    let ca_pem = m
        .trust
        .as_ref()
        .map(|t| keystore::truststore_to_ca_pem(Path::new(t.path), t.password))
        .transpose()?;
    let pair = m
        .identity
        .as_ref()
        .map(|id| {
            keystore::client_keystore_to_pem(Path::new(id.path), id.password, id.key_alias)
        })
        .transpose()?;
    let (certificate_pem, key_pem) = match pair {
        Some((c, k)) => (Some(c), Some(k)),
        None => (None, None),
    };
    Ok(MaterializedSslPem {
        ca_pem,
        certificate_pem,
        key_pem,
    })
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::path::{Path, PathBuf};

    use super::*;

    fn fixture_assets() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
    }

    /// 与 `logen-worker/tests/fixtures/kafka_asset_broker.yaml` 中口令一致。
    const FIXTURE_TRUSTSTORE_PASSWORD: &str = "vKFoWrbf_El1pCtcUVHZn0ygI5Mu8izQ";
    const FIXTURE_KEYSTORE_PASSWORD: &str =
        "8c4804e1504aa139bd827c9c016f11d4cc7174a95352f5068a3cb2c1f4849e91";

    /// 测试内容：`truststore.jks` / `keystore.jks` 魔数。
    /// 输入：`tests/fixtures/` 下两个 JKS fixture。
    /// 预期：文件头为 `FEEDFEED`。
    #[test]
    fn assets_jks_magic() {
        for name in ["keystore.jks", "truststore.jks"] {
            let path = fixture_assets().join(name);
            assert!(path.is_file(), "missing fixture {}", path.display());
            let mut f = std::fs::File::open(&path).unwrap();
            let mut b = [0u8; 4];
            f.read_exact(&mut b).unwrap();
            assert_eq!(b, [0xFE, 0xED, 0xFE, 0xED], "{}", path.display());
        }
    }

    /// 测试内容：仅 truststore 经主入口导出 CA PEM。
    /// 输入：`JksTruststore` + fixture 口令。
    /// 预期：`ca_pem` 含 CERTIFICATE 块。
    #[test]
    fn materialize_jks_trust_only() {
        let trust_p = fixture_assets()
            .join("truststore.jks")
            .to_string_lossy()
            .into_owned();
        assert!(Path::new(&trust_p).is_file());
        let out = materialize_java_ssl_pem(&JavaSslMaterial {
            trust: Some(JksTruststore {
                path: trust_p.as_str(),
                password: FIXTURE_TRUSTSTORE_PASSWORD,
            }),
            identity: None,
        })
        .expect("materialize trust");
        let ca = out.ca_pem.expect("ca");
        assert!(ca.contains("BEGIN CERTIFICATE") || ca.contains("BEGIN TRUSTED CERTIFICATE"));
        assert!(out.certificate_pem.is_none());
        assert!(out.key_pem.is_none());
    }

    /// 测试内容：JKS trust + JKS identity 经主入口一次 materialize。
    /// 输入：fixture truststore / keystore 与口令；多私钥时指定 `agent`。
    /// 预期：ca / cert / key 均为非空 PEM。
    #[test]
    fn materialize_typed_jks_trust_and_jks_identity() {
        let assets = fixture_assets();
        let trust_p = assets.join("truststore.jks").to_string_lossy().into_owned();
        let key_p = assets.join("keystore.jks").to_string_lossy().into_owned();
        assert!(Path::new(&trust_p).is_file());
        assert!(Path::new(&key_p).is_file());
        let out = materialize_java_ssl_pem(&JavaSslMaterial {
            trust: Some(JksTruststore {
                path: trust_p.as_str(),
                password: FIXTURE_TRUSTSTORE_PASSWORD,
            }),
            identity: Some(JksKeystore {
                path: key_p.as_str(),
                password: FIXTURE_KEYSTORE_PASSWORD,
                key_alias: Some("agent"),
            }),
        })
        .expect("materialize");
        assert!(out.ca_pem.as_ref().unwrap().contains("BEGIN CERTIFICATE"));
        assert!(out
            .certificate_pem
            .as_ref()
            .unwrap()
            .contains("BEGIN CERTIFICATE"));
        let key = out.key_pem.as_ref().unwrap();
        assert!(key.contains("BEGIN PRIVATE KEY") || key.contains("BEGIN RSA PRIVATE KEY"));
    }

    /// 测试内容：多私钥且未指定 alias 时自动选 `agent`。
    /// 输入：`tests/fixtures/keystore.jks`（含 agent 等）。
    /// 预期：`resolve_jks_key_alias` 返回 `agent`（忽略大小写）。
    #[test]
    fn resolve_alias_prefers_agent_when_multiple() {
        let key_p = fixture_assets().join("keystore.jks");
        let alias = resolve_jks_key_alias(&key_p, FIXTURE_KEYSTORE_PASSWORD, None).expect("alias");
        assert_eq!(alias.to_lowercase(), "agent");
    }

    /// 测试内容：多私钥时可显式覆盖自动 `agent`。
    /// 输入：`key_alias=server`。
    /// 预期：返回 `server`。
    #[test]
    fn resolve_alias_honors_explicit() {
        let key_p = fixture_assets().join("keystore.jks");
        let alias =
            resolve_jks_key_alias(&key_p, FIXTURE_KEYSTORE_PASSWORD, Some("server")).expect("alias");
        assert_eq!(alias.to_lowercase(), "server");
    }
}

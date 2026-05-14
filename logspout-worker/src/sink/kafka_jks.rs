//! Kafka：`librdkafka` [`ClientConfig`] 与模板 [`KafkaConfig`] 的 TLS 字段适配。
//!
//! 将平铺的 `ssl.*` 字段映射为 **`java_ssl_pem`** 的 [`TrustMaterial`] / [`IdentityMaterial`]（**PEM / JKS / P12 可独立组合**），再落盘并写入 rdkafka。

use std::path::Path;

use java_ssl_pem::{
    materialize_java_ssl_pem,
    IdentityMaterial, JavaSslMaterial, JavaSslPemError, PemMaterial, TrustMaterial,
};
use logspout_dsl::KafkaConfig;
use rdkafka::config::ClientConfig;
use tempfile::TempDir;

fn path_ext_lower(s: &str) -> String {
    Path::new(s)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase()
}

fn pem_material_from_field(s: &str) -> PemMaterial<'_> {
    let t = s.trim();
    if t.starts_with("-----BEGIN") {
        PemMaterial::Inline(t)
    } else {
        PemMaterial::Path(t)
    }
}

fn trust_from_kafka(k: &KafkaConfig) -> Result<Option<TrustMaterial<'_>>, JavaSslPemError> {
    let ca_pem = k.ssl_ca_pem.as_deref().map(str::trim).filter(|s| !s.is_empty());
    let ca_loc = k.ssl_ca_location.as_deref().map(str::trim).filter(|s| !s.is_empty());
    if ca_pem.is_some() && ca_loc.is_some() {
        return Err(JavaSslPemError::Config {
            detail: "both ssl.ca.pem and ssl.ca.location are set; use only one CA trust source"
                .into(),
        });
    }
    let ts = k
        .ssl_truststore_location
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    if (ca_pem.is_some() || ca_loc.is_some()) && ts.is_some() {
        return Err(JavaSslPemError::Config {
            detail: "set either ssl.ca.pem / ssl.ca.location or ssl.truststore.location, not multiple trust sources"
                .into(),
        });
    }
    if let Some(p) = ca_pem {
        return Ok(Some(TrustMaterial::Pem(pem_material_from_field(p))));
    }
    if let Some(p) = ca_loc {
        return Ok(Some(TrustMaterial::Pem(pem_material_from_field(p))));
    }
    if let Some(p) = ts {
        let ext = path_ext_lower(p);
        let pwd = k.ssl_truststore_password.as_deref().unwrap_or("");
        return Ok(Some(if ext == "jks" {
            TrustMaterial::Jks { path: p, password: pwd }
        } else if ext == "p12" || ext == "pfx" {
            TrustMaterial::P12 { path: p, password: pwd }
        } else {
            TrustMaterial::Pem(PemMaterial::Path(p))
        }));
    }
    Ok(None)
}

fn identity_from_kafka(k: &KafkaConfig) -> Result<Option<IdentityMaterial<'_>>, JavaSslPemError> {
    let cert = k
        .ssl_certificate_pem
        .as_deref()
        .or(k.ssl_certificate_location.as_deref())
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let key = k
        .ssl_private_key_pem
        .as_deref()
        .or(k.ssl_key_location.as_deref())
        .or(k.ssl_key_pem.as_deref())
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let ks = k
        .ssl_keystore_location
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    if (cert.is_some() || key.is_some()) && ks.is_some() {
        return Err(JavaSslPemError::Config {
            detail: "set either PEM certificate/key fields or ssl.keystore.location, not both".into(),
        });
    }
    if cert.is_some() ^ key.is_some() {
        return Err(JavaSslPemError::ClientIdentity {
            detail: "provide both PEM certificate and private key, or ssl.keystore.location for JKS/P12"
                .into(),
        });
    }
    if let (Some(c), Some(ke)) = (cert, key) {
        return Ok(Some(IdentityMaterial::Pem {
            certificate: pem_material_from_field(c),
            private_key: pem_material_from_field(ke),
        }));
    }
    if let Some(p) = ks {
        let ext = path_ext_lower(p);
        let pwd = k.ssl_keystore_password.as_deref().unwrap_or("");
        if ext == "jks" {
            return Ok(Some(IdentityMaterial::Jks {
                path: p,
                password: pwd,
                key_alias: k
                    .ssl_keystore_alias
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty()),
            }));
        }
        if ext == "p12" || ext == "pfx" {
            return Ok(Some(IdentityMaterial::P12 { path: p, password: pwd }));
        }
        return Err(JavaSslPemError::ClientIdentity {
            detail: format!(
                "ssl.keystore.location={p:?}: expected .jks, .p12, or .pfx when not using PEM cert/key fields"
            ),
        });
    }
    Ok(None)
}

/// 与 Java `ssl.endpoint.identification.algorithm` 置空即关闭主机名校验的语义对齐：空串 / `None` → 不校验。
fn kafka_ssl_verify_certificate(k: &KafkaConfig) -> bool {
    match &k.ssl_endpoint_identification_algorithm {
        None => true,
        Some(s) => !s.trim().is_empty(),
    }
}

fn java_ssl_material_from_kafka(k: &KafkaConfig) -> Result<JavaSslMaterial<'_>, JavaSslPemError> {
    Ok(JavaSslMaterial {
        trust: trust_from_kafka(k)?,
        identity: identity_from_kafka(k)?,
    })
}

/// 将 PEM 路径写入 librdkafka（`ssl.ca.location`、`ssl.certificate.location`、`ssl.key.location`）；若从 JKS/P12 转换则返回 [`TempDir`] 须与 producer 同生命周期。
pub(crate) fn configure_librdkafka_ssl(
    cfg: &mut ClientConfig,
    k: &KafkaConfig,
) -> Result<Option<TempDir>, JavaSslPemError> {
    let verify = kafka_ssl_verify_certificate(k);
    let m = java_ssl_material_from_kafka(k)?;
    let out = materialize_java_ssl_pem(&m)?;
    if let Some(ref ca) = out.ca_file {
        cfg.set("ssl.ca.location", ca);
    }
    if let (Some(ref cert), Some(ref key)) = (&out.certificate_file, &out.key_file) {
        cfg.set("ssl.certificate.location", cert);
        cfg.set("ssl.key.location", key);
    }
    cfg.set(
        "enable.ssl.certificate.verification",
        if verify { "true" } else { "false" },
    );
    Ok(out.scratch_dir)
}

#[cfg(test)]
mod kafka_ssl_adapter_tests {
    //! 与 `java-ssl-pem` 单测互补：此处只验证经 [`KafkaConfig`] → librdkafka 的整条路径。

    use std::path::Path;
    use std::path::PathBuf;

    use logspout_dsl::KafkaConfig;
    use rdkafka::config::ClientConfig;

    use super::configure_librdkafka_ssl;
    use crate::kafka_smoke::{
        kafka_config_fixture_jks_dir, FIXTURE_BOOTSTRAP_BROKER, FIXTURE_KEYSTORE_PASSWORD,
        FIXTURE_TRUSTSTORE_PASSWORD,
    };

    fn assets_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("assets")
    }

    fn fixture_kafka_config() -> KafkaConfig {
        kafka_config_fixture_jks_dir(FIXTURE_BOOTSTRAP_BROKER, "fixture", &assets_dir(), false)
    }

    /// 测试内容：fixture JKS 经 `configure_librdkafka_ssl` 写入 `ssl.*.location` 等 librdkafka 配置。
    /// 输入：`kafka_config_fixture_jks_dir`（不跳过主机名校验）；内存 `ClientConfig`。
    /// 预期：`ssl.ca.location` / 证书 / 私钥 路径均为存在的文件；`enable.ssl.certificate.verification` 为 `true`。
    #[test]
    fn configure_librdkafka_ssl_fixture_sets_locations() {
        let k = fixture_kafka_config();
        let mut cfg = ClientConfig::new();
        let tmp = configure_librdkafka_ssl(&mut cfg, &k).expect("configure SSL");
        assert!(tmp.is_some(), "expected temp dir for JKS materialization");
        let m = cfg.config_map();
        let ca = m.get("ssl.ca.location").expect("ssl.ca.location");
        let cert = m.get("ssl.certificate.location").expect("cert location");
        let key = m.get("ssl.key.location").expect("key location");
        assert!(Path::new(ca).is_file(), "ca path: {ca}");
        assert!(Path::new(cert).is_file(), "cert path: {cert}");
        assert!(Path::new(key).is_file(), "key path: {key}");
        assert_eq!(
            m.get("enable.ssl.certificate.verification")
                .map(String::as_str),
            Some("true")
        );
    }

    /// 测试内容：fixture 口令常量与 `jks_fixture` 一致（防漂移）。
    /// 输入：无。
    /// 预期：与 `kafka_smoke` 再导出常量逐字节相同。
    #[test]
    fn fixture_passwords_match_jks_fixture_module() {
        assert_eq!(
            FIXTURE_TRUSTSTORE_PASSWORD,
            crate::jks_fixture::FIXTURE_TRUSTSTORE_PASSWORD
        );
        assert_eq!(
            FIXTURE_KEYSTORE_PASSWORD,
            crate::jks_fixture::FIXTURE_KEYSTORE_PASSWORD
        );
    }
}

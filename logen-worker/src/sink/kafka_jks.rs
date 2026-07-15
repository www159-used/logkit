use std::path::Path;

use java_ssl_pem::{materialize_java_ssl_pem, JavaSslMaterial, JksKeystore, JksTruststore};
use logen_model::KafkaConfig;
use rdkafka::config::ClientConfig;

use super::SinkError;

fn path_ext_lower(s: &str) -> String {
    Path::new(s)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase()
}

fn nonempty(s: Option<&str>) -> Option<&str> {
    s.map(str::trim).filter(|t| !t.is_empty())
}

fn looks_like_inline_pem(s: &str) -> bool {
    s.trim_start().starts_with("-----BEGIN")
}

fn ssl_config_err(detail: impl Into<String>) -> SinkError {
    SinkError::KafkaConfig(super::KafkaConfigError::new(detail))
}

/// 与 Java `ssl.endpoint.identification.algorithm` 置空即关闭主机名校验的语义对齐：空串 / `None` → 不校验。
fn kafka_ssl_verify_certificate(k: &KafkaConfig) -> bool {
    match &k.ssl_endpoint_identification_algorithm {
        None => true,
        Some(s) => !s.trim().is_empty(),
    }
}

/// 将 TLS 材料写入 librdkafka：JKS 经 `java-ssl-pem` 转内存 PEM；原生 PEM 字段透传。
pub(crate) fn configure_librdkafka_ssl(
    cfg: &mut ClientConfig,
    k: &KafkaConfig,
) -> Result<(), SinkError> {
    let ca_pem = nonempty(k.ssl_ca_pem.as_deref());
    let ca_loc = nonempty(k.ssl_ca_location.as_deref());
    let ts = nonempty(k.ssl_truststore_location.as_deref());
    let cert = nonempty(
        k.ssl_certificate_pem
            .as_deref()
            .or(k.ssl_certificate_location.as_deref()),
    );
    let key = nonempty(
        k.ssl_private_key_pem
            .as_deref()
            .or(k.ssl_key_location.as_deref())
            .or(k.ssl_key_pem.as_deref()),
    );
    let ks = nonempty(k.ssl_keystore_location.as_deref());

    if ca_pem.is_some() && ca_loc.is_some() {
        return Err(ssl_config_err(
            "both ssl.ca.pem and ssl.ca.location are set; use only one CA trust source",
        ));
    }
    if (ca_pem.is_some() || ca_loc.is_some()) && ts.is_some() {
        return Err(ssl_config_err(
            "set either ssl.ca.pem / ssl.ca.location or ssl.truststore.location, not multiple trust sources",
        ));
    }
    if (cert.is_some() || key.is_some()) && ks.is_some() {
        return Err(ssl_config_err(
            "set either PEM certificate/key fields or ssl.keystore.location, not both",
        ));
    }
    if cert.is_some() ^ key.is_some() {
        return Err(ssl_config_err(
            "provide both PEM certificate and private key, or ssl.keystore.location for .jks",
        ));
    }

    let mut jks = JavaSslMaterial::default();
    if let Some(p) = ts {
        let ext = path_ext_lower(p);
        if ext != "jks" {
            return Err(ssl_config_err(format!(
                "ssl.truststore.location={p:?}: expected .jks (P12/PFX and PEM truststore paths not supported here; use ssl.ca.pem / ssl.ca.location)"
            )));
        }
        jks.trust = Some(JksTruststore {
            path: p,
            password: k.ssl_truststore_password.as_deref().unwrap_or(""),
        });
    }
    if let Some(p) = ks {
        let ext = path_ext_lower(p);
        if ext != "jks" {
            return Err(ssl_config_err(format!(
                "ssl.keystore.location={p:?}: expected .jks (P12/PFX not supported); or use PEM cert/key fields"
            )));
        }
        jks.identity = Some(JksKeystore {
            path: p,
            password: k.ssl_keystore_password.as_deref().unwrap_or(""),
            key_alias: nonempty(k.ssl_keystore_alias.as_deref()),
        });
    }

    if jks.trust.is_some() || jks.identity.is_some() {
        let out = materialize_java_ssl_pem(&jks)?;
        if let Some(ref ca) = out.ca_pem {
            cfg.set("ssl.ca.pem", ca);
        }
        if let (Some(ref c), Some(ref ke)) = (&out.certificate_pem, &out.key_pem) {
            cfg.set("ssl.certificate.pem", c);
            cfg.set("ssl.key.pem", ke);
        }
    }

    if let Some(p) = ca_pem {
        cfg.set("ssl.ca.pem", p);
    } else if let Some(p) = ca_loc {
        if looks_like_inline_pem(p) {
            cfg.set("ssl.ca.pem", p);
        } else {
            cfg.set("ssl.ca.location", p);
        }
    }

    if let (Some(c), Some(ke)) = (cert, key) {
        if looks_like_inline_pem(c) {
            cfg.set("ssl.certificate.pem", c);
        } else {
            cfg.set("ssl.certificate.location", c);
        }
        if looks_like_inline_pem(ke) {
            cfg.set("ssl.key.pem", ke);
        } else {
            cfg.set("ssl.key.location", ke);
        }
    }

    cfg.set(
        "enable.ssl.certificate.verification",
        if kafka_ssl_verify_certificate(k) {
            "true"
        } else {
            "false"
        },
    );
    Ok(())
}

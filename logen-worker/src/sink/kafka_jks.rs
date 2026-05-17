use std::path::Path;

use java_ssl_pem::{
    materialize_java_ssl_pem,
    IdentityMaterial, JavaSslMaterial, JavaSslPemError, PemMaterial, TrustMaterial,
};
use logen_dsl::KafkaConfig;
use rdkafka::config::ClientConfig;

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

/// 将 PEM 文本写入 librdkafka（`ssl.ca.pem`、`ssl.certificate.pem`、`ssl.key.pem`）。
pub(crate) fn configure_librdkafka_ssl(
    cfg: &mut ClientConfig,
    k: &KafkaConfig,
) -> Result<(), super::SinkError> {
    configure_librdkafka_ssl_inner(cfg, k)?;
    Ok(())
}

fn configure_librdkafka_ssl_inner(
    cfg: &mut ClientConfig,
    k: &KafkaConfig,
) -> Result<(), JavaSslPemError> {
    let verify = kafka_ssl_verify_certificate(k);
    let m = java_ssl_material_from_kafka(k)?;
    let out = materialize_java_ssl_pem(&m)?;
    if let Some(ref ca) = out.ca_pem {
        cfg.set("ssl.ca.pem", ca);
    }
    if let (Some(ref cert), Some(ref key)) = (&out.certificate_pem, &out.key_pem) {
        cfg.set("ssl.certificate.pem", cert);
        cfg.set("ssl.key.pem", key);
    }
    cfg.set(
        "enable.ssl.certificate.verification",
        if verify { "true" } else { "false" },
    );
    Ok(())
}

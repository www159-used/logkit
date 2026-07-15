//! JKS truststore / keystore → PEM（不支持 P12）。

use std::fs;
use std::path::Path;

use crate::jks::JksStore;
use crate::pem::{der_to_pem, normalize_pem_document, push_cert_der};
use crate::JavaSslPemError;

fn with_path(mut e: JavaSslPemError, path: &Path) -> JavaSslPemError {
    match &mut e {
        JavaSslPemError::Jks { path: p, .. } if p.is_empty() => {
            *p = path.display().to_string();
        }
        _ => {}
    }
    e
}

fn require_jks_path(path: &Path) -> Result<(), JavaSslPemError> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    if ext != "jks" {
        return Err(JavaSslPemError::jks(
            "path",
            path,
            format!("expected .jks (got .{ext}); P12/PFX is not supported"),
        ));
    }
    Ok(())
}

fn load_jks(path: &Path, password: &str) -> Result<JksStore, JavaSslPemError> {
    require_jks_path(path)?;
    let bytes = fs::read(path).map_err(|e| JavaSslPemError::io(path, "read", e))?;
    JksStore::load(&bytes, password).map_err(|e| with_path(e, path))
}

/// 选私钥别名：0 报错；1 直接用；多条时显式 `want` 优先，否则优先 `agent`，再否则报错。
fn pick_key_alias(
    aliases: Vec<String>,
    want: Option<&str>,
    empty_detail: &str,
) -> Result<String, JavaSslPemError> {
    match aliases.len() {
        0 => Err(JavaSslPemError::ClientIdentity {
            detail: empty_detail.into(),
        }),
        1 => Ok(aliases.into_iter().next().unwrap()),
        _ => {
            if let Some(want) = want.map(str::trim).filter(|s| !s.is_empty()) {
                let want_l = want.to_lowercase();
                return aliases
                    .iter()
                    .find(|a| a.to_lowercase() == want_l)
                    .cloned()
                    .ok_or_else(|| JavaSslPemError::ClientIdentity {
                        detail: format!(
                            "keystore.alias={want:?} does not match any private-key alias; available: {}",
                            aliases.join(", ")
                        ),
                    });
            }
            if let Some(agent) = aliases
                .iter()
                .find(|a| a.eq_ignore_ascii_case("agent"))
                .cloned()
            {
                return Ok(agent);
            }
            Err(JavaSslPemError::ClientIdentity {
                detail: format!(
                    "multiple private-key entries and no 'agent' alias; set keystore.alias (available: {})",
                    aliases.join(", ")
                ),
            })
        }
    }
}

/// 解析 JKS 私钥别名（多私钥：显式 `key_alias` → 否则优先 `agent`）。
pub(crate) fn resolve_key_alias(
    path: &Path,
    password: &str,
    key_alias: Option<&str>,
) -> Result<String, JavaSslPemError> {
    let ks = load_jks(path, password)?;
    pick_key_alias(
        ks.private_key_aliases(),
        key_alias,
        "keystore: no PrivateKeyEntry (need a key entry for mTLS)",
    )
}

/// JKS truststore → 合并 CA PEM。
pub(crate) fn truststore_to_ca_pem(path: &Path, password: &str) -> Result<String, JavaSslPemError> {
    let ks = load_jks(path, password)?;
    let mut pem = String::new();
    let mut n = 0usize;
    for (alias, cert) in ks.trusted_certs() {
        if !cert.cert_type.eq_ignore_ascii_case("X509") {
            return Err(JavaSslPemError::jks(
                "truststore",
                path,
                format!(
                    "alias={alias:?}: unsupported cert type {:?}",
                    cert.cert_type
                ),
            ));
        }
        push_cert_der(&mut pem, &cert.der);
        n += 1;
    }
    if n == 0 {
        return Err(JavaSslPemError::jks(
            "truststore",
            path,
            "no TrustedCertificateEntry (wrong file or empty truststore)",
        ));
    }
    normalize_pem_document(&pem)
}

/// JKS client keystore → `(certificate_pem, key_pem)`。
pub(crate) fn client_keystore_to_pem(
    path: &Path,
    password: &str,
    entry_alias: Option<&str>,
) -> Result<(String, String), JavaSslPemError> {
    let ks = load_jks(path, password)?;
    let alias = pick_key_alias(
        ks.private_key_aliases(),
        entry_alias,
        "keystore: no PrivateKeyEntry (need a key entry for mTLS)",
    )?;
    let (key_der, chain) = ks
        .decrypt_private_key(&alias, password)
        .map_err(|e| with_path(e, path))?;

    let mut cert_pem = String::new();
    for cert in &chain {
        if !cert.cert_type.eq_ignore_ascii_case("X509") {
            return Err(JavaSslPemError::jks(
                "keystore",
                path,
                format!("unsupported certificate type {:?}", cert.cert_type),
            ));
        }
        push_cert_der(&mut cert_pem, &cert.der);
    }
    if cert_pem.trim().is_empty() {
        return Err(JavaSslPemError::jks(
            "keystore",
            path,
            format!("empty certificate chain for alias {alias:?}"),
        ));
    }
    Ok((
        normalize_pem_document(&cert_pem)?,
        normalize_pem_document(&der_to_pem("PRIVATE KEY", &key_der))?,
    ))
}

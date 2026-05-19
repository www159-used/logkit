use anyhow::{Context, Result};
use pullout::SpineError;

use crate::config::TlsPaths;

pub struct TlsMaterial {
    pub ca_pem: Vec<u8>,
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

fn decrypt_agent_key(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.starts_with("config:") {
        pullout::pull_out(trimmed).map_err(|e: SpineError| anyhow::anyhow!("pullout agent.key: {e}"))
    } else {
        Ok(trimmed.to_string())
    }
}

pub fn load_material(paths: &TlsPaths) -> Result<TlsMaterial> {
    let ca_pem = std::fs::read(&paths.ca).with_context(|| format!("read CA {}", paths.ca.display()))?;
    let cert_pem =
        std::fs::read(&paths.cert).with_context(|| format!("read cert {}", paths.cert.display()))?;
    let key_raw = std::fs::read_to_string(&paths.key)
        .with_context(|| format!("read key {}", paths.key.display()))?;
    let key_pem = decrypt_agent_key(&key_raw)?.into_bytes();
    Ok(TlsMaterial {
        ca_pem,
        cert_pem,
        key_pem,
    })
}

pub fn build_http_client(material: &TlsMaterial, insecure: bool) -> Result<reqwest::Client> {
    let mut identity_pem = material.cert_pem.clone();
    if !identity_pem.ends_with(b"\n") {
        identity_pem.push(b'\n');
    }
    identity_pem.extend_from_slice(&material.key_pem);
    let identity = reqwest::Identity::from_pem(&identity_pem).context("parse client cert/key PEM")?;

    let mut builder = reqwest::Client::builder()
        .use_rustls_tls()
        .identity(identity)
        .redirect(reqwest::redirect::Policy::none());

    if insecure {
        // 等同 curl --insecure：接受自签/过期/主机名与证书不符
        builder = builder.danger_accept_invalid_certs(true);
    } else {
        let ca = reqwest::Certificate::from_pem(&material.ca_pem).context("parse CA PEM")?;
        builder = builder.add_root_certificate(ca);
    }

    builder.build().context("build HTTPS client")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decrypt_plain_pem_passthrough() {
        let pem = "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----";
        assert_eq!(decrypt_agent_key(pem).unwrap(), pem);
    }
}

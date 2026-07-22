use anyhow::{Context, Result};

use crate::config::TlsPaths;

pub struct TlsMaterial {
    pub ca_pem: Vec<u8>,
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

pub fn load_material(paths: &TlsPaths) -> Result<TlsMaterial> {
    let ca_pem =
        std::fs::read(&paths.ca).with_context(|| format!("read CA {}", paths.ca.display()))?;
    let cert_pem = std::fs::read(&paths.cert)
        .with_context(|| format!("read cert {}", paths.cert.display()))?;
    let key_pem = pullout::decrypt_key_file(&paths.key)
        .map_err(|e| anyhow::anyhow!("agent.key: {e}"))?
        .into_bytes();
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
    let identity =
        reqwest::Identity::from_pem(&identity_pem).context("parse client cert/key PEM")?;

    let mut builder = reqwest::Client::builder()
        .identity(identity)
        .redirect(reqwest::redirect::Policy::none());

    if insecure {
        builder = builder.danger_accept_invalid_certs(true);
    } else {
        let ca = reqwest::Certificate::from_pem(&material.ca_pem).context("parse CA PEM")?;
        builder = builder.tls_certs_only([ca]);
    }

    builder.build().context("build HTTPS client")
}

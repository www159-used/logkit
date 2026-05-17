//! Java 风格 TLS 材料（**`.jks` / `.p12` / `.pfx`** 与 **PEM/DER**）解析为 **内存中的 PEM 文本**，供支持 `ssl.ca.pem` / `ssl.certificate.pem` / `ssl.key.pem` 等内联配置的 TLS 栈使用（**不依赖 Kafka / librdkafka**）。
//!
//! 入口类型为 [`JavaSslMaterial`]：其中 **`trust`** 与 **`identity`** 各自为 [`Option`]，且分别用 [`TrustMaterial`] / [`IdentityMaterial`] **枚举**区分 **PEM / JKS / P12**（二者格式可不同，例如 JKS trust + PEM mTLS）。
//!
//! - **`.jks` / `.p12` / `.pfx`**：纯 Rust [`jks`]（含 PKCS#12 / `p12-keystore`）解析，**不**调用本机 `openssl` 可执行文件。
//! - 多私钥 keystore：未指定别名时取**私钥别名升序第一条**（与常见 Java「只配 location+password」接近）。

use std::fs;
use std::io::Read;
use std::path::Path;

use jks::{KeyStore, KeyStoreOptions};
use p12_keystore::{KeyStore as P12KeyStore, KeyStoreEntry as P12Entry, Pkcs12ImportPolicy};
use pem::Pem;

mod error;

pub use error::JavaSslPemError;

/// PEM：内联文本或文件路径（路径亦可指向 PEM/DER；**信任锚**路径会经校验后读入内存）。
#[derive(Debug, Clone, Copy)]
pub enum PemMaterial<'a> {
    Inline(&'a str),
    Path(&'a str),
}

/// 校验服务端证书用的**信任锚**（CA）。`Jks` / `P12` 为 Java truststore；`Pem` 为 CA 链或单张证书文件。
#[derive(Debug, Clone, Copy)]
pub enum TrustMaterial<'a> {
    Pem(PemMaterial<'a>),
    Jks {
        path: &'a str,
        password: &'a str,
    },
    P12 {
        path: &'a str,
        password: &'a str,
    },
}

/// 客户端 **mTLS** 身份。可与 [`TrustMaterial`] **独立**选择存储形态（例如 JKS trust + PEM identity）。
#[derive(Debug, Clone, Copy)]
pub enum IdentityMaterial<'a> {
    Pem {
        certificate: PemMaterial<'a>,
        private_key: PemMaterial<'a>,
    },
    Jks {
        path: &'a str,
        password: &'a str,
        key_alias: Option<&'a str>,
    },
    P12 {
        path: &'a str,
        password: &'a str,
    },
}

/// TLS 材料：信任链与客户端身份**各自**可选、**类型枚举化**（不再平铺 `ssl.*` 字段）。
#[derive(Debug, Clone, Copy, Default)]
pub struct JavaSslMaterial<'a> {
    pub trust: Option<TrustMaterial<'a>>,
    pub identity: Option<IdentityMaterial<'a>>,
}

/// [`materialize_java_ssl_pem`] 的输出：信任链与客户端证书对应的 **PEM 文本**（UTF-8）。
#[derive(Debug, Default)]
pub struct MaterializedSslPem {
    pub ca_pem: Option<String>,
    pub certificate_pem: Option<String>,
    pub key_pem: Option<String>,
}

fn ext_lower(path: &Path) -> String {
    path.extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase()
}

fn is_jks_p12_pfx(ext: &str) -> bool {
    matches!(ext, "jks" | "p12" | "pfx")
}

fn der_to_pem(tag: &str, der: &[u8]) -> String {
    pem::encode(&Pem::new(tag, der.to_vec()))
}

fn strip_utf8_bom(mut b: &[u8]) -> &[u8] {
    if b.len() >= 3 && b[..3] == [0xEF, 0xBB, 0xBF] {
        b = &b[3..];
    }
    b
}

fn trim_ascii_prefix_ws(mut b: &[u8]) -> &[u8] {
    while let Some((&first, rest)) = b.split_first() {
        if matches!(first, b' ' | b'\t' | b'\n' | b'\r') {
            b = rest;
        } else {
            break;
        }
    }
    b
}

fn looks_like_pem_bytes(b: &[u8]) -> bool {
    let t = strip_utf8_bom(b);
    t.windows(11).any(|w| w == b"-----BEGIN ")
}

fn looks_like_der_x509(b: &[u8]) -> bool {
    let t = trim_ascii_prefix_ws(strip_utf8_bom(b));
    !t.is_empty() && t[0] == 0x30
}

fn read_file_head(path: &Path, max: usize) -> Result<Vec<u8>, JavaSslPemError> {
    let mut f = fs::File::open(path).map_err(|e| JavaSslPemError::io(path, "open", e))?;
    let mut buf = vec![0u8; max];
    let n = f
        .read(&mut buf)
        .map_err(|e| JavaSslPemError::io(path, "read", e))?;
    buf.truncate(n);
    Ok(buf)
}

fn validate_x509_der(der: &[u8], what: &str) -> Result<(), JavaSslPemError> {
    if der.len() < 32 {
        return Err(JavaSslPemError::CertEncoding {
            detail: format!("{what}: DER too short"),
        });
    }
    if der[0] != 0x30 {
        return Err(JavaSslPemError::CertEncoding {
            detail: format!("{what}: DER does not start with SEQUENCE (0x30)"),
        });
    }
    Ok(())
}

/// 规范换行并以单个 `\n` 结尾，供 TLS 栈稳定解析。
fn normalize_pem_document(s: &str) -> Result<String, JavaSslPemError> {
    let mut t = s.replace("\r\n", "\n");
    t = t.trim().to_string();
    if t.is_empty() {
        return Err(JavaSslPemError::CertEncoding {
            detail: "empty PEM document".into(),
        });
    }
    if !t.ends_with('\n') {
        t.push('\n');
    }
    Ok(t)
}

fn is_p12_path(path: &Path) -> bool {
    matches!(ext_lower(path).as_str(), "p12" | "pfx")
}

fn load_jks_file(path: &Path, password: &str) -> Result<KeyStore, JavaSslPemError> {
    let bytes = fs::read(path).map_err(|e| JavaSslPemError::io(path, "read", e))?;
    let mut ks = KeyStore::with_options(KeyStoreOptions {
        ordered_aliases: true,
        ..Default::default()
    });
    ks.load(&mut bytes.as_slice(), password.as_bytes())
        .map_err(|e| JavaSslPemError::jks("load", path, e.to_string()))?;
    Ok(ks)
}

fn load_p12_keystore(path: &Path, password: &str) -> Result<P12KeyStore, JavaSslPemError> {
    let data = fs::read(path).map_err(|e| JavaSslPemError::io(path, "read", e))?;
    let path_s = path.display().to_string();
    match P12KeyStore::from_pkcs12(&data, password, Pkcs12ImportPolicy::Strict) {
        Ok(ks) => Ok(ks),
        Err(strict_err) => P12KeyStore::from_pkcs12(&data, password, Pkcs12ImportPolicy::Relaxed).map_err(
            |relaxed_err| JavaSslPemError::Pkcs12 {
                path: path_s,
                detail: format!("strict: {strict_err}; relaxed: {relaxed_err}"),
            },
        ),
    }
}

fn push_cert_der(pem: &mut String, der: &[u8]) {
    pem.push_str(&der_to_pem("CERTIFICATE", der));
    if !pem.ends_with('\n') {
        pem.push('\n');
    }
}

fn p12_truststore_to_ca_pem_string(path: &Path, password: &str) -> Result<String, JavaSslPemError> {
    let ks = load_p12_keystore(path, password)?;
    let mut pem = String::new();
    let mut n = 0usize;
    for (_alias, entry) in ks.entries() {
        match entry {
            P12Entry::Certificate(cert) => {
                push_cert_der(&mut pem, cert.as_der());
                n += 1;
            }
            P12Entry::PrivateKeyChain(chain) => {
                for cert in chain.certs() {
                    push_cert_der(&mut pem, cert.as_der());
                    n += 1;
                }
            }
            P12Entry::Secret(_) => {}
        }
    }
    if n == 0 {
        return Err(JavaSslPemError::Pkcs12 {
            path: path.display().to_string(),
            detail: "no trusted certificates (wrong file or empty truststore)".into(),
        });
    }
    normalize_pem_document(&pem)
}

fn resolve_p12_client_alias<'a>(
    ks: &'a P12KeyStore,
    yaml_alias: Option<&str>,
) -> Result<(&'a str, &'a p12_keystore::PrivateKeyChain), JavaSslPemError> {
    let mut chains: Vec<(&str, &p12_keystore::PrivateKeyChain)> = ks
        .entries()
        .filter_map(|(alias, entry)| match entry {
            P12Entry::PrivateKeyChain(chain) => Some((alias.as_str(), chain)),
            _ => None,
        })
        .collect();
    chains.sort_by_key(|(alias, _)| *alias);
    match chains.len() {
        0 => Err(JavaSslPemError::ClientIdentity {
            detail: "PKCS#12 keystore: no private key entry (need a key entry for mTLS)".into(),
        }),
        1 => Ok(chains[0]),
        _ => {
            if let Some(want) = yaml_alias.map(str::trim).filter(|s| !s.is_empty()) {
                let want_l = want.to_lowercase();
                for (alias, chain) in &chains {
                    if alias.to_lowercase() == want_l {
                        return Ok((*alias, *chain));
                    }
                }
                return Err(JavaSslPemError::ClientIdentity {
                    detail: format!(
                        "keystore.alias={want:?} does not match any private-key alias; available: {}",
                        chains
                            .iter()
                            .map(|(a, _)| *a)
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                });
            }
            Ok(chains[0])
        }
    }
}

fn p12_client_keystore_to_pem_strings(
    path: &Path,
    password: &str,
    entry_alias: Option<&str>,
) -> Result<(String, String), JavaSslPemError> {
    let ks = load_p12_keystore(path, password)?;
    let (_alias, chain) = resolve_p12_client_alias(&ks, entry_alias)?;
    let mut cert_pem = String::new();
    for cert in chain.certs() {
        push_cert_der(&mut cert_pem, cert.as_der());
    }
    if cert_pem.trim().is_empty() {
        return Err(JavaSslPemError::Pkcs12 {
            path: path.display().to_string(),
            detail: "private key entry has empty certificate chain".into(),
        });
    }
    let key_pem = der_to_pem("PRIVATE KEY", chain.key().as_der());
    Ok((
        normalize_pem_document(&cert_pem)?,
        normalize_pem_document(&key_pem)?,
    ))
}

/// 将 **JKS truststore** 中的受信任证书导出为单个 PEM 文本（多条 `BEGIN CERTIFICATE`）。
pub fn jks_truststore_to_ca_pem_string(jks: &Path, password: &str) -> Result<String, JavaSslPemError> {
    if is_p12_path(jks) {
        return p12_truststore_to_ca_pem_string(jks, password);
    }
    let ks = load_jks_file(jks, password)?;
    let mut pem = String::new();
    let mut n = 0usize;
    for alias in ks.aliases() {
        if ks.is_trusted_certificate_entry(&alias) {
            let tce = ks
                .get_trusted_certificate_entry(&alias)
                .map_err(|e| JavaSslPemError::jks("truststore", jks, format!("trusted entry {alias:?}: {e}")))?;
            if !tce.certificate.cert_type.eq_ignore_ascii_case("X509") {
                return Err(JavaSslPemError::jks(
                    "truststore",
                    jks,
                    format!(
                        "alias={alias:?}: unsupported cert type {:?}",
                        tce.certificate.cert_type
                    ),
                ));
            }
            pem.push_str(&der_to_pem("CERTIFICATE", &tce.certificate.content));
            if !pem.ends_with('\n') {
                pem.push('\n');
            }
            n += 1;
        }
    }
    if n == 0 {
        return Err(JavaSslPemError::jks(
            "truststore",
            jks,
            "no TrustedCertificateEntry (wrong file or empty truststore)",
        ));
    }
    normalize_pem_document(&pem)
}

fn resolve_client_keystore_alias(
    ks: &KeyStore,
    yaml_alias: Option<&str>,
) -> Result<String, JavaSslPemError> {
    let mut pk_aliases: Vec<String> = ks
        .aliases()
        .into_iter()
        .filter(|a| ks.is_private_key_entry(a))
        .collect();
    pk_aliases.sort();
    match pk_aliases.len() {
        0 => Err(JavaSslPemError::ClientIdentity {
            detail: "keystore: no PrivateKeyEntry (need a key entry for mTLS)".into(),
        }),
        1 => Ok(pk_aliases.remove(0)),
        _ => {
            if let Some(want) = yaml_alias.map(str::trim).filter(|s| !s.is_empty()) {
                let want_l = want.to_lowercase();
                for a in &pk_aliases {
                    if a.to_lowercase() == want_l {
                        return Ok(a.clone());
                    }
                }
                return Err(JavaSslPemError::ClientIdentity {
                    detail: format!(
                        "keystore.alias={want:?} does not match any private-key alias; available: {}",
                        pk_aliases.join(", ")
                    ),
                });
            }
            Ok(pk_aliases.remove(0))
        }
    }
}

/// 将 **JKS keystore** 中选定私钥条目导出为证书链 PEM + PKCS#8 私钥 PEM（内存）。
pub fn jks_client_keystore_to_pem_strings(
    jks: &Path,
    password: &str,
    entry_alias: Option<&str>,
) -> Result<(String, String), JavaSslPemError> {
    if is_p12_path(jks) {
        return p12_client_keystore_to_pem_strings(jks, password, entry_alias);
    }
    let ks = load_jks_file(jks, password)?;
    let alias = resolve_client_keystore_alias(&ks, entry_alias)?;
    let pke = ks
        .get_private_key_entry(&alias, password.as_bytes())
        .map_err(|e| JavaSslPemError::jks("keystore", jks, format!("decrypt private key: {e}")))?;

    let mut cert_pem = String::new();
    for cert in &pke.certificate_chain {
        if !cert.cert_type.eq_ignore_ascii_case("X509") {
            return Err(JavaSslPemError::jks(
                "keystore",
                jks,
                format!("unsupported certificate type {:?}", cert.cert_type),
            ));
        }
        cert_pem.push_str(&der_to_pem("CERTIFICATE", &cert.content));
        if !cert_pem.ends_with('\n') {
            cert_pem.push('\n');
        }
    }
    if cert_pem.trim().is_empty() {
        return Err(JavaSslPemError::jks(
            "keystore",
            jks,
            format!("empty certificate chain for alias {alias:?}"),
        ));
    }

    let key_pem = der_to_pem("PRIVATE KEY", &pke.private_key);
    Ok((
        normalize_pem_document(&cert_pem)?,
        normalize_pem_document(&key_pem)?,
    ))
}

/// 信任锚：PEM 链或单张 X.509 DER 文件；JKS/P12 须走 `truststore_location`。
fn read_trust_anchor_file_to_pem(path: &Path, label: &'static str) -> Result<String, JavaSslPemError> {
    if !path.is_file() {
        return Err(JavaSslPemError::TrustPath {
            label,
            path: path.display().to_string(),
            detail: "not a regular file".into(),
        });
    }
    let ext = ext_lower(path);
    if is_jks_p12_pfx(&ext) {
        return Err(JavaSslPemError::TrustPath {
            label,
            path: path.display().to_string(),
            detail: "use truststore_location + truststore_password for Java truststores (.jks / .p12 / .pfx)"
                .into(),
        });
    }
    let head = read_file_head(path, 4096)?;
    if looks_like_pem_bytes(&head) {
        let raw = fs::read_to_string(path).map_err(|e| JavaSslPemError::io(path, "read", e))?;
        if !(raw.contains("BEGIN CERTIFICATE") || raw.contains("BEGIN TRUSTED CERTIFICATE")) {
            return Err(JavaSslPemError::TrustPath {
                label,
                path: path.display().to_string(),
                detail: "PEM must be a CA / certificate bundle (-----BEGIN CERTIFICATE----- or TRUSTED CERTIFICATE)"
                    .into(),
            });
        }
        return normalize_pem_document(&raw);
    }
    if looks_like_der_x509(&head) {
        let bytes = fs::read(path).map_err(|e| JavaSslPemError::io(path, "read", e))?;
        validate_x509_der(&bytes, "CA / trust anchor")?;
        let pem = der_to_pem("CERTIFICATE", &bytes);
        return normalize_pem_document(&pem);
    }
    Err(JavaSslPemError::TrustPath {
        label,
        path: path.display().to_string(),
        detail: "expected PEM (-----BEGIN ...) or a single X.509 DER certificate file (often .cer)".into(),
    })
}

fn materialize_pem_trust(p: PemMaterial<'_>) -> Result<String, JavaSslPemError> {
    match p {
        PemMaterial::Inline(text) => {
            let t = text.trim();
            if t.is_empty() {
                return Err(JavaSslPemError::TrustField {
                    field: "trust.pem.inline",
                    detail: "empty".into(),
                });
            }
            normalize_pem_document(t)
        }
        PemMaterial::Path(s) => {
            let t = s.trim();
            if t.is_empty() {
                return Err(JavaSslPemError::TrustField {
                    field: "trust.pem.path",
                    detail: "empty".into(),
                });
            }
            if t.starts_with("-----BEGIN") {
                return normalize_pem_document(t);
            }
            read_trust_anchor_file_to_pem(Path::new(t), "trust.pem.path")
        }
    }
}

fn materialize_trust(t: &TrustMaterial<'_>) -> Result<Option<String>, JavaSslPemError> {
    match t {
        TrustMaterial::Pem(p) => Ok(Some(materialize_pem_trust(*p)?)),
        TrustMaterial::Jks { path, password } => Ok(Some(jks_truststore_to_ca_pem_string(
            Path::new(path),
            password,
        )?)),
        TrustMaterial::P12 { path, password } => Ok(Some(jks_truststore_to_ca_pem_string(
            Path::new(path),
            password,
        )?)),
    }
}

fn materialize_pem_identity_one(p: PemMaterial<'_>, label: &'static str) -> Result<String, JavaSslPemError> {
    match p {
        PemMaterial::Inline(text) => {
            let t = text.trim();
            if t.is_empty() {
                return Err(JavaSslPemError::ClientIdentity {
                    detail: format!("{label}: empty inline PEM"),
                });
            }
            normalize_pem_document(t)
        }
        PemMaterial::Path(s) => {
            let t = s.trim();
            if t.is_empty() {
                return Err(JavaSslPemError::ClientIdentity {
                    detail: format!("{label}: empty path"),
                });
            }
            if t.starts_with("-----BEGIN") {
                return normalize_pem_document(t);
            }
            let path = Path::new(t);
            let bytes = fs::read(path).map_err(|e| JavaSslPemError::io(path, "read", e))?;
            if looks_like_pem_bytes(&bytes) {
                let raw = String::from_utf8_lossy(&bytes);
                if label == "client_cert" {
                    if !(raw.contains("BEGIN CERTIFICATE") || raw.contains("BEGIN TRUSTED CERTIFICATE")) {
                        return Err(JavaSslPemError::ClientIdentity {
                            detail: format!(
                                "{}: PEM file must contain a certificate block",
                                path.display()
                            ),
                        });
                    }
                } else if !(raw.contains("BEGIN PRIVATE KEY")
                    || raw.contains("BEGIN RSA PRIVATE KEY")
                    || raw.contains("BEGIN EC PRIVATE KEY"))
                {
                    return Err(JavaSslPemError::ClientIdentity {
                        detail: format!("{}: PEM file must contain a private key block", path.display()),
                    });
                }
                return normalize_pem_document(&raw);
            }
            if label == "client_cert" && looks_like_der_x509(&bytes) {
                validate_x509_der(&bytes, "client certificate")?;
                let pem = der_to_pem("CERTIFICATE", &bytes);
                return normalize_pem_document(&pem);
            }
            Err(JavaSslPemError::ClientIdentity {
                detail: format!(
                    "{label}: unsupported file format for {}",
                    path.display()
                ),
            })
        }
    }
}

fn materialize_identity(id: &IdentityMaterial<'_>) -> Result<Option<(String, String)>, JavaSslPemError> {
    match id {
        IdentityMaterial::Pem {
            certificate,
            private_key,
        } => {
            let cert = materialize_pem_identity_one(*certificate, "client_cert")?;
            let key = materialize_pem_identity_one(*private_key, "client_key")?;
            Ok(Some((cert, key)))
        }
        IdentityMaterial::Jks {
            path,
            password,
            key_alias,
        } => jks_client_keystore_to_pem_strings(Path::new(path), password, *key_alias).map(Some),
        IdentityMaterial::P12 { path, password } => {
            jks_client_keystore_to_pem_strings(Path::new(path), password, None).map(Some)
        }
    }
}

/// 将 [`JavaSslMaterial`] 解析为 PEM 文本；失败时返回 [`JavaSslPemError`]。
pub fn materialize_java_ssl_pem(m: &JavaSslMaterial<'_>) -> Result<MaterializedSslPem, JavaSslPemError> {
    let ca_pem = match &m.trust {
        Some(t) => materialize_trust(t)?,
        None => None,
    };
    let pair = match &m.identity {
        Some(i) => materialize_identity(i)?,
        None => None,
    };
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
    use super::*;
    use std::io::Read;
    use std::path::Path;
    use std::path::PathBuf;

    fn worker_assets() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..").join("assets")
    }

    /// 与 `logen-worker/tests/fixtures/kafka_asset_broker.yaml` 中 `sink.kafka` 口令一致（勿与真实环境口令混用）。
    const FIXTURE_TRUSTSTORE_PASSWORD: &str = "vKFoWrbf_El1pCtcUVHZn0ygI5Mu8izQ";
    const FIXTURE_KEYSTORE_PASSWORD: &str =
        "8c4804e1504aa139bd827c9c016f11d4cc7174a95352f5068a3cb2c1f4849e91";

    /// 测试内容：JKS truststore fixture 导出为 CA PEM 字符串。
    #[test]
    fn jks_truststore_fixture_emits_ca_pem() {
        let ts = worker_assets().join("truststore.jks");
        assert!(ts.is_file(), "missing fixture {}", ts.display());
        let pem = jks_truststore_to_ca_pem_string(&ts, FIXTURE_TRUSTSTORE_PASSWORD).expect("truststore → PEM");
        assert!(
            pem.contains("BEGIN CERTIFICATE") || pem.contains("BEGIN TRUSTED CERTIFICATE"),
            "expected certificate PEM"
        );
    }

    /// 测试内容：JKS keystore fixture 导出客户端 cert/key PEM 字符串。
    #[test]
    fn jks_keystore_fixture_emits_client_cert_and_key_pem() {
        let ks = worker_assets().join("keystore.jks");
        assert!(ks.is_file(), "missing fixture {}", ks.display());
        let (cert, key) =
            jks_client_keystore_to_pem_strings(&ks, FIXTURE_KEYSTORE_PASSWORD, None).expect("keystore → PEM");
        assert!(cert.contains("BEGIN CERTIFICATE"), "cert PEM");
        assert!(
            key.contains("BEGIN PRIVATE KEY") || key.contains("BEGIN RSA PRIVATE KEY"),
            "key PEM"
        );
    }

    /// 测试内容：`truststore.jks` / `keystore.jks` 魔数。
    #[test]
    fn assets_jks_magic() {
        for name in ["keystore.jks", "truststore.jks"] {
            let path = worker_assets().join(name);
            assert!(path.is_file(), "missing fixture {}", path.display());
            let mut f = std::fs::File::open(&path).unwrap();
            let mut b = [0u8; 4];
            f.read_exact(&mut b).unwrap();
            assert_eq!(b, [0xFE, 0xED, 0xFE, 0xED], "{}", path.display());
        }
    }

    /// 测试内容：显式 [`TrustMaterial::Jks`] + [`IdentityMaterial::Jks`] 与 fixture 口令可完成 [`materialize_java_ssl_pem`]。
    #[test]
    fn materialize_typed_jks_trust_and_jks_identity() {
        let assets = worker_assets();
        let trust_p = assets.join("truststore.jks").to_string_lossy().into_owned();
        let key_p = assets.join("keystore.jks").to_string_lossy().into_owned();
        assert!(Path::new(&trust_p).is_file());
        assert!(Path::new(&key_p).is_file());
        let m = JavaSslMaterial {
            trust: Some(TrustMaterial::Jks {
                path: trust_p.as_str(),
                password: FIXTURE_TRUSTSTORE_PASSWORD,
            }),
            identity: Some(IdentityMaterial::Jks {
                path: key_p.as_str(),
                password: FIXTURE_KEYSTORE_PASSWORD,
                key_alias: None,
            }),
        };
        let out = materialize_java_ssl_pem(&m).expect("materialize");
        let ca = out.ca_pem.as_ref().expect("ca");
        assert!(ca.contains("BEGIN CERTIFICATE"));
        let c = out.certificate_pem.as_ref().expect("cert");
        let k = out.key_pem.as_ref().expect("key");
        assert!(c.contains("BEGIN CERTIFICATE"));
        assert!(k.contains("BEGIN PRIVATE KEY") || k.contains("BEGIN RSA PRIVATE KEY"));
    }
}

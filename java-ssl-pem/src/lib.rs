//! Java 风格 TLS 材料（**`.jks` / `.p12` / `.pfx`** 与 **PEM/DER**）解析为 **磁盘上的 PEM 路径**，供任意「只认 `ssl.ca.location` 式路径」的 TLS 栈使用（**不依赖 Kafka / librdkafka**）。
//!
//! 入口类型为 [`JavaSslMaterial`]：其中 **`trust`** 与 **`identity`** 各自为 [`Option`]，且分别用 [`TrustMaterial`] / [`IdentityMaterial`] **枚举**区分 **PEM / JKS / P12**（二者格式可不同，例如 JKS trust + PEM mTLS）。
//!
//! - **`.jks`**：纯 Rust [`jks`] 解析。
//! - **`.p12`/`.pfx`**：调用本机 **`openssl pkcs12`**（`PATH` 须可执行）。
//! - 多私钥 JKS：未指定别名时取**私钥别名升序第一条**（与常见 Java「只配 location+password」接近）。

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;

use jks::{KeyStore, KeyStoreOptions};
use pem::Pem;
use tempfile::TempDir;

mod error;

pub use error::JavaSslPemError;

/// PEM：内联文本或文件路径（路径亦可指向 PEM/DER；**信任锚**路径会经 [`materialize_ca_trust_file_path`] 校验）。
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

/// [`materialize_java_ssl_pem`] 的输出：信任链与客户端证书对应的 **PEM 文件路径**；若曾从 JKS/P12/内联 PEM 落盘，则带 [`MaterializedSslPem::scratch_dir`]，须与这些路径**同生命周期**持有。
#[derive(Debug)]
pub struct MaterializedSslPem {
    pub ca_file: Option<String>,
    pub certificate_file: Option<String>,
    pub key_file: Option<String>,
    pub scratch_dir: Option<TempDir>,
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

/// 规范换行并以单个 `\n` 结尾，供 OpenSSL 等稳定解析。
fn write_pem_text_file(path: &Path, pem_text: &str) -> Result<(), JavaSslPemError> {
    let mut s = pem_text.replace("\r\n", "\n");
    s = s.trim().to_string();
    if s.is_empty() {
        return Err(JavaSslPemError::CertEncoding {
            detail: format!("write {}: empty PEM", path.display()),
        });
    }
    if !s.ends_with('\n') {
        s.push('\n');
    }
    fs::write(path, s.as_bytes()).map_err(|e| JavaSslPemError::io(path, "write", e))?;
    Ok(())
}

fn load_jks(path: &Path, password: &str) -> Result<KeyStore, JavaSslPemError> {
    let bytes = fs::read(path).map_err(|e| JavaSslPemError::io(path, "read", e))?;
    let mut ks = KeyStore::with_options(KeyStoreOptions {
        ordered_aliases: true,
        ..Default::default()
    });
    ks.load(&mut bytes.as_slice(), password.as_bytes())
        .map_err(|e| JavaSslPemError::jks("load", path, e.to_string()))?;
    Ok(ks)
}

/// 将 **JKS truststore** 中的受信任证书导出为单个 PEM 文件（多条 `BEGIN CERTIFICATE`）。
pub fn jks_truststore_to_ca_pem(
    jks: &Path,
    password: &str,
    out: &Path,
) -> Result<(), JavaSslPemError> {
    let ks = load_jks(jks, password)?;
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
    write_pem_text_file(out, &pem)?;
    Ok(())
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
            detail: "JKS keystore: no PrivateKeyEntry (need a key entry for mTLS)".into(),
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

/// 将 **JKS keystore** 中选定私钥条目导出为证书链 PEM + PKCS#8 私钥 PEM。
pub fn jks_client_keystore_to_pem_files(
    jks: &Path,
    password: &str,
    entry_alias: Option<&str>,
    cert_out: &Path,
    key_out: &Path,
) -> Result<(), JavaSslPemError> {
    let ks = load_jks(jks, password)?;
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
    write_pem_text_file(cert_out, &cert_pem)?;
    write_pem_text_file(key_out, &key_pem)?;
    Ok(())
}

#[derive(Clone, Copy)]
enum Pkcs12Extract {
    CaChain,
    ClientCert,
    ClientKey,
}

fn openssl_pkcs12_extract(
    p12: &Path,
    pass: &str,
    out: &Path,
    mode: Pkcs12Extract,
) -> Result<(), JavaSslPemError> {
    let path_s = p12.display().to_string();
    let mut last_err = String::new();
    for legacy in [false, true] {
        let mut cmd = Command::new("openssl");
        cmd.arg("pkcs12");
        if legacy {
            cmd.arg("-legacy");
        }
        cmd.arg("-in").arg(p12).arg("-nodes");
        match mode {
            Pkcs12Extract::CaChain => {
                cmd.arg("-nokeys");
            }
            Pkcs12Extract::ClientCert => {
                cmd.args(["-clcerts", "-nokeys"]);
            }
            Pkcs12Extract::ClientKey => {
                cmd.arg("-nocerts");
            }
        }
        cmd.arg("-out")
            .arg(out)
            .env("OPENSSL_PKCS12_PASS", pass)
            .args(["-passin", "env:OPENSSL_PKCS12_PASS"]);
        let output = cmd.output().map_err(|e| JavaSslPemError::OpenSsl {
            detail: format!("failed to spawn openssl: {e}"),
        })?;
        if output.status.success() {
            return Ok(());
        }
        last_err = String::from_utf8_lossy(&output.stderr).into_owned();
        if legacy {
            return Err(JavaSslPemError::Pkcs12 {
                path: path_s,
                detail: last_err,
            });
        }
    }
    Err(JavaSslPemError::Pkcs12 {
        path: path_s,
        detail: last_err,
    })
}

fn ensure_tmp_root(tmp: &mut Option<TempDir>, label: &'static str) -> Result<PathBuf, JavaSslPemError> {
    if tmp.is_none() {
        *tmp = Some(TempDir::new().map_err(|e| JavaSslPemError::TempDir {
            label,
            source: e,
        })?);
    }
    Ok(tmp.as_ref().expect("just set").path().to_path_buf())
}

fn der_trust_file_to_temp_pem(
    path: &Path,
    tmp: &mut Option<TempDir>,
    label: &'static str,
) -> Result<String, JavaSslPemError> {
    let bytes = fs::read(path).map_err(|e| JavaSslPemError::io(path, "read", e))?;
    validate_x509_der(&bytes, "CA / trust anchor")?;
    let base = ensure_tmp_root(tmp, label)?;
    let out = base.join("ca-from-der.pem");
    let pem = der_to_pem("CERTIFICATE", &bytes);
    write_pem_text_file(&out, &pem)?;
    Ok(out.to_string_lossy().into_owned())
}

/// 信任锚：PEM 链或单张 X.509 DER；JKS/P12 须走 `truststore_location`。
fn materialize_ca_trust_file_path(
    path: &Path,
    tmp: &mut Option<TempDir>,
    label: &'static str,
) -> Result<String, JavaSslPemError> {
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
        let head_s = String::from_utf8_lossy(&head);
        if !(head_s.contains("BEGIN CERTIFICATE")
            || head_s.contains("BEGIN TRUSTED CERTIFICATE"))
        {
            return Err(JavaSslPemError::TrustPath {
                label,
                path: path.display().to_string(),
                detail: "PEM must be a CA / certificate bundle (-----BEGIN CERTIFICATE----- or TRUSTED CERTIFICATE)"
                    .into(),
            });
        }
        return Ok(path.to_string_lossy().into_owned());
    }
    if looks_like_der_x509(&head) {
        return der_trust_file_to_temp_pem(path, tmp, label);
    }
    Err(JavaSslPemError::TrustPath {
        label,
        path: path.display().to_string(),
        detail: "expected PEM (-----BEGIN ...) or a single X.509 DER certificate file (often .cer)".into(),
    })
}

fn materialize_pem_trust(
    p: PemMaterial<'_>,
    tmp: &mut Option<TempDir>,
) -> Result<String, JavaSslPemError> {
    match p {
        PemMaterial::Inline(text) => {
            let t = text.trim();
            if t.is_empty() {
                return Err(JavaSslPemError::TrustField {
                    field: "trust.pem.inline",
                    detail: "empty".into(),
                });
            }
            let base = ensure_tmp_root(tmp, "trust-pem-inline")?;
            let out = base.join("ca-inline.pem");
            write_pem_text_file(&out, t)?;
            Ok(out.to_string_lossy().into_owned())
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
                let base = ensure_tmp_root(tmp, "trust-path-as-inline-pem")?;
                let out = base.join("ca-inline.pem");
                write_pem_text_file(&out, t)?;
                return Ok(out.to_string_lossy().into_owned());
            }
            materialize_ca_trust_file_path(Path::new(t), tmp, "trust.pem.path")
        }
    }
}

fn materialize_trust(
    t: &TrustMaterial<'_>,
    tmp: &mut Option<TempDir>,
) -> Result<Option<String>, JavaSslPemError> {
    match t {
        TrustMaterial::Pem(p) => Ok(Some(materialize_pem_trust(*p, tmp)?)),
        TrustMaterial::Jks { path, password } => {
            let path = Path::new(path);
            let base = ensure_tmp_root(tmp, "truststore-jks")?;
            let out = base.join("ca-chain.pem");
            jks_truststore_to_ca_pem(path, password, &out)?;
            Ok(Some(out.to_string_lossy().into_owned()))
        }
        TrustMaterial::P12 { path, password } => {
            let path = Path::new(path);
            let base = ensure_tmp_root(tmp, "truststore-p12")?;
            let out = base.join("ca-chain.pem");
            openssl_pkcs12_extract(path, password, &out, Pkcs12Extract::CaChain)?;
            Ok(Some(out.to_string_lossy().into_owned()))
        }
    }
}

fn materialize_pem_identity_one(
    p: PemMaterial<'_>,
    tmp: &mut Option<TempDir>,
    label: &'static str,
) -> Result<String, JavaSslPemError> {
    match p {
        PemMaterial::Inline(text) => {
            let t = text.trim();
            if t.is_empty() {
                return Err(JavaSslPemError::ClientIdentity {
                    detail: format!("{label}: empty inline PEM"),
                });
            }
            let base = ensure_tmp_root(tmp, "client-pem-inline")?;
            let fname = format!("{label}.pem");
            let out = base.join(fname);
            write_pem_text_file(&out, t)?;
            Ok(out.to_string_lossy().into_owned())
        }
        PemMaterial::Path(s) => {
            let t = s.trim();
            if t.is_empty() {
                return Err(JavaSslPemError::ClientIdentity {
                    detail: format!("{label}: empty path"),
                });
            }
            if t.starts_with("-----BEGIN") {
                let base = ensure_tmp_root(tmp, "client-pem-path-inline")?;
                let out = base.join(format!("{label}-inline.pem"));
                write_pem_text_file(&out, t)?;
                return Ok(out.to_string_lossy().into_owned());
            }
            Ok(t.to_string())
        }
    }
}

fn materialize_identity(
    id: &IdentityMaterial<'_>,
    tmp: &mut Option<TempDir>,
) -> Result<Option<(String, String)>, JavaSslPemError> {
    match id {
        IdentityMaterial::Pem {
            certificate,
            private_key,
        } => {
            let cert = materialize_pem_identity_one(*certificate, tmp, "client_cert")?;
            let key = materialize_pem_identity_one(*private_key, tmp, "client_key")?;
            Ok(Some((cert, key)))
        }
        IdentityMaterial::Jks {
            path,
            password,
            key_alias,
        } => {
            let path = Path::new(path);
            let base = ensure_tmp_root(tmp, "keystore-jks")?;
            let cert_out = base.join("client-cert.pem");
            let key_out = base.join("client-key.pem");
            jks_client_keystore_to_pem_files(path, password, *key_alias, &cert_out, &key_out)?;
            Ok(Some((
                cert_out.to_string_lossy().into_owned(),
                key_out.to_string_lossy().into_owned(),
            )))
        }
        IdentityMaterial::P12 { path, password } => {
            let p12_path = Path::new(path);
            let base = ensure_tmp_root(tmp, "keystore-p12")?;
            let cert_out = base.join("client-cert.pem");
            let key_out = base.join("client-key.pem");
            openssl_pkcs12_extract(p12_path, password, &cert_out, Pkcs12Extract::ClientCert)?;
            openssl_pkcs12_extract(p12_path, password, &key_out, Pkcs12Extract::ClientKey)?;
            Ok(Some((
                cert_out.to_string_lossy().into_owned(),
                key_out.to_string_lossy().into_owned(),
            )))
        }
    }
}

/// 将 [`JavaSslMaterial`] 解析为 PEM 文件路径；失败时返回 [`JavaSslPemError`]。
pub fn materialize_java_ssl_pem(m: &JavaSslMaterial<'_>) -> Result<MaterializedSslPem, JavaSslPemError> {
    let mut tmp: Option<TempDir> = None;
    let ca_file = match &m.trust {
        Some(t) => materialize_trust(t, &mut tmp)?,
        None => None,
    };
    let pair = match &m.identity {
        Some(i) => materialize_identity(i, &mut tmp)?,
        None => None,
    };
    let (certificate_file, key_file) = match pair {
        Some((c, k)) => (Some(c), Some(k)),
        None => (None, None),
    };
    Ok(MaterializedSslPem {
        ca_file,
        certificate_file,
        key_file,
        scratch_dir: tmp,
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

    /// 与 `logspout-worker/src/jks_fixture.rs` 保持一致（勿与真实环境口令混用）。
    const FIXTURE_TRUSTSTORE_PASSWORD: &str = "vKFoWrbf_El1pCtcUVHZn0ygI5Mu8izQ";
    const FIXTURE_KEYSTORE_PASSWORD: &str =
        "8c4804e1504aa139bd827c9c016f11d4cc7174a95352f5068a3cb2c1f4849e91";

    /// 测试内容：JKS truststore fixture 导出为 CA PEM。
    /// 输入：相对本 crate 的 `../assets/truststore.jks` 与 fixture 口令。
    /// 预期：写出 PEM 且含 `BEGIN CERTIFICATE`。
    #[test]
    fn jks_truststore_fixture_emits_ca_pem() {
        let ts = worker_assets().join("truststore.jks");
        assert!(ts.is_file(), "missing fixture {}", ts.display());
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("ca.pem");
        jks_truststore_to_ca_pem(&ts, FIXTURE_TRUSTSTORE_PASSWORD, &out).expect("truststore → PEM");
        let pem = std::fs::read_to_string(&out).unwrap();
        assert!(
            pem.contains("BEGIN CERTIFICATE") || pem.contains("BEGIN TRUSTED CERTIFICATE"),
            "expected certificate PEM"
        );
    }

    /// 测试内容：JKS keystore fixture 导出客户端 cert/key PEM。
    /// 输入：`../assets/keystore.jks`。
    /// 预期：证书与私钥 PEM 块存在。
    #[test]
    fn jks_keystore_fixture_emits_client_cert_and_key_pem() {
        let ks = worker_assets().join("keystore.jks");
        assert!(ks.is_file(), "missing fixture {}", ks.display());
        let dir = tempfile::tempdir().unwrap();
        let cert_out = dir.path().join("c.pem");
        let key_out = dir.path().join("k.pem");
        jks_client_keystore_to_pem_files(&ks, FIXTURE_KEYSTORE_PASSWORD, None, &cert_out, &key_out)
            .expect("keystore → PEM");
        let cert = std::fs::read_to_string(&cert_out).unwrap();
        let key = std::fs::read_to_string(&key_out).unwrap();
        assert!(cert.contains("BEGIN CERTIFICATE"), "cert PEM");
        assert!(
            key.contains("BEGIN PRIVATE KEY") || key.contains("BEGIN RSA PRIVATE KEY"),
            "key PEM"
        );
    }

    /// 测试内容：`truststore.jks` / `keystore.jks` 魔数。
    /// 输入：`../assets/*.jks`。
    /// 预期：文件头 `FE ED FE ED`。
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
    /// 输入：`../assets` 下 `truststore.jks` / `keystore.jks`。
    /// 预期：写出 `ca_file` 与 cert/key 路径且文件存在。
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
        let ca = out.ca_file.as_ref().expect("ca");
        assert!(Path::new(ca).is_file(), "{ca}");
        let c = out.certificate_file.as_ref().expect("cert");
        let k = out.key_file.as_ref().expect("key");
        assert!(Path::new(c).is_file());
        assert!(Path::new(k).is_file());
    }
}

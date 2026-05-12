//! Java **`.jks`** truststore / keystore → PEM：由 **`jks`** crate 解析 JKS 并写出临时 PEM，供 **librdkafka**（`ssl.*.location`）使用。
//!
//! **`.p12`/`.pfx`** 仍用 **`openssl pkcs12`**（须 `PATH` 中有 `openssl`）。**`.jks`** 由 **`jks`** 解析。客户端 JKS 含**多个私钥**时：未配 **`ssl.keystore.alias`** 则取**私钥别名升序第一个**（稳定、贴近「只配 location+password」的常见 Java 行为；与某 JDK 下 KeyManager 的遍历顺序**不保证逐字节一致**，需完全一致时请显式写 alias）。

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;

use jks::{KeyStore, KeyStoreOptions};
use logspout_dsl::KafkaConfig;
use pem::Pem;
use rdkafka::config::ClientConfig;
use tempfile::TempDir;

fn err(msg: impl Into<String>) -> String {
    msg.into()
}

fn hostname_verify_enabled(k: &KafkaConfig) -> bool {
    match &k.ssl_endpoint_identification_algorithm {
        None => true,
        Some(s) => !s.trim().is_empty(),
    }
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

fn is_pem_trust_ext(ext: &str) -> bool {
    matches!(ext, "pem" | "crt" | "cer" | "ca-bundle" | "bundle" | "der")
}

fn is_truststore_pem_or_der_filename(ext: &str) -> bool {
    is_pem_trust_ext(ext) || ext.is_empty()
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

fn read_file_head(path: &Path, max: usize) -> Result<Vec<u8>, String> {
    let mut f = fs::File::open(path).map_err(|e| err(format!("open {}: {e}", path.display())))?;
    let mut buf = vec![0u8; max];
    let n = f
        .read(&mut buf)
        .map_err(|e| err(format!("read {}: {e}", path.display())))?;
    buf.truncate(n);
    Ok(buf)
}

fn validate_x509_der(der: &[u8], what: &str) -> Result<(), String> {
    if der.len() < 32 {
        return Err(err(format!("{what}: DER too short")));
    }
    if der[0] != 0x30 {
        return Err(err(format!("{what}: DER does not start with SEQUENCE (0x30)")));
    }
    Ok(())
}

/// 规范换行并以单个 `\n` 结尾，供 OpenSSL/librdkafka 稳定解析。
fn write_pem_text_file(path: &Path, pem_text: &str) -> Result<(), String> {
    let mut s = pem_text.replace("\r\n", "\n");
    s = s.trim().to_string();
    if s.is_empty() {
        return Err(err(format!("write {}: empty PEM", path.display())));
    }
    if !s.ends_with('\n') {
        s.push('\n');
    }
    fs::write(path, s.as_bytes()).map_err(|e| err(format!("write {}: {e}", path.display())))?;
    Ok(())
}

fn load_jks(path: &Path, password: &str) -> Result<KeyStore, String> {
    let bytes = fs::read(path).map_err(|e| err(format!("read {}: {e}", path.display())))?;
    let mut ks = KeyStore::with_options(KeyStoreOptions {
        ordered_aliases: true,
        ..Default::default()
    });
    ks.load(&mut bytes.as_slice(), password.as_bytes())
        .map_err(|e| err(format!("JKS {}: {e}", path.display())))?;
    Ok(ks)
}

fn jks_truststore_to_ca_pem(jks: &Path, password: &str, out: &Path) -> Result<(), String> {
    let ks = load_jks(jks, password)?;
    let mut pem = String::new();
    let mut n = 0usize;
    for alias in ks.aliases() {
        if ks.is_trusted_certificate_entry(&alias) {
            let tce = ks
                .get_trusted_certificate_entry(&alias)
                .map_err(|e| err(format!("JKS trusted entry {alias:?}: {e}")))?;
            if !tce.certificate.cert_type.eq_ignore_ascii_case("X509") {
                return Err(err(format!(
                    "JKS {} alias={alias:?}: unsupported cert type {:?}",
                    jks.display(),
                    tce.certificate.cert_type
                )));
            }
            pem.push_str(&der_to_pem("CERTIFICATE", &tce.certificate.content));
            if !pem.ends_with('\n') {
                pem.push('\n');
            }
            n += 1;
        }
    }
    if n == 0 {
        return Err(err(format!(
            "JKS truststore {}: no TrustedCertificateEntry (wrong file or empty truststore)",
            jks.display()
        )));
    }
    write_pem_text_file(out, &pem)?;
    Ok(())
}

fn resolve_client_keystore_alias(
    ks: &KeyStore,
    yaml_alias: Option<&str>,
) -> Result<String, String> {
    let mut pk_aliases: Vec<String> = ks
        .aliases()
        .into_iter()
        .filter(|a| ks.is_private_key_entry(a))
        .collect();
    pk_aliases.sort();
    match pk_aliases.len() {
        0 => Err(err(
            "JKS client keystore: no PrivateKeyEntry (need a key entry for mTLS)",
        )),
        1 => Ok(pk_aliases.remove(0)),
        _ => {
            if let Some(want) = yaml_alias.map(str::trim).filter(|s| !s.is_empty()) {
                let want_l = want.to_lowercase();
                for a in &pk_aliases {
                    if a.to_lowercase() == want_l {
                        return Ok(a.clone());
                    }
                }
                return Err(err(format!(
                    "ssl.keystore.alias={want:?} does not match any private-key alias; available: {}",
                    pk_aliases.join(", ")
                )));
            }
            // 与「只配 location + password」的 Java 客户端常见体验对齐：多私钥时由运行时选一个。
            // JVM 实际顺序依赖 JDK/算法；此处取**私钥别名升序第一个**，稳定且可预测；需与 Java 完全一致时用 ssl.keystore.alias。
            Ok(pk_aliases.remove(0))
        }
    }
}

fn jks_client_keystore_to_pem_files(
    jks: &Path,
    password: &str,
    entry_alias: Option<&str>,
    cert_out: &Path,
    key_out: &Path,
) -> Result<(), String> {
    let ks = load_jks(jks, password)?;
    let alias = resolve_client_keystore_alias(&ks, entry_alias)?;
    let pke = ks
        .get_private_key_entry(&alias, password.as_bytes())
        .map_err(|e| err(format!("JKS decrypt private key {}: {e}", jks.display())))?;

    let mut cert_pem = String::new();
    for cert in &pke.certificate_chain {
        if !cert.cert_type.eq_ignore_ascii_case("X509") {
            return Err(err(format!(
                "JKS {}: unsupported certificate type {:?}",
                jks.display(),
                cert.cert_type
            )));
        }
        cert_pem.push_str(&der_to_pem("CERTIFICATE", &cert.content));
        if !cert_pem.ends_with('\n') {
            cert_pem.push('\n');
        }
    }
    if cert_pem.trim().is_empty() {
        return Err(err(format!(
            "JKS {}: empty certificate chain for alias {alias:?}",
            jks.display()
        )));
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
) -> Result<(), String> {
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
        let output = cmd
            .output()
            .map_err(|e| err(format!("running openssl: {e}")))?;
        if output.status.success() {
            return Ok(());
        }
        last_err = String::from_utf8_lossy(&output.stderr).into_owned();
        if legacy {
            return Err(err(format!("openssl pkcs12 failed: {last_err}")));
        }
    }
    Err(err(format!("openssl pkcs12 failed: {last_err}")))
}

fn ensure_tmp_root(tmp: &mut Option<TempDir>, label: &str) -> Result<PathBuf, String> {
    if tmp.is_none() {
        *tmp = Some(
            TempDir::new().map_err(|e| err(format!("kafka TLS temp directory ({label}): {e}")))?,
        );
    }
    Ok(tmp.as_ref().expect("just set").path().to_path_buf())
}

fn der_trust_file_to_temp_pem(path: &Path, tmp: &mut Option<TempDir>, label: &str) -> Result<String, String> {
    let bytes = fs::read(path).map_err(|e| err(format!("read {}: {e}", path.display())))?;
    validate_x509_der(&bytes, "ssl.ca / trust anchor")?;
    let base = ensure_tmp_root(tmp, label)?;
    let out = base.join("ca-from-der.pem");
    let pem = der_to_pem("CERTIFICATE", &bytes);
    write_pem_text_file(&out, &pem)?;
    Ok(out.to_string_lossy().into_owned())
}

/// 信任锚文件：支持 PEM 文本，或**单张** X.509 DER（常见 `.cer` 二进制）；JKS/P12 须走 `ssl.truststore.location`。
fn materialize_ca_trust_file_path(
    path: &Path,
    tmp: &mut Option<TempDir>,
    label: &str,
) -> Result<String, String> {
    if !path.is_file() {
        return Err(err(format!(
            "{label}: not a regular file: {}",
            path.display()
        )));
    }
    let ext = ext_lower(path);
    if is_jks_p12_pfx(&ext) {
        return Err(err(format!(
            "{label}={}: for Java truststores use ssl.truststore.location + ssl.truststore.password",
            path.display()
        )));
    }
    let head = read_file_head(path, 4096)?;
    if looks_like_pem_bytes(&head) {
        let head_s = String::from_utf8_lossy(&head);
        if !(head_s.contains("BEGIN CERTIFICATE")
            || head_s.contains("BEGIN TRUSTED CERTIFICATE"))
        {
            return Err(err(format!(
                "{label} {}: PEM must be a CA / certificate bundle (-----BEGIN CERTIFICATE----- or TRUSTED CERTIFICATE)",
                path.display()
            )));
        }
        return Ok(path.to_string_lossy().into_owned());
    }
    if looks_like_der_x509(&head) {
        return der_trust_file_to_temp_pem(path, tmp, label);
    }
    Err(err(format!(
        "{label} {}: expected PEM (-----BEGIN ...) or a single X.509 DER certificate file (often .cer); OpenSSL refused to load it",
        path.display()
    )))
}

fn materialize_ssl_ca_pem_field(p: &str, tmp: &mut Option<TempDir>) -> Result<String, String> {
    let t = p.trim();
    if t.is_empty() {
        return Err(err("ssl.ca.pem: empty"));
    }
    if t.starts_with("-----BEGIN") {
        let base = ensure_tmp_root(tmp, "ca-pem-inline")?;
        let out = base.join("ca-inline.pem");
        write_pem_text_file(&out, t)?;
        return Ok(out.to_string_lossy().into_owned());
    }
    materialize_ca_trust_file_path(Path::new(t), tmp, "ssl.ca.pem")
}

fn materialize_ssl_ca_location_field(p: &str, tmp: &mut Option<TempDir>) -> Result<String, String> {
    let t = p.trim();
    if t.is_empty() {
        return Err(err("ssl.ca.location: empty"));
    }
    if t.starts_with("-----BEGIN") {
        let base = ensure_tmp_root(tmp, "ca-location-inline")?;
        let out = base.join("ca-inline.pem");
        write_pem_text_file(&out, t)?;
        return Ok(out.to_string_lossy().into_owned());
    }
    materialize_ca_trust_file_path(Path::new(t), tmp, "ssl.ca.location")
}

/// 解析 `ssl.ca.*` / `ssl.truststore.location`，必要时把 JKS/P12 转成临时目录下的 PEM 路径。
fn materialize_trust_anchor_pem_path(
    k: &KafkaConfig,
    tmp: &mut Option<TempDir>,
) -> Result<Option<String>, String> {
    if let Some(p) = k
        .ssl_ca_pem
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        return Ok(Some(materialize_ssl_ca_pem_field(p, tmp)?));
    }
    if let Some(p) = k
        .ssl_ca_location
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        return Ok(Some(materialize_ssl_ca_location_field(p, tmp)?));
    }
    if let Some(p) = k
        .ssl_truststore_location
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let path = Path::new(p);
        let ext = ext_lower(path);
        if is_truststore_pem_or_der_filename(&ext) {
            return Ok(Some(materialize_ca_trust_file_path(
                path,
                tmp,
                "ssl.truststore.location",
            )?));
        }
        let pwd = k.ssl_truststore_password.as_deref().unwrap_or("");
        if ext == "jks" {
            let base = ensure_tmp_root(tmp, "truststore-jks")?;
            let out = base.join("ca-chain.pem");
            jks_truststore_to_ca_pem(path, pwd, &out)?;
            return Ok(Some(out.to_string_lossy().into_owned()));
        }
        if ext == "p12" || ext == "pfx" {
            let base = ensure_tmp_root(tmp, "truststore-p12")?;
            let out = base.join("ca-chain.pem");
            openssl_pkcs12_extract(path, pwd, &out, Pkcs12Extract::CaChain)?;
            return Ok(Some(out.to_string_lossy().into_owned()));
        }
        return Err(err(format!(
            "ssl.truststore.location={p:?}: expected .jks, .p12/.pfx, or CA file (.pem/.crt/.cer/.der or extensionless PEM/DER)"
        )));
    }
    Ok(None)
}

fn materialize_client_pem_pair(
    k: &KafkaConfig,
    tmp: &mut Option<TempDir>,
) -> Result<Option<(String, String)>, String> {
    let cert = k
        .ssl_certificate_pem
        .as_deref()
        .or(k.ssl_certificate_location.as_deref());
    let key = k
        .ssl_private_key_pem
        .as_deref()
        .or(k.ssl_key_location.as_deref())
        .or(k.ssl_key_pem.as_deref());

    if let (Some(c), Some(ke)) = (cert, key) {
        return Ok(Some((c.trim().to_string(), ke.trim().to_string())));
    }
    if cert.is_some() ^ key.is_some() {
        return Err(err(
            "SSL client auth: provide both PEM certificate and private key paths, or ssl.keystore.location + ssl.keystore.password for .jks/.p12",
        ));
    }

    let Some(ks) = k
        .ssl_keystore_location
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    else {
        return Ok(None);
    };
    let path = Path::new(ks);
    let ext = ext_lower(path);
    if !is_jks_p12_pfx(&ext) {
        return Err(err(format!(
            "ssl.keystore.location={ks:?}: expected .jks, .p12, or .pfx when not using PEM cert/key fields"
        )));
    }
    let pwd = k.ssl_keystore_password.as_deref().unwrap_or("");
    let base = ensure_tmp_root(tmp, "keystore")?;

    if ext == "jks" {
        let cert_out = base.join("client-cert.pem");
        let key_out = base.join("client-key.pem");
        jks_client_keystore_to_pem_files(
            path,
            pwd,
            k.ssl_keystore_alias.as_deref(),
            &cert_out,
            &key_out,
        )?;
        return Ok(Some((
            cert_out.to_string_lossy().into_owned(),
            key_out.to_string_lossy().into_owned(),
        )));
    }

    let p12_path = path.to_path_buf();
    let openssl_pass = pwd.to_string();

    let cert_out = base.join("client-cert.pem");
    let key_out = base.join("client-key.pem");
    openssl_pkcs12_extract(
        &p12_path,
        &openssl_pass,
        &cert_out,
        Pkcs12Extract::ClientCert,
    )?;
    openssl_pkcs12_extract(&p12_path, &openssl_pass, &key_out, Pkcs12Extract::ClientKey)?;

    Ok(Some((
        cert_out.to_string_lossy().into_owned(),
        key_out.to_string_lossy().into_owned(),
    )))
}

/// 将 PEM 路径写入 librdkafka（`ssl.ca.location`、`ssl.certificate.location`、`ssl.key.location`）；若从 JKS/P12 转换则返回 [`TempDir`] 须与 producer 同生命周期。
pub(crate) fn configure_librdkafka_ssl(
    cfg: &mut ClientConfig,
    k: &KafkaConfig,
) -> Result<Option<TempDir>, String> {
    let mut tmp: Option<TempDir> = None;
    if let Some(ca) = materialize_trust_anchor_pem_path(k, &mut tmp)? {
        cfg.set("ssl.ca.location", &ca);
    }
    if let Some((cert, key)) = materialize_client_pem_pair(k, &mut tmp)? {
        cfg.set("ssl.certificate.location", &cert);
        cfg.set("ssl.key.location", &key);
    }
    let verify = hostname_verify_enabled(k);
    cfg.set(
        "enable.ssl.certificate.verification",
        if verify { "true" } else { "false" },
    );
    Ok(tmp)
}

#[cfg(test)]
mod assets_jks_tests {
    //! 仓库 `assets/*.jks` fixture；各用例意图见紧邻 `#[test]` 的 `///` 三段说明。

    use std::io::Read;
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::kafka_smoke::{
        kafka_config_fixture_jks_dir, FIXTURE_BOOTSTRAP_BROKER, FIXTURE_KEYSTORE_PASSWORD,
        FIXTURE_TRUSTSTORE_PASSWORD,
    };
    use tempfile::tempdir;

    fn assets_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("assets")
    }

    fn fixture_kafka_config() -> KafkaConfig {
        kafka_config_fixture_jks_dir(FIXTURE_BOOTSTRAP_BROKER, "fixture", &assets_dir(), false)
    }

    /// 测试内容：fixture JKS 文件存在且魔数为 Java JKS（`0xFEEDFEED`）。
    /// 输入：`CARGO_MANIFEST_DIR/assets/{keystore,truststore}.jks`。
    /// 预期：两个文件可读；文件头 4 字节为 `FE ED FE ED`。
    #[test]
    fn assets_jks_files_exist_and_magic() {
        let ks = assets_dir().join("keystore.jks");
        let ts = assets_dir().join("truststore.jks");
        assert!(ks.is_file(), "missing {}", ks.display());
        assert!(ts.is_file(), "missing {}", ts.display());
        for path in [&ks, &ts] {
            let mut f = std::fs::File::open(path).unwrap();
            let mut b = [0u8; 4];
            f.read_exact(&mut b).unwrap();
            assert_eq!(b, [0xFE, 0xED, 0xFE, 0xED], "{}", path.display());
        }
    }

    /// 测试内容：fixture `truststore.jks` 可解析为 CA PEM 链。
    /// 输入：`assets/truststore.jks` 与 fixture 口令；输出到临时目录下 `ca.pem`。
    /// 预期：`jks_truststore_to_ca_pem` 成功；PEM 含 `BEGIN CERTIFICATE` 或 `BEGIN TRUSTED CERTIFICATE`。
    #[test]
    fn jks_truststore_fixture_emits_ca_pem() {
        let dir = tempdir().unwrap();
        let out = dir.path().join("ca.pem");
        jks_truststore_to_ca_pem(
            &assets_dir().join("truststore.jks"),
            FIXTURE_TRUSTSTORE_PASSWORD,
            &out,
        )
        .expect("truststore → PEM");
        let pem = std::fs::read_to_string(&out).unwrap();
        assert!(
            pem.contains("BEGIN CERTIFICATE") || pem.contains("BEGIN TRUSTED CERTIFICATE"),
            "expected certificate PEM, got {} bytes",
            pem.len()
        );
    }

    /// 测试内容：fixture `keystore.jks` 可导出客户端证书链与私钥 PEM。
    /// 输入：`assets/keystore.jks`、fixture 口令、无 `ssl.keystore.alias`（默认私钥别名）；临时 `c.pem` / `k.pem`。
    /// 预期：证书 PEM 含 `BEGIN CERTIFICATE`；私钥 PEM 含 `BEGIN PRIVATE KEY` 或 `BEGIN RSA PRIVATE KEY`。
    #[test]
    fn jks_keystore_fixture_emits_client_cert_and_key_pem() {
        let dir = tempdir().unwrap();
        let cert_out = dir.path().join("c.pem");
        let key_out = dir.path().join("k.pem");
        jks_client_keystore_to_pem_files(
            &assets_dir().join("keystore.jks"),
            FIXTURE_KEYSTORE_PASSWORD,
            None,
            &cert_out,
            &key_out,
        )
        .expect("keystore → PEM");
        let cert = std::fs::read_to_string(&cert_out).unwrap();
        let key = std::fs::read_to_string(&key_out).unwrap();
        assert!(cert.contains("BEGIN CERTIFICATE"), "cert PEM");
        assert!(
            key.contains("BEGIN PRIVATE KEY") || key.contains("BEGIN RSA PRIVATE KEY"),
            "key PEM"
        );
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
}

//! DER → PEM 编码（JKS 导出用）。

use pem::Pem;

use crate::JavaSslPemError;

pub(crate) fn der_to_pem(tag: &str, der: &[u8]) -> String {
    pem::encode(&Pem::new(tag, der.to_vec()))
}

pub(crate) fn push_cert_der(pem: &mut String, der: &[u8]) {
    pem.push_str(&der_to_pem("CERTIFICATE", der));
    if !pem.ends_with('\n') {
        pem.push('\n');
    }
}

/// 规范换行并以单个 `\n` 结尾，供 TLS 栈稳定解析。
pub(crate) fn normalize_pem_document(s: &str) -> Result<String, JavaSslPemError> {
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

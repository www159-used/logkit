//! 解密 spine 密文：动态加载 `librootkey_crypto.so`（dlopen，无 Python 依赖）。

mod spine;

use std::path::Path;

pub use spine::{default_library_path, linux_lib_arch, pull_out_spine, set_verbose, SpineError};

/// 解密密文字符串，返回明文（UTF-8）。
pub fn pull_out(ciphertext: &str) -> Result<String, SpineError> {
    pull_out_spine(ciphertext)
}

/// 解密 key 材料：先整段交给 [`pull_out`] / so（不区分 key_id）；失败则原样返回。
pub fn decrypt_key_material(raw: &str) -> Result<String, SpineError> {
    let trimmed = raw.trim();
    match pull_out(trimmed) {
        Ok(plain) => Ok(plain),
        Err(_) => Ok(trimmed.to_string()),
    }
}

/// 读 key 文件并 [`decrypt_key_material`]。
pub fn decrypt_key_file(path: &Path) -> Result<String, SpineError> {
    let raw = std::fs::read_to_string(path).map_err(|source| SpineError::ReadWorkKey {
        path: path.to_path_buf(),
        source,
    })?;
    decrypt_key_material(&raw)
}

#[cfg(test)]
mod key_tests {
    use super::*;

    /// 测试内容：so 解不开时（含明文 PEM）回退原文。
    #[test]
    fn decrypt_falls_back_to_raw_when_pull_out_fails() {
        let pem = "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----";
        assert_eq!(decrypt_key_material(pem).unwrap(), pem);

        let token = "certpk:yb:v1:abc";
        assert_eq!(decrypt_key_material(token).unwrap(), token);
    }
}

//! 解密 spine 密文：动态加载 `librootkey_crypto.so`（dlopen，无 Python 依赖）。

mod spine;

use std::path::Path;

pub use spine::{default_library_path, linux_lib_arch, pull_out_spine, set_verbose, SpineError};

/// 解密密文字符串，返回明文（UTF-8）。
pub fn pull_out(ciphertext: &str) -> Result<String, SpineError> {
    pull_out_spine(ciphertext)
}

/// 解密 key 材料：`config:...` spine 密文走 [`pull_out`]，否则视为明文 PEM。
pub fn decrypt_key_material(raw: &str) -> Result<String, SpineError> {
    let trimmed = raw.trim();
    if trimmed.starts_with("config:") {
        pull_out(trimmed)
    } else {
        Ok(trimmed.to_string())
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

    /// 测试内容：非 `config:` 前缀的 key 材料原样返回。
    /// 输入：标准 PEM 私钥片段字符串。
    /// 预期：[`decrypt_key_material`] 成功且输出与输入相同。
    #[test]
    fn decrypt_plain_pem_passthrough() {
        let pem = "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----";
        assert_eq!(decrypt_key_material(pem).unwrap(), pem);
    }
}

//! 解密 spine 密文：动态加载 `librootkey_crypto.so`（dlopen，无 Python 依赖）。

mod spine;

pub use spine::{default_library_path, linux_lib_arch, pull_out_spine, set_verbose, SpineError};

/// 解密密文字符串，返回明文（UTF-8）。
pub fn pull_out(ciphertext: &str) -> Result<String, SpineError> {
    pull_out_spine(ciphertext)
}

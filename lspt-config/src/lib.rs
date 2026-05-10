//! Shared configuration (TOML), embedded reference defaults, and [`LsptError`].
//!
//! CLI 可用 **`LSPT_DEFAULTS_FILE`** 指向 TOML（等价于前置 `--defaults-file`），见 [`parse_cli_args`]。

mod config;
mod embed;
mod error;

pub use config::{
    load_merged, ClientSection, DaemonSection, GrpcSection, LogServerSection, LsptConfig,
    ProtocolSection,
};
pub use error::LsptError;

use std::path::PathBuf;

/// 若命令行未出现 `--defaults-file` 且设置了环境变量 **`LSPT_DEFAULTS_FILE`**（非空路径），
/// 则等价于在参数最前面插入 `--defaults-file <path>`，便于 `lspt` / `lsptd` 与示例 TOML 对齐。
fn inject_defaults_file_from_env(args: Vec<String>) -> Vec<String> {
    if args.iter().any(|a| a == "--defaults-file") {
        return args;
    }
    let Ok(p) = std::env::var("LSPT_DEFAULTS_FILE") else {
        return args;
    };
    let p = p.trim();
    if p.is_empty() {
        return args;
    }
    let mut out = vec!["--defaults-file".to_string(), p.to_string()];
    out.extend(args);
    out
}

/// Strip `--defaults-file <path>` from argv；可与 [`inject_defaults_file_from_env`] 配合。
/// 返回 `(覆盖用 TOML 路径, 余下子命令与参数)`。
pub fn parse_cli_args(args: Vec<String>) -> Result<(Option<PathBuf>, Vec<String>), LsptError> {
    let args = inject_defaults_file_from_env(args);
    let mut defaults = None;
    let mut rest = Vec::new();
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--defaults-file" {
            let path = args
                .get(i + 1)
                .ok_or_else(|| {
                    LsptError::Cli("--defaults-file must be followed by a path".into())
                })?
                .clone();
            defaults = Some(PathBuf::from(path));
            i += 2;
        } else {
            rest.push(args[i].clone());
            i += 1;
        }
    }
    Ok((defaults, rest))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn defaults_file_env_and_flag_precedence() {
        std::env::set_var("LSPT_DEFAULTS_FILE", "/tmp/x.toml");
        let (d, rest) = parse_cli_args(vec!["ping".into()]).expect("parse");
        assert_eq!(d.as_deref(), Some(Path::new("/tmp/x.toml")));
        assert_eq!(rest, vec!["ping".to_string()]);

        let (d2, _) = parse_cli_args(vec![
            "--defaults-file".into(),
            "/tmp/y.toml".into(),
            "ping".into(),
        ])
        .expect("parse");
        assert_eq!(d2.as_deref(), Some(Path::new("/tmp/y.toml")));

        std::env::remove_var("LSPT_DEFAULTS_FILE");
    }
}

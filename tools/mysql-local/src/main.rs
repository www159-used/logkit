//! 内部工具：连接本机 MySQL（路径与用户随 OEM 解析）。

use std::ffi::OsString;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use clap::Parser;

fn mysql_bin_path() -> PathBuf {
    resolve_oem::mysql_bin_path()
}

fn mysql_socket_path() -> PathBuf {
    resolve_oem::mysql_socket_path()
}

/// 未指定 `-u` 时按顺序尝试的默认用户（去重）。
fn default_user_candidates(oem: &str) -> Vec<String> {
    let mut out = vec![format!("{oem}_root"), "root".into()];
    out.dedup();
    out
}

/// 将 `-psecret`、`-uroot` 拆成 clap 可识别的 `-p` / `-u` 与独立值（不处理 `--password=` 等长选项）。
fn normalize_mysql_style_flags(args: impl IntoIterator<Item = OsString>) -> Vec<OsString> {
    /// `-p` 粘连时，下列后缀更像 mysql 其它短选项而非密码，不拆分。
    const P_SUFFIX_DENY: &[&str] = &["rotocol", "ort", "ipe", "ager", "rint", "erform"];

    fn split_attached(prefix: &str, arg: &str, deny_suffix: &[&str]) -> Option<(String, String)> {
        if arg.starts_with("--") || !arg.starts_with(prefix) {
            return None;
        }
        let rest = &arg[prefix.len()..];
        if rest.is_empty() || rest.starts_with('-') {
            return None;
        }
        if prefix == "-p"
            && deny_suffix
                .iter()
                .any(|d| rest == *d || rest.starts_with(d))
        {
            return None;
        }
        Some((prefix.to_string(), rest.to_string()))
    }

    let mut out = Vec::new();
    for arg in args {
        let Some(s) = arg.to_str() else {
            out.push(arg);
            continue;
        };
        if let Some((flag, value)) =
            split_attached("-p", s, P_SUFFIX_DENY).or_else(|| split_attached("-u", s, &[]))
        {
            out.push(OsString::from(flag));
            out.push(OsString::from(value));
        } else {
            out.push(arg);
        }
    }
    out
}

/// `mysql_local` 自身识别的选项；其余参数透传给 `mysql`。
#[derive(Parser, Debug)]
#[command(
    name = "mysql_local",
    about = "连接本机 MySQL（路径与用户随 OEM 解析）",
    disable_version_flag = true,
    disable_help_flag = true
)]
struct Cli {
    /// MySQL 用户（省略时按 `{oem}_root`、`root` 顺序探测）
    #[arg(short = 'u', long = "user")]
    user: Option<String>,

    /// 密码：`-p` / `-ppassword` / `--password=...`；省略时从控制台读取
    #[arg(
        short = 'p',
        long = "password",
        num_args = 0..=1,
        default_missing_value = ""
    )]
    password: Option<String>,

    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    mysql_args: Vec<OsString>,
}

fn read_password_from_tty() -> String {
    eprint!("Enter password: ");
    let _ = io::stderr().flush();
    rpassword::read_password().unwrap_or_else(|e| {
        eprintln!("mysql_local: 读取密码失败: {e}");
        std::process::exit(1);
    })
}

fn resolve_password(opt: Option<String>) -> String {
    match opt {
        Some(s) if !s.is_empty() => s,
        _ => read_password_from_tty(),
    }
}

fn mysql_connect_ok(bin: &Path, socket: &Path, user: &str, password: &str) -> bool {
    Command::new(bin)
        .arg(format!("-u{user}"))
        .arg(format!("-S{}", socket.display()))
        .arg(format!("-p{password}"))
        .arg("-e")
        .arg("SELECT 1")
        .arg("--ssl-mode=REQUIRED")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// 显式 `-u` 时仅用该用户；否则在 [`default_user_candidates`] 中探测第一个可连用户。
fn resolve_user(
    oem: &str,
    explicit: Option<String>,
    password: &str,
    bin: &Path,
    socket: &Path,
) -> String {
    if let Some(user) = explicit {
        return user;
    }
    let candidates = default_user_candidates(oem);
    for user in &candidates {
        if mysql_connect_ok(bin, socket, user, password) {
            return user.clone();
        }
    }
    eprintln!(
        "mysql_local: 无法用以下用户连接 {}: {}",
        socket.display(),
        candidates.join(", ")
    );
    std::process::exit(1);
}

fn parse_cli(argv: impl IntoIterator<Item = OsString>) -> Cli {
    let argv: Vec<OsString> = std::iter::once(OsString::from("mysql_local"))
        .chain(normalize_mysql_style_flags(argv))
        .collect();
    Cli::try_parse_from(argv).unwrap_or_else(|e| e.exit())
}

fn main() {
    let oem = resolve_oem::oem_name();
    let cli = parse_cli(std::env::args_os().skip(1));

    let password = resolve_password(cli.password);
    let bin = mysql_bin_path();
    let socket = mysql_socket_path();
    let user = resolve_user(&oem, cli.user, &password, &bin, &socket);

    let mut cmd = Command::new(&bin);
    cmd.arg(format!("-u{user}"))
        .arg(format!("-S{}", socket.display()))
        .arg(format!("-p{password}"))
        .arg("--ssl-mode=REQUIRED");
    for arg in cli.mysql_args {
        cmd.arg(arg);
    }
    cmd.stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    let status = cmd.status().unwrap_or_else(|e| {
        eprintln!("mysql_local: 执行 {}: {e}", bin.display());
        std::process::exit(1);
    });
    std::process::exit(status.code().unwrap_or(1));
}

#[cfg(test)]
mod tests {
    use super::*;
    /// 测试内容：本地 wrapper 路径 helper 与 resolve-oem 一致，用户候选随 OEM 变化。
    /// 输入：resolve_oem 默认 OEM（未设 OEM_NAME 时为 yotta）。
    /// 预期：`mysql_bin_path`/`mysql_socket_path` 等于 `resolve_oem::mysql_*`；用户候选为 `{oem}_root` 后 `root`。
    #[test]
    fn paths_and_user_candidates_follow_oem_name() {
        let oem = resolve_oem::oem_name();
        assert_eq!(mysql_bin_path(), resolve_oem::mysql_bin_path());
        assert_eq!(mysql_socket_path(), resolve_oem::mysql_socket_path());
        assert_eq!(
            default_user_candidates(&oem),
            vec![format!("{oem}_root"), "root".into()]
        );
    }

    /// 测试内容：粘连短参规范化。
    /// 输入：`-psecret`、`-uroot`。
    /// 预期：拆成独立 flag 与值。
    #[test]
    fn normalize_splits_attached_short_flags() {
        let out =
            normalize_mysql_style_flags([OsString::from("-psecret"), OsString::from("-uroot")]);
        assert_eq!(
            out,
            vec![
                OsString::from("-p"),
                OsString::from("secret"),
                OsString::from("-u"),
                OsString::from("root"),
            ]
        );
        assert_eq!(
            normalize_mysql_style_flags([OsString::from("-protocol")]),
            vec![OsString::from("-protocol")]
        );
    }

    /// 测试内容：clap 解析 -p 相关参数。
    /// 输入：`-p`、`-psecret`、`--password=abc`。
    /// 预期：`password` 为 `None` / `Some("")` / `Some("secret")` 等。
    #[test]
    fn clap_password_flags() {
        let no_p = parse_cli([]);
        assert_eq!(no_p.password, None);

        let p_only = parse_cli([OsString::from("-p")]);
        assert_eq!(p_only.password.as_deref(), Some(""));

        let inline = parse_cli([OsString::from("-psecret")]);
        assert_eq!(inline.password.as_deref(), Some("secret"));

        let long = parse_cli([OsString::from("--password=abc")]);
        assert_eq!(long.password.as_deref(), Some("abc"));
    }

    /// 测试内容：clap 解析 -u 与透传参数。
    /// 输入：`-u admin mydb`、`-uroot -e SELECT 1`。
    /// 预期：user 被提取，其余进入 `mysql_args`。
    #[test]
    fn clap_user_and_trailing() {
        let p = parse_cli([
            OsString::from("-u"),
            OsString::from("admin"),
            OsString::from("mydb"),
        ]);
        assert_eq!(p.user.as_deref(), Some("admin"));
        assert_eq!(p.mysql_args.len(), 1);
        assert_eq!(p.mysql_args[0], OsString::from("mydb"));

        let p2 = parse_cli([
            OsString::from("-uroot"),
            OsString::from("-e"),
            OsString::from("SELECT 1"),
        ]);
        assert_eq!(p2.user.as_deref(), Some("root"));
        assert_eq!(p2.mysql_args.len(), 2);
    }

    /// 测试内容：`-h` 不被本工具占用，可透传给 mysql。
    /// 输入：`-h 127.0.0.1`。
    /// 预期：出现在 `mysql_args` 中。
    #[test]
    fn clap_forwards_mysql_h_flag() {
        let p = parse_cli([OsString::from("-h"), OsString::from("127.0.0.1")]);
        assert_eq!(
            p.mysql_args,
            vec![OsString::from("-h"), OsString::from("127.0.0.1")]
        );
    }
}

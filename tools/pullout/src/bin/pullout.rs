//! 从 stdin / 参数 / 文件读取 spine 密文，向 stdout 写入明文。

use std::env;
use std::fs;
use std::io::{self, IsTerminal, Read, Write};

fn main() {
    if let Err(e) = run() {
        eprintln!("pullout: {e}");
        std::process::exit(1);
    }
}

fn usage() {
    eprintln!(
        "用法:\n  \
         pullout 'config:...'           # 密文作参数（推荐）\n  \
         pullout -f /path/to/secret    # 从文件读入\n  \
         echo -n 'config:...' | pullout # 管道\n  \
         pullout                       # 交互：粘贴密文后空行结束，或 Ctrl-D\n\
         \n\
         调试日志在 stderr，默认开启；PULLOUT_QUIET=1 可关闭。\n\
         通过 dlopen librootkey_crypto.so 解密；PULLOUT_LIB 可指定 so 路径。"
    );
}

fn read_stdin_until_eof() -> io::Result<String> {
    pullout::trace("reading stdin until EOF (pipe 或 Ctrl-D)...");
    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;
    Ok(input)
}

/// 交互终端：按行读，去掉换行拼成一行（便于粘贴折行的 base64）。
fn read_stdin_interactive() -> io::Result<String> {
    pullout::trace(
        "stdin 是终端：请粘贴密文（可多行），单独一行回车结束；勿只输入 Ctrl+D 前的半截",
    );
    let stdin = io::stdin();
    let mut out = String::new();
    loop {
        pullout::trace("  等待输入...");
        let mut line = String::new();
        let n = stdin.read_line(&mut line)?;
        if n == 0 {
            pullout::trace("  stdin EOF");
            break;
        }
        let t = line.trim_end_matches(['\r', '\n']);
        if t.is_empty() {
            if out.is_empty() {
                continue;
            }
            pullout::trace("  收到空行，结束输入");
            break;
        }
        out.push_str(t);
        pullout::trace(format!("  累计 {} bytes", out.len()));
    }
    Ok(out)
}

fn read_ciphertext() -> io::Result<String> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.iter().any(|a| a == "-h" || a == "--help") {
        usage();
        std::process::exit(0);
    }
    if let Some(pos) = args.iter().position(|a| a == "-f" || a == "--file") {
        let path = args
            .get(pos + 1)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "缺少 -f 文件路径"))?;
        pullout::trace(format!("read file {path}"));
        return fs::read_to_string(path);
    }
    if !args.is_empty() {
        let joined = args.join("");
        pullout::trace(format!("ciphertext from argv ({} bytes)", joined.len()));
        return Ok(joined);
    }
    if io::stdin().is_terminal() {
        read_stdin_interactive()
    } else {
        read_stdin_until_eof()
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    pullout::trace(format!(
        "pullout {} (verbose={}, PULLOUT_QUIET=1 关闭日志)",
        env!("CARGO_PKG_VERSION"),
        pullout::trace_enabled()
    ));
    let input = read_ciphertext()?;
    let token = input.trim();
    if token.is_empty() {
        usage();
        return Err("密文为空".into());
    }
    pullout::trace(format!("ciphertext ready ({} bytes)", token.len()));
    let plain = pullout::pull_out(token)?;
    pullout::trace(format!("writing stdout ({} bytes)", plain.len()));
    io::stdout().write_all(plain.as_bytes())?;
    let _ = io::stdout().flush();
    pullout::trace("exit 0");
    Ok(())
}

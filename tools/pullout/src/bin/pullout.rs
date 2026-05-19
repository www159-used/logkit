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
         pullout -v 'config:...'        # 开启 stderr 调试日志\n  \
         pullout -f /path/to/secret    # 从文件读入\n  \
         echo -n 'config:...' | pullout # 管道\n  \
         pullout                       # 交互：粘贴密文后空行结束，或 Ctrl-D\n\
         \n\
         PULLOUT_LIB 可指定 librootkey_crypto.so 路径。"
    );
}

fn parse_args() -> (bool, Vec<String>) {
    let mut verbose = false;
    let mut rest = Vec::new();
    for arg in env::args().skip(1) {
        match arg.as_str() {
            "-v" | "--verbose" => verbose = true,
            "-h" | "--help" => {
                usage();
                std::process::exit(0);
            }
            s if s.starts_with('-') && s.len() > 2 && !s.starts_with("--") => {
                let mut has_f = false;
                for c in s.chars().skip(1) {
                    match c {
                        'v' => verbose = true,
                        'f' => has_f = true,
                        _ => {}
                    }
                }
                if has_f {
                    rest.push("-f".into());
                }
            }
            _ => rest.push(arg),
        }
    }
    (verbose, rest)
}

fn read_stdin_until_eof() -> io::Result<String> {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;
    Ok(input)
}

/// 交互终端：按行读，去掉换行拼成一行（便于粘贴折行的 base64）。
fn read_stdin_interactive() -> io::Result<String> {
    let stdin = io::stdin();
    let mut out = String::new();
    loop {
        let mut line = String::new();
        let n = stdin.read_line(&mut line)?;
        if n == 0 {
            break;
        }
        let t = line.trim_end_matches(['\r', '\n']);
        if t.is_empty() {
            if out.is_empty() {
                continue;
            }
            break;
        }
        out.push_str(t);
    }
    Ok(out)
}

fn read_ciphertext(args: &[String]) -> io::Result<String> {
    if let Some(pos) = args.iter().position(|a| a == "-f" || a == "--file") {
        let path = args
            .get(pos + 1)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "缺少 -f 文件路径"))?;
        return fs::read_to_string(path);
    }
    if !args.is_empty() {
        return Ok(args.join(""));
    }
    if io::stdin().is_terminal() {
        read_stdin_interactive()
    } else {
        read_stdin_until_eof()
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let (verbose, args) = parse_args();
    pullout::set_verbose(verbose);
    let input = read_ciphertext(&args)?;
    let token = input.trim();
    if token.is_empty() {
        usage();
        return Err("密文为空".into());
    }
    let plain = pullout::pull_out(token)?;
    io::stdout().write_all(plain.as_bytes())?;
    let _ = io::stdout().flush();
    Ok(())
}

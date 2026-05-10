//! lsptd — 守护进程：在约定路径监听 Unix 流套接字，处理最简文本行协议。

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::thread;
use std::{env, fs, io};

/// 与客户端约定：未设置 `LSPT_SOCKET` 时使用此路径（类 MySQL sock 文件习惯）。
const DEFAULT_SOCKET_PATH: &str = "/tmp/lsptd.sock";

fn socket_path() -> String {
    env::var("LSPT_SOCKET").unwrap_or_else(|_| DEFAULT_SOCKET_PATH.to_string())
}

fn handle_client(mut stream: UnixStream) -> io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let line = line.trim_end_matches(['\r', '\n']);

    let response = match line {
        "PING" => "PONG\n".to_string(),
        s if s.starts_with("ECHO ") => format!("{}\n", s),
        "" => "ERR empty line\n".to_string(),
        _ => "ERR unknown command\n".to_string(),
    };

    stream.write_all(response.as_bytes())?;
    stream.flush()?;
    Ok(())
}

#[cfg(unix)]
fn main() -> io::Result<()> {
    let path = socket_path();
    let path_ref = Path::new(&path);

    if path_ref.exists() {
        fs::remove_file(path_ref)?;
    }

    let listener = UnixListener::bind(path_ref)?;
    eprintln!("lsptd listening on {}", path);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(move || {
                    if let Err(e) = handle_client(stream) {
                        eprintln!("client error: {e}");
                    }
                });
            }
            Err(e) => eprintln!("accept error: {e}"),
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn main() {
    eprintln!("lsptd requires Unix domain sockets");
}

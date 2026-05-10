//! lspt — 客户端：连接约定 sock，发送一行命令并打印响应。

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::{env, io};

const DEFAULT_SOCKET_PATH: &str = "/tmp/lsptd.sock";

fn socket_path() -> String {
    env::var("LSPT_SOCKET").unwrap_or_else(|_| DEFAULT_SOCKET_PATH.to_string())
}

fn usage() {
    eprintln!("usage: lspt ping | lspt echo <text>");
}

#[cfg(unix)]
fn main() -> io::Result<()> {
    let mut args: Vec<String> = env::args().skip(1).collect();
    if args.is_empty() {
        usage();
        return Ok(());
    }

    let cmd = args.remove(0);
    let request = match cmd.as_str() {
        "ping" => "PING\n".to_string(),
        "echo" => {
            if args.is_empty() {
                usage();
                return Ok(());
            }
            format!("ECHO {}\n", args.join(" "))
        }
        _ => {
            usage();
            return Ok(());
        }
    };

    let mut stream = UnixStream::connect(socket_path())?;
    stream.write_all(request.as_bytes())?;
    stream.flush()?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    print!("{}", line);
    Ok(())
}

#[cfg(not(unix))]
fn main() {
    eprintln!("lspt requires Unix domain sockets");
}

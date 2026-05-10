//! lspt — CLI：`ping` / `echo` / `list` / `start` / `stop` / `stat`。

use std::env;

use http::Uri;
use hyper_util::rt::TokioIo;
use lspt_proto::lspt_client::LsptClient;
use lspt_proto::{
    EchoRequest, ListServersRequest, PingRequest, StartLogServerRequest, StatServerRequest,
    StopLogServerRequest,
};
use tonic::transport::Endpoint;
use tower::service_fn;

use lspt_config::{load_merged, parse_cli_args, LsptError};

fn usage() {
    eprintln!("usage: lspt [--defaults-file PATH] ping | echo <text> | list | start [CONFIG.json] | stop <id> | stat [id_prefix]");
    eprintln!("  list: id, config_path, alive, healthy (tab-separated)");
    eprintln!("  stat: prefix match on server id; omit prefix to show all; prints eps and details");
}

#[cfg(unix)]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

#[cfg(unix)]
async fn run() -> Result<(), LsptError> {
    let args: Vec<String> = env::args().skip(1).collect();
    let (defaults, mut rest) = parse_cli_args(args)?;
    let cfg = load_merged(defaults.as_deref())?;
    if rest.is_empty() {
        usage();
        return Err(LsptError::Cli("missing subcommand".into()));
    }

    let sock_path = cfg.client.socket_path.clone();
    let max_dec = cfg.protocol.grpc.max_decoding_message_size_bytes as usize;
    let max_enc = cfg.protocol.grpc.max_encoding_message_size_bytes as usize;

    let path = sock_path.clone();
    let endpoint = Endpoint::from_shared(cfg.protocol.grpc.client_connect_uri.clone())
        .map_err(|e| LsptError::Grpc(e.to_string()))?;
    let channel = endpoint
        .connect_with_connector(service_fn(move |_: Uri| {
            let path = path.clone();
            async move {
                let s = tokio::net::UnixStream::connect(path).await?;
                Ok::<_, std::io::Error>(TokioIo::new(s))
            }
        }))
        .await
        .map_err(|e| LsptError::Grpc(e.to_string()))?;

    let mut client = LsptClient::new(channel)
        .max_decoding_message_size(max_dec)
        .max_encoding_message_size(max_enc);

    let cmd = rest.remove(0);
    match cmd.as_str() {
        "ping" => {
            let r = client
                .ping(PingRequest {})
                .await
                .map_err(|s| LsptError::Grpc(s.to_string()))?;
            println!("{}", r.into_inner().pong);
        }
        "echo" => {
            if rest.is_empty() {
                usage();
                return Err(LsptError::Cli("echo needs a payload".into()));
            }
            let r = client
                .echo(EchoRequest {
                    message: rest.join(" "),
                })
                .await
                .map_err(|s| LsptError::Grpc(s.to_string()))?;
            println!("{}", r.into_inner().message);
        }
        "list" => {
            let r = client
                .list_servers(ListServersRequest {})
                .await
                .map_err(|s| LsptError::Grpc(s.to_string()))?;
            for s in r.into_inner().servers {
                println!("{}\t{}\t{}\t{}", s.id, s.config_path, s.alive, s.healthy);
            }
        }
        "stat" => {
            let id_prefix = rest.first().cloned().unwrap_or_default();
            let r = client
                .stat_server(StatServerRequest { id_prefix })
                .await
                .map_err(|s| LsptError::Grpc(s.to_string()))?;
            let list = r.into_inner().servers;
            if list.is_empty() {
                println!("(no matching servers)");
                return Ok(());
            }
            for s in list {
                println!("id:\t\t{}", s.id);
                println!("config_path:\t{}", s.config_path);
                println!("alive:\t\t{}", s.alive);
                println!("healthy:\t{}", s.healthy);
                println!("eps:\t\t{:.3}\t(realtime est.: extrapolated total / uptime)", s.eps);
                println!("eps_interval:\t{:.3}\t(last heartbeat window Δ/Δt)", s.eps_interval);
                println!("events_total:\t{}", s.log_events_total);
                println!("events_est:\t{:.1}", s.log_events_estimated);
                println!("sec_since_hb:\t{:.3}", s.seconds_since_heartbeat);
                println!("hb_timeout_s:\t{}", s.heartbeat_timeout_secs);
                println!("hb_interval_s:\t{}", s.heartbeat_interval_secs);
                println!();
            }
        }
        "start" => {
            let config_path = rest.first().cloned().unwrap_or_default();
            let r = client
                .start_log_server(StartLogServerRequest { config_path })
                .await
                .map_err(|s| LsptError::Grpc(s.to_string()))?;
            let inner = r.into_inner();
            println!("{}\t{}", inner.id, inner.status);
        }
        "stop" => {
            let id = rest.first().cloned().unwrap_or_default();
            if id.is_empty() {
                usage();
                return Err(LsptError::Cli("stop needs <id>".into()));
            }
            let r = client
                .stop_log_server(StopLogServerRequest { id })
                .await
                .map_err(|s| LsptError::Grpc(s.to_string()))?;
            println!("{}", r.into_inner().status);
        }
        _ => {
            usage();
            return Err(LsptError::Cli("unknown subcommand".into()));
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn main() {
    eprintln!("lspt requires Unix domain sockets");
}

//! gRPC client 连接（Unix / TCP）。

use std::path::Path;

use http::Uri;
use hyper_util::rt::TokioIo;
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;

use crate::config::{ClientConnect, LOCAL_GRPC_AUTHORITY_URI};
use crate::LogenError;

/// 按 [`ClientConnect`] 建立 tonic Channel。
pub async fn connect_client_channel(connect: &ClientConnect) -> Result<Channel, LogenError> {
    match connect {
        ClientConnect::Unix { socket } => connect_unix(socket).await,
        ClientConnect::Tcp { host, port } => connect_tcp(host, *port).await,
    }
}

async fn connect_unix(socket: &Path) -> Result<Channel, LogenError> {
    let path = socket.to_path_buf();
    let endpoint = Endpoint::from_static(LOCAL_GRPC_AUTHORITY_URI);
    endpoint
        .connect_with_connector(service_fn(move |_: Uri| {
            let path = path.clone();
            async move {
                let s = tokio::net::UnixStream::connect(path).await?;
                Ok::<_, std::io::Error>(TokioIo::new(s))
            }
        }))
        .await
        .map_err(|e| {
            LogenError::Grpc(format!(
                "transport error on unix socket {}: {e}. Start logend first, or pass -S/--socket, or use -H for tcp remote.",
                socket.display()
            ))
        })
}

async fn connect_tcp(host: &str, port: u16) -> Result<Channel, LogenError> {
    let uri = format!("http://{host}:{port}/");
    Endpoint::from_shared(uri.clone())
        .map_err(|e| LogenError::Grpc(e.to_string()))?
        .connect()
        .await
        .map_err(|e| {
            LogenError::Grpc(format!(
                "tcp connect to {uri} failed: {e}. Check logend [logend].bind/port and firewall."
            ))
        })
}

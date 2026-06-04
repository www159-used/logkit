use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Instant;

use anyhow::{Context, Result};
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use reqwest::Client;
use tokio::net::TcpListener;

const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
];

fn copy_forward_headers(
    in_req: &Request<impl hyper::body::Body>,
    out: reqwest::RequestBuilder,
) -> reqwest::RequestBuilder {
    let mut builder = out;
    for (name, value) in in_req.headers().iter() {
        if name == http::header::HOST {
            continue;
        }
        if HOP_BY_HOP
            .iter()
            .any(|h| name.as_str().eq_ignore_ascii_case(h))
        {
            continue;
        }
        builder = builder.header(name, value);
    }
    builder
}

#[derive(Clone)]
pub struct Mapping {
    pub listen: u16,
    pub upstream_port: u16, // 已由 config::tcp_port 校验
    pub upstream_host: String,
    pub client: Client,
}

impl Mapping {
    pub async fn serve(self) -> Result<()> {
        let addr = SocketAddr::from(([0, 0, 0, 0], self.listen));
        let listener = TcpListener::bind(addr)
            .await
            .with_context(|| format!("bind HTTP :{}", self.listen))?;
        log::info!(
            target: "jumpserver",
            "listening http://0.0.0.0:{} -> https://{}:{}",
            self.listen,
            self.upstream_host,
            self.upstream_port,
        );
        loop {
            let (stream, peer) = listener.accept().await.context("accept")?;
            let mapping = self.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(move |req| {
                    let mapping = mapping.clone();
                    async move { mapping.forward(req, peer).await }
                });
                if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
                    log::warn!(target: "jumpserver", "connection {peer}: {e}");
                }
            });
        }
    }

    async fn forward(
        &self,
        in_req: Request<hyper::body::Incoming>,
        peer: SocketAddr,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        let started = Instant::now();
        let method = in_req.method().clone();
        let path_q = in_req
            .uri()
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("/")
            .to_string();

        let result = self.forward_inner(in_req).await;
        let elapsed = started.elapsed();
        match &result {
            Ok(resp) => log::debug!(
                target: "jumpserver",
                "{method} {path_q} <- {peer} {} {elapsed:?}",
                resp.status(),
            ),
            Err(e) => log::warn!(
                target: "jumpserver",
                "{method} {path_q} <- {peer} ERR {e:#} {elapsed:?}",
            ),
        }
        Ok(result.unwrap_or_else(|e| {
            Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(Full::new(Bytes::from(format!("jumpserver: {e}"))))
                .unwrap()
        }))
    }

    async fn forward_inner(
        &self,
        in_req: Request<hyper::body::Incoming>,
    ) -> Result<Response<Full<Bytes>>> {
        let (parts, body) = in_req.into_parts();
        let body_bytes = body
            .collect()
            .await
            .context("read request body")?
            .to_bytes();

        let url = format!(
            "https://{}:{}{}",
            self.upstream_host,
            self.upstream_port,
            parts
                .uri
                .path_and_query()
                .map(|pq| pq.as_str())
                .unwrap_or("/")
        );

        let method = reqwest::Method::from_bytes(parts.method.as_str().as_bytes())
            .context("invalid HTTP method")?;
        let mut builder = self.client.request(method, &url);
        builder = copy_forward_headers(
            &Request::from_parts(parts.clone(), Full::new(body_bytes.clone())),
            builder,
        );
        if !body_bytes.is_empty() {
            builder = builder.body(body_bytes);
        }

        let upstream = builder.send().await.context("upstream HTTPS request")?;
        let status = upstream.status();
        let headers = upstream.headers().clone();
        let resp_body = upstream.bytes().await.context("read upstream body")?;

        let mut out = Response::builder().status(status);
        for (name, value) in headers.iter() {
            if HOP_BY_HOP
                .iter()
                .any(|h| name.as_str().eq_ignore_ascii_case(h))
            {
                continue;
            }
            out = out.header(name, value);
        }
        out.body(Full::new(resp_body)).context("build response")
    }
}

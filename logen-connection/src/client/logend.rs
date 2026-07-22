use logen_config::{connect_client_channel, ClientConnect};
use logen_proto::logen_client::LogenClient;
use tonic::transport::Channel;

use crate::error::ConnectionError;

pub(super) async fn logen_client(
    connect: &ClientConnect,
) -> Result<LogenClient<Channel>, ConnectionError> {
    let channel = connect_client_channel(connect).await?;
    Ok(LogenClient::new(channel))
}

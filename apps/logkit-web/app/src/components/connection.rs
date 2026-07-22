use crate::i18n::Msg;

use crate::model::{ConnectionId, ConnectionKind};

pub fn kind_label(i18n: crate::i18n::I18n, kind: ConnectionKind) -> &'static str {
    match kind {
        ConnectionKind::Local => i18n.t(Msg::KindLocal),
        ConnectionKind::Remote => i18n.t(Msg::KindRemote),
    }
}

pub fn kind_class(kind: ConnectionKind) -> &'static str {
    match kind {
        ConnectionKind::Local => "badge badge-local",
        ConnectionKind::Remote => "badge badge-remote",
    }
}

pub fn workers_href(connection_id: ConnectionId) -> String {
    format!("/c/{connection_id}/workers")
}

pub fn worker_detail_href(connection_id: ConnectionId, worker_id: &str) -> String {
    format!("/c/{connection_id}/workers/{worker_id}")
}

pub fn worker_new_href(connection_id: ConnectionId) -> String {
    format!("/c/{connection_id}/workers/new")
}

pub fn connection_new_href() -> &'static str {
    "/connections/new"
}

pub fn connection_edit_href(connection_id: ConnectionId) -> String {
    format!("/connections/{connection_id}/edit")
}

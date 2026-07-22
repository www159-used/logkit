use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use rusqlite::{params, Connection, OptionalExtension, Row};

use crate::connection::{ConnectionKind, LogendConnection};
use crate::connection_id::ConnectionId;
use crate::error::ConnectionError;

const DB_FILE_NAME: &str = "logen-connection.db";

/// 本地 SQLite 持久化（Console 侧，非 logend）。
#[derive(Clone)]
pub(super) struct Store {
    conn: Arc<Mutex<Connection>>,
}

impl fmt::Debug for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Store").finish_non_exhaustive()
    }
}

impl Store {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, ConnectionError> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(&path)?;
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA foreign_keys = ON;",
        )?;
        init_schema(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn default_path() -> Result<PathBuf, ConnectionError> {
        let home = logen_config::resolve_logkit_home(None)?;
        Ok(home.join(DB_FILE_NAME))
    }

    pub fn open_default() -> Result<Self, ConnectionError> {
        Self::new(Self::default_path()?)
    }

    pub fn load(&self) -> Result<Vec<LogendConnection>, ConnectionError> {
        let conn = self.lock_conn()?;
        select_all(&conn)
    }

    pub fn upsert(&self, connection: LogendConnection) -> Result<LogendConnection, ConnectionError> {
        let conn = self.lock_conn()?;
        write_one(&conn, &connection)?;
        Ok(connection)
    }

    pub fn delete(&self, id: ConnectionId) -> Result<ConnectionId, ConnectionError> {
        let conn = self.lock_conn()?;
        let changed = conn.execute("DELETE FROM connections WHERE id = ?1", [id.to_string()])?;
        if changed == 0 {
            return Err(ConnectionError::msg(format!("connection not found: {id}")));
        }
        Ok(id)
    }

    pub fn get(&self, id: ConnectionId) -> Result<LogendConnection, ConnectionError> {
        let conn = self.lock_conn()?;
        select_one(&conn, id)
    }

    fn lock_conn(&self) -> Result<std::sync::MutexGuard<'_, Connection>, ConnectionError> {
        self.conn
            .lock()
            .map_err(|_| ConnectionError::msg("connection store lock poisoned"))
    }
}

fn init_schema(conn: &Connection) -> Result<(), ConnectionError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER NOT NULL
        );
        INSERT INTO schema_version (version)
        SELECT 1
        WHERE NOT EXISTS (SELECT 1 FROM schema_version);

        CREATE TABLE IF NOT EXISTS connections (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            kind TEXT NOT NULL,
            socket TEXT NOT NULL DEFAULT '',
            host TEXT NOT NULL DEFAULT '',
            port INTEGER NOT NULL DEFAULT 0,
            defaults_file TEXT NOT NULL DEFAULT '',
            auto_kafka_protocol INTEGER,
            notes TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_connections_name
            ON connections(name COLLATE NOCASE);",
    )?;
    Ok(())
}

fn select_all(conn: &Connection) -> Result<Vec<LogendConnection>, ConnectionError> {
    let mut stmt = conn.prepare(
        "SELECT id, name, kind, socket, host, port, defaults_file, auto_kafka_protocol, notes
         FROM connections
         ORDER BY lower(name)",
    )?;
    let rows = stmt.query_map([], row_to_connection)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

fn select_one(conn: &Connection, id: ConnectionId) -> Result<LogendConnection, ConnectionError> {
    conn.query_row(
        "SELECT id, name, kind, socket, host, port, defaults_file, auto_kafka_protocol, notes
         FROM connections
         WHERE id = ?1",
        [id.to_string()],
        row_to_connection,
    )
    .optional()?
    .ok_or_else(|| ConnectionError::msg(format!("connection not found: {id}")))
}

fn write_one(conn: &Connection, connection: &LogendConnection) -> Result<(), ConnectionError> {
    let kind = kind_to_str(connection.kind);
    let auto_kafka = match connection.auto_kafka_protocol {
        Some(true) => Some(1_i64),
        Some(false) => Some(0_i64),
        None => None,
    };
    conn.execute(
        "INSERT INTO connections (
            id, name, kind, socket, host, port, defaults_file, auto_kafka_protocol, notes
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
         ON CONFLICT(id) DO UPDATE SET
            name = excluded.name,
            kind = excluded.kind,
            socket = excluded.socket,
            host = excluded.host,
            port = excluded.port,
            defaults_file = excluded.defaults_file,
            auto_kafka_protocol = excluded.auto_kafka_protocol,
            notes = excluded.notes",
        params![
            connection.id.to_string(),
            connection.name,
            kind,
            connection.socket,
            connection.host,
            connection.port,
            connection.defaults_file,
            auto_kafka,
            connection.notes,
        ],
    )?;
    Ok(())
}

fn row_to_connection(row: &Row<'_>) -> rusqlite::Result<LogendConnection> {
    let id_raw: String = row.get(0)?;
    let id = ConnectionId::from_str(&id_raw).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
    })?;
    let kind_raw: String = row.get(2)?;
    let kind = kind_from_str(&kind_raw).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, Box::new(e))
    })?;
    let auto_kafka: Option<i64> = row.get(7)?;
    Ok(LogendConnection {
        id,
        name: row.get(1)?,
        kind,
        socket: row.get(3)?,
        host: row.get(4)?,
        port: row.get(5)?,
        defaults_file: row.get(6)?,
        auto_kafka_protocol: auto_kafka.map(|v| v != 0),
        notes: row.get(8)?,
    })
}

fn kind_to_str(kind: ConnectionKind) -> &'static str {
    match kind {
        ConnectionKind::Local => "local",
        ConnectionKind::Remote => "remote",
    }
}

fn kind_from_str(raw: &str) -> Result<ConnectionKind, ConnectionError> {
    match raw {
        "local" => Ok(ConnectionKind::Local),
        "remote" => Ok(ConnectionKind::Remote),
        other => Err(ConnectionError::msg(format!("unknown connection kind: {other}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_store() -> (Store, PathBuf) {
        let dir = std::env::temp_dir().join(format!("logen-connection-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("connections.db");
        let store = Store::new(&db_path).unwrap();
        (store, dir)
    }

    /// 测试内容：Store 读写与 upsert/delete。
    /// 输入：临时 SQLite 路径；local + remote 两条连接。
    /// 预期：load 条数正确；upsert 覆盖；delete 后仅剩一条。
    #[test]
    fn store_roundtrip() {
        let (store, dir) = temp_store();

        let local = LogendConnection::new_local("dev");
        store.upsert(local).unwrap();

        let remote = LogendConnection::new_remote("132", "10.0.0.5", 11159);
        store.upsert(remote).unwrap();
        assert_eq!(store.load().unwrap().len(), 2);

        let mut local = store
            .load()
            .unwrap()
            .into_iter()
            .find(|c| c.kind == ConnectionKind::Local)
            .unwrap();
        local.name = "dev-renamed".into();
        store.upsert(local).unwrap();
        assert!(
            store
                .load()
                .unwrap()
                .iter()
                .any(|c| c.name == "dev-renamed")
        );

        let id = store
            .load()
            .unwrap()
            .into_iter()
            .find(|c| c.kind == ConnectionKind::Local)
            .unwrap()
            .id;
        store.delete(id).unwrap();
        assert_eq!(store.load().unwrap().len(), 1);
        assert_eq!(store.load().unwrap()[0].kind, ConnectionKind::Remote);

        let _ = std::fs::remove_dir_all(dir);
    }
}

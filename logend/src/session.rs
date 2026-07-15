//! 控制脚本会话的 daemon 侧生命周期管理。

use std::collections::HashMap;
use std::sync::Arc;

use logen_script::{ControlSession, ScriptError, Value};
use tokio::sync::Mutex;
use uuid::Uuid;

/// 一个可被多次求值的控制脚本会话。
pub struct DaemonControlSession {
    evaluator: Mutex<ControlSession>,
}

impl DaemonControlSession {
    pub fn new(evaluator: ControlSession) -> Self {
        Self {
            evaluator: Mutex::new(evaluator),
        }
    }

    pub async fn execute(&self, source: &str) -> Result<(Option<Value>, String), ScriptError> {
        let mut evaluator = self.evaluator.lock().await;
        let value = evaluator.execute(source)?;
        let output = evaluator.output()?;
        Ok((value, output))
    }
}

/// 所有活动控制会话的索引。
#[derive(Default)]
pub struct ControlSessionStore {
    sessions: Mutex<HashMap<String, Arc<DaemonControlSession>>>,
}

impl ControlSessionStore {
    pub async fn open(&self, evaluator: ControlSession) -> String {
        let id = Uuid::new_v4().to_string();
        self.sessions
            .lock()
            .await
            .insert(id.clone(), Arc::new(DaemonControlSession::new(evaluator)));
        id
    }

    pub async fn get(&self, id: &str) -> Option<Arc<DaemonControlSession>> {
        self.sessions.lock().await.get(id).cloned()
    }

    pub async fn execute(
        &self,
        id: &str,
        source: &str,
    ) -> Result<(Option<Value>, String), ScriptError> {
        let session = self
            .get(id)
            .await
            .ok_or_else(|| ScriptError::eval_msg("unknown control session"))?;
        session.execute(source).await
    }

    pub async fn close(&self, id: &str) {
        self.sessions.lock().await.remove(id);
    }
}

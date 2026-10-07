use serde::Serialize;
use std::error::Error;
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RunDslError {
    pub code: String,
    pub location: String,
    pub position: usize,
    pub message: String,
    pub remediation: String,
}

impl RunDslError {
    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self {
            code: "DSL_INTERNAL_ERROR".into(),
            location: "task".into(),
            position: 0,
            message: message.into(),
            remediation: "retry with a valid DataJig DSL v1 task".into(),
        }
    }
}

impl fmt::Display for RunDslError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} at {} (byte {}): {}. {}",
            self.code, self.location, self.position, self.message, self.remediation
        )
    }
}

impl Error for RunDslError {}

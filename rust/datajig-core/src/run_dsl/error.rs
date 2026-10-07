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
    pub(crate) fn new(
        code: impl Into<String>,
        location: impl Into<String>,
        position: usize,
        message: impl Into<String>,
        remediation: impl Into<String>,
    ) -> Self {
        Self {
            code: code.into(),
            location: location.into(),
            position,
            message: message.into(),
            remediation: remediation.into(),
        }
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::new(
            "DSL_INTERNAL_ERROR",
            "task",
            0,
            message,
            "retry with a valid DataJig DSL v1 task",
        )
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

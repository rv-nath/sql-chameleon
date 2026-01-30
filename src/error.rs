// Error types

use std::fmt;
use crate::ast::Dialect;

/// Errors that can occur during parsing
#[derive(Debug, Clone)]
pub struct ParseError {
    pub message: String,
    pub location: Option<String>,
}

impl ParseError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            location: None,
        }
    }

    pub fn with_location(message: impl Into<String>, location: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            location: Some(location.into()),
        }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.location {
            Some(loc) => write!(f, "Parse error at {}: {}", loc, self.message),
            None => write!(f, "Parse error: {}", self.message),
        }
    }
}

impl std::error::Error for ParseError {}

/// Errors that can occur during SQL emission
#[derive(Debug, Clone)]
pub struct EmitError {
    pub message: String,
}

impl EmitError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for EmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Emit error: {}", self.message)
    }
}

impl std::error::Error for EmitError {}

/// Top-level transpiler errors
#[derive(Debug)]
pub enum TranspileError {
    UnsupportedDialect(Dialect),
    Parse(ParseError),
    Emit(EmitError),
}

impl fmt::Display for TranspileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TranspileError::UnsupportedDialect(d) => {
                write!(f, "Unsupported dialect: {}", d)
            }
            TranspileError::Parse(e) => write!(f, "{}", e),
            TranspileError::Emit(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for TranspileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TranspileError::Parse(e) => Some(e),
            TranspileError::Emit(e) => Some(e),
            TranspileError::UnsupportedDialect(_) => None,
        }
    }
}

// Conversion implementations for using ? operator
impl From<ParseError> for TranspileError {
    fn from(err: ParseError) -> Self {
        TranspileError::Parse(err)
    }
}

impl From<EmitError> for TranspileError {
    fn from(err: EmitError) -> Self {
        TranspileError::Emit(err)
    }
}

// Convert sqlparser errors to our ParseError
impl From<sqlparser::parser::ParserError> for ParseError {
    fn from(err: sqlparser::parser::ParserError) -> Self {
        ParseError::new(err.to_string())
    }
}

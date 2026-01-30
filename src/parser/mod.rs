// SqlParser trait and implementations

pub mod mysql;
pub mod oracle;

use crate::ast::{Dialect, Statement};
use crate::error::ParseError;

/// Trait for parsing SQL from a specific dialect into dialect-neutral AST
pub trait SqlParser: Send + Sync {
    /// Parse SQL string into dialect-neutral AST
    fn parse(&self, sql: &str) -> Result<Vec<Statement>, ParseError>;

    /// What dialect does this parser handle?
    fn dialect(&self) -> Dialect;
}

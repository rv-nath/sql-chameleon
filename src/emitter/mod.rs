// SqlEmitter trait and implementations

pub mod mysql;
pub mod oracle;

use crate::ast::{Dialect, Statement};
use crate::error::EmitError;

/// Trait for emitting SQL in a specific dialect from dialect-neutral AST
pub trait SqlEmitter: Send + Sync {
    /// Convert dialect-neutral AST to SQL string
    fn emit(&self, statements: &[Statement]) -> Result<String, EmitError> {
        let results: Result<Vec<String>, EmitError> = statements
            .iter()
            .map(|stmt| self.emit_statement(stmt))
            .collect();
        Ok(results?.join("\n\n"))
    }

    /// Emit a single statement
    fn emit_statement(&self, stmt: &Statement) -> Result<String, EmitError>;

    /// What dialect does this emitter produce?
    fn dialect(&self) -> Dialect;

    /// Get supplementary statements (triggers, sequences, etc.)
    /// Override for dialects that need extra statements for features like AUTO_INCREMENT
    fn supplementary_statements(&self) -> Vec<String> {
        vec![]
    }

    /// Reset internal state (call between conversions)
    /// Override for dialects that track state during emission
    fn reset(&mut self) {
        // default: nothing to reset
    }
}

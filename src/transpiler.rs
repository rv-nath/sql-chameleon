// Main SqlTranspiler

use std::collections::HashMap;

use crate::ast::Dialect;
use crate::error::TranspileError;
use crate::parser::SqlParser;
use crate::parser::mysql::MySqlParser;
use crate::emitter::SqlEmitter;
use crate::emitter::mysql::MySqlEmitter;
use crate::emitter::oracle::OracleEmitter;

pub struct SqlTranspiler {
    parsers: HashMap<Dialect, Box<dyn SqlParser>>,
    emitters: HashMap<Dialect, Box<dyn SqlEmitter>>,
}

impl SqlTranspiler {
    pub fn new() -> Self {
        let mut parsers: HashMap<Dialect, Box<dyn SqlParser>> = HashMap::new();
        let mut emitters: HashMap<Dialect, Box<dyn SqlEmitter>> = HashMap::new();

        // Register MySQL
        parsers.insert(Dialect::MySQL, Box::new(MySqlParser::new()));
        emitters.insert(Dialect::MySQL, Box::new(MySqlEmitter::new()));

        // Register Oracle (emitter only for now)
        emitters.insert(Dialect::Oracle, Box::new(OracleEmitter::new()));

        Self { parsers, emitters }
    }

    /// Convert SQL from one dialect to another
    pub fn convert(
        &self,
        sql: &str,
        from: Dialect,
        to: Dialect,
    ) -> Result<String, TranspileError> {
        // 1. Find the right parser
        let parser = self
            .parsers
            .get(&from)
            .ok_or(TranspileError::UnsupportedDialect(from))?;

        // 2. Find the right emitter
        let emitter = self
            .emitters
            .get(&to)
            .ok_or(TranspileError::UnsupportedDialect(to))?;

        // 3. Parse source SQL → neutral AST
        let statements = parser.parse(sql)?;

        // 4. Emit target SQL
        let output = emitter.emit(&statements)?;

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mysql_roundtrip_simple() {
        let transpiler = SqlTranspiler::new();

        let input = "CREATE TABLE users (id INT NOT NULL, name VARCHAR(100));";

        let output = transpiler.convert(input, Dialect::MySQL, Dialect::MySQL).unwrap();

        assert!(output.contains("CREATE TABLE"), "Should have CREATE TABLE");
        assert!(output.contains("users"), "Should have table name");
        assert!(output.contains("id"), "Should have column id");
        assert!(output.contains("INT"), "Should have INT type");
        assert!(output.contains("NOT NULL"), "Should have NOT NULL");
        assert!(output.contains("name"), "Should have column name");
        assert!(output.contains("VARCHAR(100)"), "Should have VARCHAR(100)");
    }

    #[test]
    fn test_mysql_roundtrip_full_table() {
        let transpiler = SqlTranspiler::new();

        let input = "CREATE TABLE orders (
            id BIGINT NOT NULL AUTO_INCREMENT,
            user_id INT NOT NULL,
            total DECIMAL(10,2) DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );";

        let output = transpiler.convert(input, Dialect::MySQL, Dialect::MySQL).unwrap();

        assert!(output.contains("BIGINT"), "Should have BIGINT");
        assert!(output.contains("AUTO_INCREMENT"), "Should have AUTO_INCREMENT");
        assert!(output.contains("DECIMAL(10,2)"), "Should have DECIMAL(10,2)");
        assert!(output.contains("DEFAULT CURRENT_TIMESTAMP"), "Should have DEFAULT CURRENT_TIMESTAMP");
        assert!(output.contains("PRIMARY KEY"), "Should have PRIMARY KEY");
        assert!(output.contains("FOREIGN KEY"), "Should have FOREIGN KEY");
        assert!(output.contains("REFERENCES users"), "Should have REFERENCES");
        assert!(output.contains("ON DELETE CASCADE"), "Should have ON DELETE CASCADE");
    }

    #[test]
    fn test_mysql_to_oracle() {
        let transpiler = SqlTranspiler::new();

        let input = "CREATE TABLE users (
            id BIGINT NOT NULL,
            name VARCHAR(100) NOT NULL,
            active BOOLEAN DEFAULT FALSE,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id)
        );";

        let output = transpiler.convert(input, Dialect::MySQL, Dialect::Oracle).unwrap();

        // INT types should become NUMBER
        assert!(output.contains("NUMBER(19)"), "BIGINT should map to NUMBER(19), got: {}", output);
        // VARCHAR should become VARCHAR2
        assert!(output.contains("VARCHAR2(100 CHAR)"), "VARCHAR should map to VARCHAR2, got: {}", output);
        // BOOLEAN should become NUMBER(1)
        assert!(output.contains("NUMBER(1)"), "BOOLEAN should map to NUMBER(1), got: {}", output);
        // FALSE should become 0
        assert!(output.contains("DEFAULT 0"), "FALSE should map to 0, got: {}", output);
        // CURRENT_TIMESTAMP should become SYSTIMESTAMP
        assert!(output.contains("SYSTIMESTAMP"), "Should use SYSTIMESTAMP, got: {}", output);
        // DATETIME should become TIMESTAMP
        assert!(output.contains("TIMESTAMP"), "DATETIME should map to TIMESTAMP, got: {}", output);
    }

    #[test]
    fn test_unsupported_dialect() {
        let transpiler = SqlTranspiler::new();

        let input = "CREATE TABLE t (id INT);";
        let result = transpiler.convert(input, Dialect::Oracle, Dialect::MySQL);

        assert!(result.is_err(), "Should fail for unsupported dialect");
    }
}

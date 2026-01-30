// Oracle Emitter

use crate::ast::{
    Column, Constraint, DataType, DefaultValue, Dialect, IntegerType, ReferentialAction,
    Statement, StringType, Table, TemporalType,
};
use crate::error::EmitError;
use super::SqlEmitter;

/// Emitter for Oracle SQL dialect
pub struct OracleEmitter;

impl OracleEmitter {
    pub fn new() -> Self {
        Self
    }
}

impl SqlEmitter for OracleEmitter {
    fn dialect(&self) -> Dialect {
        Dialect::Oracle
    }

    fn emit(&self, statements: &[Statement]) -> Result<String, EmitError> {
        let mut preamble = String::from(
            "-- Oracle session settings\n\
             ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS';\n\
             ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD';\n\
             SET DEFINE OFF;\n"
        );

        let results: Result<Vec<String>, EmitError> = statements
            .iter()
            .map(|stmt| self.emit_statement(stmt))
            .collect();

        preamble.push('\n');
        preamble.push_str(&results?.join("\n\n"));
        Ok(preamble)
    }

    fn emit_statement(&self, stmt: &Statement) -> Result<String, EmitError> {
        match stmt {
            Statement::CreateTable(table) => self.emit_create_table(table),
            Statement::DropTable { name, if_exists, cascade } => {
                self.emit_drop_table(name, *if_exists, *cascade)
            }
            Statement::CreateIndex { name, table, columns, unique } => {
                let cols: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
                let unique_str = if *unique { "UNIQUE " } else { "" };
                Ok(format!("CREATE {}INDEX {} ON {} ({});", unique_str, name, table, cols.join(", ")))
            }
            Statement::LockTables { tables } => {
                // Oracle uses different locking syntax; emit as comment
                let names: Vec<String> = tables.iter().map(|(n, _)| n.clone()).collect();
                Ok(format!("-- LOCK TABLES {} (not applicable in Oracle)", names.join(", ")))
            }
            Statement::UnlockTables => {
                Ok("-- UNLOCK TABLES (not applicable in Oracle)".to_string())
            }
            Statement::Insert { table, columns, values } => {
                self.emit_insert(table, columns, values)
            }
            Statement::Use { database } => {
                Ok(format!("ALTER SESSION SET CURRENT_SCHEMA = {};", database))
            }
            Statement::CreateDatabase { name, .. } => {
                // Oracle doesn't have CREATE DATABASE like MySQL.
                // The equivalent is CREATE USER (schema) + GRANT + ALTER SESSION.
                Ok(format!(
                    "-- CREATE USER {name} IDENTIFIED BY {name};\n\
                     -- GRANT CONNECT, RESOURCE TO {name};\n\
                     ALTER SESSION SET CURRENT_SCHEMA = {name};"
                ))
            }
            Statement::Commit => Ok("COMMIT;".to_string()),
            Statement::SetVariable { raw_sql } => {
                Ok(format!("-- {} (MySQL session variable, skipped)", raw_sql))
            }
            Statement::AlterTable { name, operations } => {
                self.emit_alter_table(name, operations)
            }
            Statement::RawStatement { raw_sql } => Ok(format!("{};", raw_sql)),
        }
    }
}

// Oracle emit helpers
impl OracleEmitter {
    /// Emit CREATE TABLE
    fn emit_create_table(&self, table: &Table) -> Result<String, EmitError> {
        let mut sql = format!("CREATE TABLE {} (\n", table.name);

        // Emit columns
        let column_strs: Vec<String> = table
            .columns
            .iter()
            .map(|col| self.emit_column(col))
            .collect();

        // Emit constraints
        let constraint_strs: Vec<String> = table
            .constraints
            .iter()
            .filter_map(|c| self.emit_constraint(c))
            .collect();

        let mut all_parts = column_strs;
        all_parts.extend(constraint_strs);

        sql.push_str(&all_parts.join(",\n"));
        sql.push_str("\n);");

        // Append SEQUENCE + TRIGGER for AUTO_INCREMENT columns
        for col in &table.columns {
            if col.auto_increment {
                sql.push_str(&format!(
                    "\n\nCREATE SEQUENCE {table}_seq START WITH 1 INCREMENT BY 1 NOCACHE;",
                    table = table.name
                ));
                sql.push_str(&format!(
                    r#"

CREATE OR REPLACE TRIGGER trg_{table}_ai
BEFORE INSERT ON {table}
FOR EACH ROW
WHEN (NEW.{column} IS NULL)
BEGIN
  :NEW.{column} := {table}_seq.NEXTVAL;
END;
/"#,
                    table = table.name,
                    column = col.name
                ));
            }
        }

        // Append CREATE INDEX for Index constraints
        // Oracle doesn't support inline KEY/INDEX in CREATE TABLE
        for constraint in &table.constraints {
            if let Constraint::Index { name, columns, unique } = constraint {
                let cols: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
                let unique_str = if *unique { "UNIQUE " } else { "" };
                // Prefix index name with table name to avoid duplicates across tables
                let idx_name = format!("{}_{}", table.name, name);
                sql.push_str(&format!(
                    "\n\nCREATE {unique_str}INDEX {idx_name} ON {table} ({cols});",
                    unique_str = unique_str,
                    idx_name = idx_name,
                    table = table.name,
                    cols = cols.join(", ")
                ));
            }
        }

        Ok(sql)
    }

    /// Emit ALTER TABLE statement
    /// Oracle uses ADD (col_def) and MODIFY (col_def) syntax without COLUMN keyword
    fn emit_alter_table(
        &self,
        table: &str,
        operations: &[crate::ast::AlterOperation],
    ) -> Result<String, EmitError> {
        let stmts: Vec<String> = operations
            .iter()
            .map(|op| match op {
                crate::ast::AlterOperation::AddColumn(col) => {
                    format!("ALTER TABLE {} ADD ({});", table, self.emit_column(col).trim())
                }
                crate::ast::AlterOperation::ModifyColumn(col) => {
                    format!("ALTER TABLE {} MODIFY ({});", table, self.emit_column(col).trim())
                }
                crate::ast::AlterOperation::DropColumn { name } => {
                    format!("ALTER TABLE {} DROP COLUMN {};", table, name)
                }
                crate::ast::AlterOperation::RenameColumn { old_name, new_name } => {
                    format!("ALTER TABLE {} RENAME COLUMN {} TO {};", table, old_name, new_name)
                }
                crate::ast::AlterOperation::AddConstraint(constraint) => {
                    match self.emit_constraint(constraint) {
                        Some(c) => format!("ALTER TABLE {} ADD {};", table, c.trim()),
                        None => format!("-- ALTER TABLE {} ADD unsupported constraint", table),
                    }
                }
                crate::ast::AlterOperation::DropConstraint { name } => {
                    format!("ALTER TABLE {} DROP CONSTRAINT {};", table, name)
                }
            })
            .collect();

        Ok(stmts.join("\n"))
    }

    /// Emit a column definition
    fn emit_column(&self, col: &Column) -> String {
        let mut parts = vec![
            format!("  {}", col.name),
            self.emit_data_type(&col.data_type),
        ];

        // DEFAULT before NOT NULL (Oracle convention)
        if let Some(default) = &col.default {
            parts.push(format!("DEFAULT {}", self.emit_default(default)));
        }

        if !col.nullable {
            parts.push("NOT NULL".to_string());
        }

        // NOTE: AUTO_INCREMENT is handled separately via supplementary_statements

        parts.join(" ")
    }

    /// Emit Oracle data type
    fn emit_data_type(&self, dt: &DataType) -> String {
        match dt {
            // Integers → NUMBER(precision)
            DataType::Integer(IntegerType::TinyInt) => "NUMBER(3)".to_string(),
            DataType::Integer(IntegerType::SmallInt) => "NUMBER(5)".to_string(),
            DataType::Integer(IntegerType::Int) => "NUMBER(10)".to_string(),
            DataType::Integer(IntegerType::BigInt) => "NUMBER(19)".to_string(),

            // Strings
            DataType::String(StringType::Char { length }) => {
                format!("CHAR({} CHAR)", length)
            }
            DataType::String(StringType::Varchar { length }) => {
                format!("VARCHAR2({} CHAR)", length)
            }
            DataType::String(StringType::Text { .. }) => "CLOB".to_string(),

            // Temporal
            DataType::Temporal(TemporalType::Date) => "DATE".to_string(),
            DataType::Temporal(TemporalType::Time { precision }) => {
                match precision {
                    Some(p) => format!("TIMESTAMP({})", p),
                    None => "TIMESTAMP".to_string(),
                }
            }
            DataType::Temporal(TemporalType::Timestamp { precision, with_timezone }) => {
                let base = match precision {
                    Some(p) => format!("TIMESTAMP({})", p),
                    None => "TIMESTAMP".to_string(),
                };
                if *with_timezone {
                    format!("{} WITH TIME ZONE", base)
                } else {
                    base
                }
            }

            // Decimal
            DataType::Decimal { precision, scale } => {
                format!("NUMBER({},{})", precision, scale)
            }

            // Boolean → NUMBER(1)
            DataType::Boolean => "NUMBER(1)".to_string(),

            // Float → NUMBER
            DataType::Float => "NUMBER".to_string(),

            // Binary
            DataType::Binary { length } => {
                match length {
                    Some(l) => format!("RAW({})", l),
                    None => "BLOB".to_string(),
                }
            }
            DataType::Blob => "BLOB".to_string(),

            // JSON → CLOB (Oracle doesn't have native JSON type until 21c)
            DataType::Json => "CLOB".to_string(),

            // UUID → RAW(16)
            DataType::Uuid => "RAW(16)".to_string(),

            // ENUM → VARCHAR2 (CHECK constraint added separately)
            DataType::Enum { values } => {
                let max_len = values.iter().map(|v| v.len()).max().unwrap_or(10);
                format!("VARCHAR2({})", max_len.max(10))
            }
        }
    }

    /// Emit Oracle default value
    fn emit_default(&self, default: &DefaultValue) -> String {
        match default {
            DefaultValue::Null => "NULL".to_string(),
            DefaultValue::Boolean(true) => "1".to_string(),
            DefaultValue::Boolean(false) => "0".to_string(),
            DefaultValue::Integer(i) => i.to_string(),
            DefaultValue::Float(f) => f.to_string(),
            DefaultValue::String(s) => format!("'{}'", s.replace('\'', "''")),
            DefaultValue::CurrentTimestamp => "SYSTIMESTAMP".to_string(),
            DefaultValue::CurrentDate => "SYSDATE".to_string(),
            DefaultValue::Uuid => "SYS_GUID()".to_string(),
            DefaultValue::Expression(e) => e.clone(),
        }
    }

    /// Emit DROP TABLE
    fn emit_drop_table(&self, name: &str, if_exists: bool, cascade: bool) -> Result<String, EmitError> {
        let cascade_str = if cascade { " CASCADE CONSTRAINTS" } else { "" };

        // Oracle doesn't support IF EXISTS - needs PL/SQL block
        if if_exists {
            Ok(format!(
                r#"BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE {name}{cascade_str}';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -942 THEN RAISE; END IF;
END;
/"#
            ))
        } else {
            Ok(format!("DROP TABLE {}{};", name, cascade_str))
        }
    }

    /// Emit a constraint
    fn emit_constraint(&self, constraint: &Constraint) -> Option<String> {
        match constraint {
            Constraint::PrimaryKey { name, columns } => {
                let cols = columns.join(", ");
                match name {
                    Some(n) => Some(format!("  CONSTRAINT {} PRIMARY KEY ({})", n, cols)),
                    None => Some(format!("  PRIMARY KEY ({})", cols)),
                }
            }

            Constraint::Unique { name, columns } => {
                let cols = columns.join(", ");
                match name {
                    Some(n) => Some(format!("  CONSTRAINT {} UNIQUE ({})", n, cols)),
                    None => Some(format!("  UNIQUE ({})", cols)),
                }
            }

            Constraint::ForeignKey {
                name,
                columns,
                ref_table,
                ref_columns,
                on_delete,
                ..  // Oracle doesn't support ON UPDATE for FK
            } => {
                let cols = columns.join(", ");
                let ref_cols = ref_columns.join(", ");

                let mut fk = match name {
                    Some(n) => format!("  CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}({})", n, cols, ref_table, ref_cols),
                    None => format!("  FOREIGN KEY ({}) REFERENCES {}({})", cols, ref_table, ref_cols),
                };

                if let Some(action) = on_delete {
                    fk.push_str(&format!(" ON DELETE {}", self.emit_ref_action(action)));
                }

                Some(fk)
            }

            Constraint::Index { .. } => {
                // Oracle uses separate CREATE INDEX statements
                // Skipped in inline table definition
                None
            }

            _ => None,
        }
    }

    /// Emit INSERT statement
    /// Oracle doesn't support multi-row VALUES, so we emit one INSERT per row
    fn emit_insert(
        &self,
        table: &str,
        columns: &Option<Vec<String>>,
        values: &[Vec<crate::ast::Value>],
    ) -> Result<String, EmitError> {
        let col_part = match columns {
            Some(cols) => format!(" ({})", cols.join(", ")),
            None => String::new(),
        };

        let stmts: Vec<String> = values
            .iter()
            .map(|row| {
                let vals: Vec<String> = row.iter().map(|v| self.emit_insert_value(v)).collect();
                format!("INSERT INTO {}{} VALUES ({});", table, col_part, vals.join(","))
            })
            .collect();

        Ok(stmts.join("\n"))
    }

    /// Emit a value for INSERT (Oracle-specific)
    fn emit_insert_value(&self, value: &crate::ast::Value) -> String {
        match value {
            crate::ast::Value::Null => "NULL".to_string(),
            crate::ast::Value::Boolean(true) => "1".to_string(),
            crate::ast::Value::Boolean(false) => "0".to_string(),
            crate::ast::Value::Integer(i) => i.to_string(),
            crate::ast::Value::Float(f) => f.to_string(),
            crate::ast::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
            crate::ast::Value::CurrentTimestamp => "SYSTIMESTAMP".to_string(),
            crate::ast::Value::Uuid => "SYS_GUID()".to_string(),
            crate::ast::Value::Expression(e) => e.clone(),
        }
    }

    /// Emit referential action
    fn emit_ref_action(&self, action: &ReferentialAction) -> &'static str {
        match action {
            ReferentialAction::Cascade => "CASCADE",
            ReferentialAction::SetNull => "SET NULL",
            ReferentialAction::SetDefault => "SET DEFAULT",
            ReferentialAction::Restrict => "RESTRICT",
            ReferentialAction::NoAction => "NO ACTION",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::*;

    #[test]
    fn test_emit_simple_create_table() {
        let emitter = OracleEmitter::new();

        let table = Table {
            name: "users".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: DataType::Integer(IntegerType::Int),
                    nullable: false,
                    default: None,
                    auto_increment: false,
                    on_update_timestamp: false,
                    comment: None,
                },
                Column {
                    name: "name".to_string(),
                    data_type: DataType::String(StringType::Varchar { length: 100 }),
                    nullable: true,
                    default: None,
                    auto_increment: false,
                    on_update_timestamp: false,
                    comment: None,
                },
            ],
            constraints: vec![],
            comment: None,
        };

        let sql = emitter.emit_statement(&Statement::CreateTable(table)).unwrap();

        assert!(sql.contains("CREATE TABLE"), "Should have CREATE TABLE");
        assert!(sql.contains("users"), "Should have table name");
        assert!(sql.contains("NUMBER(10)"), "INT should map to NUMBER(10)");
        assert!(sql.contains("VARCHAR2(100 CHAR)"), "VARCHAR should map to VARCHAR2(n CHAR)");
        assert!(sql.contains("NOT NULL"), "Should have NOT NULL");
    }

    #[test]
    fn test_emit_oracle_data_types() {
        let emitter = OracleEmitter::new();

        let types = vec![
            (DataType::Integer(IntegerType::TinyInt), "NUMBER(3)"),
            (DataType::Integer(IntegerType::BigInt), "NUMBER(19)"),
            (DataType::String(StringType::Text { max_bytes: None }), "CLOB"),
            (DataType::Boolean, "NUMBER(1)"),
            (DataType::Float, "NUMBER"),
            (DataType::Blob, "BLOB"),
            (DataType::Json, "CLOB"),
            (DataType::Uuid, "RAW(16)"),
            (DataType::Decimal { precision: 10, scale: 2 }, "NUMBER(10,2)"),
        ];

        for (data_type, expected) in types {
            let table = Table {
                name: "t".to_string(),
                columns: vec![Column {
                    name: "col".to_string(),
                    data_type,
                    nullable: true,
                    default: None,
                    auto_increment: false,
                    on_update_timestamp: false,
                    comment: None,
                }],
                constraints: vec![],
                comment: None,
            };

            let sql = emitter.emit_statement(&Statement::CreateTable(table)).unwrap();
            assert!(sql.contains(expected), "Expected {} in: {}", expected, sql);
        }
    }

    #[test]
    fn test_emit_oracle_defaults() {
        let emitter = OracleEmitter::new();

        // Oracle uses SYSTIMESTAMP instead of CURRENT_TIMESTAMP
        let table = Table {
            name: "t".to_string(),
            columns: vec![Column {
                name: "created_at".to_string(),
                data_type: DataType::Temporal(TemporalType::Timestamp { precision: None, with_timezone: false }),
                nullable: true,
                default: Some(DefaultValue::CurrentTimestamp),
                auto_increment: false,
                on_update_timestamp: false,
                comment: None,
            }],
            constraints: vec![],
            comment: None,
        };

        let sql = emitter.emit_statement(&Statement::CreateTable(table)).unwrap();
        assert!(sql.contains("DEFAULT SYSTIMESTAMP"), "Expected SYSTIMESTAMP in: {}", sql);
    }

    #[test]
    fn test_emit_oracle_boolean_defaults() {
        let emitter = OracleEmitter::new();

        // Oracle uses 1/0 for boolean defaults
        let table = Table {
            name: "t".to_string(),
            columns: vec![Column {
                name: "active".to_string(),
                data_type: DataType::Boolean,
                nullable: true,
                default: Some(DefaultValue::Boolean(false)),
                auto_increment: false,
                on_update_timestamp: false,
                comment: None,
            }],
            constraints: vec![],
            comment: None,
        };

        let sql = emitter.emit_statement(&Statement::CreateTable(table)).unwrap();
        assert!(sql.contains("NUMBER(1)"), "Should use NUMBER(1) for boolean");
        assert!(sql.contains("DEFAULT 0"), "Should use 0 for false");
    }

    #[test]
    fn test_emit_oracle_auto_increment() {
        let emitter = OracleEmitter::new();

        let table = Table {
            name: "users".to_string(),
            columns: vec![Column {
                name: "id".to_string(),
                data_type: DataType::Integer(IntegerType::BigInt),
                nullable: false,
                default: None,
                auto_increment: true,
                on_update_timestamp: false,
                comment: None,
            }],
            constraints: vec![Constraint::PrimaryKey {
                name: None,
                columns: vec!["id".to_string()],
            }],
            comment: None,
        };

        let sql = emitter.emit_statement(&Statement::CreateTable(table)).unwrap();

        // Should have CREATE SEQUENCE
        assert!(sql.contains("CREATE SEQUENCE"), "Expected CREATE SEQUENCE in:\n{}", sql);
        assert!(sql.contains("users_seq"), "Expected users_seq in:\n{}", sql);

        // Should have a TRIGGER
        assert!(sql.contains("CREATE OR REPLACE TRIGGER"), "Expected TRIGGER in:\n{}", sql);
        assert!(sql.contains("BEFORE INSERT"), "Expected BEFORE INSERT in:\n{}", sql);
        assert!(sql.contains("NEXTVAL"), "Expected NEXTVAL in:\n{}", sql);
    }

    #[test]
    fn test_emit_oracle_index_as_create_index() {
        let emitter = OracleEmitter::new();

        let table = Table {
            name: "org_product".to_string(),
            columns: vec![Column {
                name: "org_id".to_string(),
                data_type: DataType::String(StringType::Varchar { length: 255 }),
                nullable: true,
                default: None,
                auto_increment: false,
                on_update_timestamp: false,
                comment: None,
            }],
            constraints: vec![Constraint::Index {
                name: "org_id".to_string(),
                columns: vec![IndexColumn {
                    name: "org_id".to_string(),
                    descending: false,
                }],
                unique: false,
            }],
            comment: None,
        };

        let sql = emitter.emit_statement(&Statement::CreateTable(table)).unwrap();

        assert!(
            sql.contains("CREATE INDEX org_product_org_id ON org_product (org_id)"),
            "Expected CREATE INDEX with table-prefixed name in:\n{}", sql
        );
    }

    #[test]
    fn test_emit_oracle_create_database() {
        let emitter = OracleEmitter::new();

        let stmt = Statement::CreateDatabase {
            name: "account".to_string(),
            if_not_exists: true,
        };

        let sql = emitter.emit_statement(&stmt).unwrap();

        assert!(sql.contains("-- CREATE USER account"), "Should have commented CREATE USER");
        assert!(sql.contains("-- GRANT CONNECT"), "Should have commented GRANT");
        assert!(sql.contains("ALTER SESSION SET CURRENT_SCHEMA = account;"),
            "Should have ALTER SESSION");
    }

    #[test]
    fn test_emit_oracle_drop_table_if_exists() {
        let emitter = OracleEmitter::new();

        // Oracle needs PL/SQL block for IF EXISTS
        let stmt = Statement::DropTable {
            name: "users".to_string(),
            if_exists: true,
            cascade: false,
        };

        let sql = emitter.emit_statement(&stmt).unwrap();
        assert!(sql.contains("BEGIN"), "Should use PL/SQL block");
        assert!(sql.contains("EXECUTE IMMEDIATE"), "Should use EXECUTE IMMEDIATE");
        assert!(sql.contains("-942"), "Should check for table-not-found error");
    }
}

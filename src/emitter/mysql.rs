// MySQL Emitter

use super::SqlEmitter;
use crate::ast::{
    Column, Constraint, DataType, DefaultValue, Dialect, IntegerType, ReferentialAction, Statement,
    StringType, Table, TemporalType,
};
use crate::error::EmitError;

/// Emitter for MySQL SQL dialect
pub struct MySqlEmitter;

impl MySqlEmitter {
    pub fn new() -> Self {
        Self
    }
}

impl SqlEmitter for MySqlEmitter {
    fn dialect(&self) -> Dialect {
        Dialect::MySQL
    }

    fn emit_statement(&self, stmt: &Statement) -> Result<String, EmitError> {
        match stmt {
            Statement::CreateTable(table) => self.emit_create_table(table),
            Statement::DropTable {
                name,
                if_exists,
                cascade,
            } => self.emit_drop_table(name, *if_exists, *cascade),
            Statement::CreateIndex {
                name,
                table,
                columns,
                unique,
            } => {
                let cols: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
                let unique_str = if *unique { "UNIQUE " } else { "" };
                Ok(format!(
                    "CREATE {}INDEX {} ON {} ({});",
                    unique_str,
                    name,
                    table,
                    cols.join(", ")
                ))
            }
            Statement::LockTables { tables } => {
                let parts: Vec<String> = tables
                    .iter()
                    .map(|(name, mode)| {
                        let mode_str = match mode {
                            crate::ast::LockMode::Read => "READ",
                            crate::ast::LockMode::Write => "WRITE",
                        };
                        format!("{} {}", name, mode_str)
                    })
                    .collect();
                Ok(format!("LOCK TABLES {};", parts.join(", ")))
            }
            Statement::UnlockTables => Ok("UNLOCK TABLES;".to_string()),
            Statement::Insert {
                table,
                columns,
                values,
            } => self.emit_insert(table, columns, values),
            Statement::Use { database } => Ok(format!("USE {};", database)),
            Statement::CreateDatabase {
                name,
                if_not_exists,
            } => {
                if *if_not_exists {
                    Ok(format!("CREATE DATABASE IF NOT EXISTS {};", name))
                } else {
                    Ok(format!("CREATE DATABASE {};", name))
                }
            }
            Statement::Commit => Ok("COMMIT;".to_string()),
            Statement::SetVariable { raw_sql } => Ok(format!("{};", raw_sql)),
            Statement::AlterTable { name, operations } => self.emit_alter_table(name, operations),
            Statement::CreateSequence { name, .. } => Ok(format!(
                "-- CREATE SEQUENCE {} (not supported in MySQL)",
                name
            )),
            Statement::CreateTrigger { name, .. } => Ok(format!(
                "-- CREATE TRIGGER {} (Oracle PL/SQL, not translatable)",
                name
            )),
            Statement::CreateSynonym { name, .. } => Ok(format!(
                "-- CREATE SYNONYM {} (not supported in MySQL)",
                name
            )),
            Statement::Grant { raw_sql } | Statement::Revoke { raw_sql } => {
                Ok(format!("-- {};", raw_sql))
            }
            Statement::CreateView {
                name,
                or_replace,
                columns,
                query,
            } => {
                let replace_str = if *or_replace { "OR REPLACE " } else { "" };
                let col_str = match columns {
                    Some(cols) => format!(" ({})", cols.join(", ")),
                    None => String::new(),
                };
                Ok(format!(
                    "CREATE {}VIEW {}{} AS\n{};",
                    replace_str, name, col_str, query
                ))
            }
            Statement::RawStatement { raw_sql } => Ok(format!("{};", raw_sql)),
        }
    }
}

// DROP TABLE
impl MySqlEmitter {
    fn emit_drop_table(
        &self,
        name: &str,
        if_exists: bool,
        _cascade: bool,
    ) -> Result<String, EmitError> {
        let sql = if if_exists {
            format!("DROP TABLE IF EXISTS {};", name)
        } else {
            format!("DROP TABLE {};", name)
        };
        Ok(sql)
    }
}

// INSERT
impl MySqlEmitter {
    fn emit_insert(
        &self,
        table: &str,
        columns: &Option<Vec<String>>,
        values: &[Vec<crate::ast::Value>],
    ) -> Result<String, EmitError> {
        let mut sql = format!("INSERT INTO {}", table);

        // Optional column list
        if let Some(cols) = columns {
            sql.push_str(&format!(" ({})", cols.join(", ")));
        }

        // Value rows
        let row_strs: Vec<String> = values
            .iter()
            .map(|row| {
                let vals: Vec<String> = row.iter().map(|v| self.emit_value(v)).collect();
                format!("({})", vals.join(","))
            })
            .collect();

        sql.push_str(&format!(" VALUES {};", row_strs.join(",")));

        Ok(sql)
    }

    fn emit_value(&self, value: &crate::ast::Value) -> String {
        match value {
            crate::ast::Value::Null => "NULL".to_string(),
            crate::ast::Value::Boolean(true) => "TRUE".to_string(),
            crate::ast::Value::Boolean(false) => "FALSE".to_string(),
            crate::ast::Value::Integer(i) => i.to_string(),
            crate::ast::Value::Float(f) => f.to_string(),
            crate::ast::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
            crate::ast::Value::CurrentTimestamp => "CURRENT_TIMESTAMP".to_string(),
            crate::ast::Value::Uuid => "UUID()".to_string(),
            crate::ast::Value::Expression(e) => e.clone(),
        }
    }
}

// ALTER TABLE
impl MySqlEmitter {
    fn emit_alter_table(
        &self,
        table: &str,
        operations: &[crate::ast::AlterOperation],
    ) -> Result<String, EmitError> {
        let stmts: Vec<String> = operations
            .iter()
            .map(|op| match op {
                crate::ast::AlterOperation::AddColumn(col) => {
                    format!(
                        "ALTER TABLE {} ADD COLUMN {};",
                        table,
                        self.emit_column(col).trim()
                    )
                }
                crate::ast::AlterOperation::ModifyColumn(col) => {
                    format!(
                        "ALTER TABLE {} MODIFY COLUMN {};",
                        table,
                        self.emit_column(col).trim()
                    )
                }
                crate::ast::AlterOperation::DropColumn { name } => {
                    format!("ALTER TABLE {} DROP COLUMN {};", table, name)
                }
                crate::ast::AlterOperation::RenameColumn { old_name, new_name } => {
                    format!(
                        "ALTER TABLE {} RENAME COLUMN {} TO {};",
                        table, old_name, new_name
                    )
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
}

// Emit helpers
impl MySqlEmitter {
    /// Emit CREATE TABLE statement
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

        // Combine columns and constraints
        let mut all_parts = column_strs;
        all_parts.extend(constraint_strs);

        sql.push_str(&all_parts.join(",\n"));
        sql.push_str("\n);");
        Ok(sql)
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
                on_update,
            } => {
                let cols = columns.join(", ");
                let ref_cols = ref_columns.join(", ");

                let mut fk = match name {
                    Some(n) => format!(
                        "  CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}({})",
                        n, cols, ref_table, ref_cols
                    ),
                    None => format!(
                        "  FOREIGN KEY ({}) REFERENCES {}({})",
                        cols, ref_table, ref_cols
                    ),
                };

                if let Some(action) = on_delete {
                    fk.push_str(&format!(" ON DELETE {}", self.emit_ref_action(action)));
                }

                if let Some(action) = on_update {
                    fk.push_str(&format!(" ON UPDATE {}", self.emit_ref_action(action)));
                }

                Some(fk)
            }

            Constraint::Index {
                name,
                columns,
                unique,
            } => {
                let cols: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
                let keyword = if *unique { "UNIQUE KEY" } else { "KEY" };
                Some(format!("  {} {} ({})", keyword, name, cols.join(", ")))
            }

            // Skip other constraints for now
            _ => None,
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

    /// Emit a column definition
    fn emit_column(&self, col: &Column) -> String {
        let mut parts = vec![
            format!("  {}", col.name),
            self.emit_data_type(&col.data_type),
        ];

        if !col.nullable {
            parts.push("NOT NULL".to_string());
        }

        // DEFAULT value
        if let Some(default) = &col.default {
            parts.push(format!("DEFAULT {}", self.emit_default(default)));
        }

        // ON UPDATE CURRENT_TIMESTAMP
        if col.on_update_timestamp {
            parts.push("ON UPDATE CURRENT_TIMESTAMP".to_string());
        }

        // AUTO_INCREMENT
        if col.auto_increment {
            parts.push("AUTO_INCREMENT".to_string());
        }

        parts.join(" ")
    }

    /// Emit a default value
    fn emit_default(&self, default: &DefaultValue) -> String {
        match default {
            DefaultValue::Null => "NULL".to_string(),
            DefaultValue::Boolean(true) => "TRUE".to_string(),
            DefaultValue::Boolean(false) => "FALSE".to_string(),
            DefaultValue::Integer(i) => i.to_string(),
            DefaultValue::Float(f) => f.to_string(),
            DefaultValue::String(s) => format!("'{}'", s.replace('\'', "''")),
            DefaultValue::CurrentTimestamp => "CURRENT_TIMESTAMP".to_string(),
            DefaultValue::CurrentDate => "CURRENT_DATE".to_string(),
            DefaultValue::Uuid => "(UUID())".to_string(),
            DefaultValue::Expression(e) => e.clone(),
        }
    }

    /// Emit a data type
    fn emit_data_type(&self, dt: &DataType) -> String {
        match dt {
            // Integers
            DataType::Integer(IntegerType::TinyInt) => "TINYINT".to_string(),
            DataType::Integer(IntegerType::SmallInt) => "SMALLINT".to_string(),
            DataType::Integer(IntegerType::Int) => "INT".to_string(),
            DataType::Integer(IntegerType::BigInt) => "BIGINT".to_string(),

            // Strings
            DataType::String(StringType::Char { length }) => format!("CHAR({})", length),
            DataType::String(StringType::Varchar { length }) => format!("VARCHAR({})", length),
            DataType::String(StringType::Text { max_bytes }) => match max_bytes {
                Some(n) if *n <= 255 => "TINYTEXT".to_string(),
                Some(n) if *n <= 65_535 => "TEXT".to_string(),
                Some(n) if *n <= 16_777_215 => "MEDIUMTEXT".to_string(),
                _ => "LONGTEXT".to_string(),
            },

            // Temporal
            DataType::Temporal(TemporalType::Date) => "DATE".to_string(),
            DataType::Temporal(TemporalType::Time { precision }) => match precision {
                Some(p) => format!("TIME({})", p),
                None => "TIME".to_string(),
            },
            DataType::Temporal(TemporalType::Timestamp { precision, .. }) => match precision {
                Some(p) => format!("DATETIME({})", p),
                None => "DATETIME".to_string(),
            },

            // Decimal
            DataType::Decimal { precision, scale } => format!("DECIMAL({},{})", precision, scale),

            // Boolean (MySQL uses TINYINT(1))
            DataType::Boolean => "TINYINT(1)".to_string(),

            // Float
            DataType::Float => "DOUBLE".to_string(),

            // Binary
            DataType::Binary { length } => match length {
                Some(l) => format!("VARBINARY({})", l),
                None => "LONGBLOB".to_string(),
            },
            DataType::Blob => "LONGBLOB".to_string(),

            // JSON
            DataType::Json => "JSON".to_string(),

            // UUID (MySQL stores as CHAR(36))
            DataType::Uuid => "CHAR(36)".to_string(),

            // ENUM
            DataType::Enum { values } => {
                let quoted: Vec<String> = values.iter().map(|v| format!("'{}'", v)).collect();
                format!("ENUM({})", quoted.join(", "))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::*;

    #[test]
    fn test_emit_simple_create_table() {
        let emitter = MySqlEmitter::new();

        // Build a simple table AST
        let table = Table {
            name: "users".to_string(),
            columns: vec![Column {
                name: "id".to_string(),
                data_type: DataType::Integer(IntegerType::Int),
                nullable: false,
                default: None,
                auto_increment: false,
                on_update_timestamp: false,
                comment: None,
            }],
            constraints: vec![],
            comment: None,
        };

        let stmt = Statement::CreateTable(table);
        let sql = emitter.emit_statement(&stmt).unwrap();

        assert!(sql.contains("CREATE TABLE"), "Should have CREATE TABLE");
        assert!(sql.contains("users"), "Should have table name");
        assert!(sql.contains("id"), "Should have column name");
        assert!(sql.contains("INT"), "Should have INT type");
        assert!(sql.contains("NOT NULL"), "Should have NOT NULL");
    }

    #[test]
    fn test_emit_integer_types() {
        let emitter = MySqlEmitter::new();

        // Test all integer types
        let types = vec![
            (DataType::Integer(IntegerType::TinyInt), "TINYINT"),
            (DataType::Integer(IntegerType::SmallInt), "SMALLINT"),
            (DataType::Integer(IntegerType::Int), "INT"),
            (DataType::Integer(IntegerType::BigInt), "BIGINT"),
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

            let sql = emitter
                .emit_statement(&Statement::CreateTable(table))
                .unwrap();
            assert!(sql.contains(expected), "Expected {} in: {}", expected, sql);
        }
    }

    #[test]
    fn test_emit_string_types() {
        let emitter = MySqlEmitter::new();

        let types = vec![
            (
                DataType::String(StringType::Char { length: 10 }),
                "CHAR(10)",
            ),
            (
                DataType::String(StringType::Varchar { length: 255 }),
                "VARCHAR(255)",
            ),
            (
                DataType::String(StringType::Text {
                    max_bytes: Some(65_535),
                }),
                "TEXT",
            ),
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

            let sql = emitter
                .emit_statement(&Statement::CreateTable(table))
                .unwrap();
            assert!(sql.contains(expected), "Expected {} in: {}", expected, sql);
        }
    }

    #[test]
    fn test_emit_temporal_types() {
        let emitter = MySqlEmitter::new();

        let types = vec![
            (DataType::Temporal(TemporalType::Date), "DATE"),
            (
                DataType::Temporal(TemporalType::Timestamp {
                    precision: None,
                    with_timezone: false,
                }),
                "DATETIME",
            ),
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

            let sql = emitter
                .emit_statement(&Statement::CreateTable(table))
                .unwrap();
            assert!(sql.contains(expected), "Expected {} in: {}", expected, sql);
        }
    }

    #[test]
    fn test_emit_other_types() {
        let emitter = MySqlEmitter::new();

        let types = vec![
            (
                DataType::Decimal {
                    precision: 10,
                    scale: 2,
                },
                "DECIMAL(10,2)",
            ),
            (DataType::Boolean, "TINYINT(1)"),
            (DataType::Float, "DOUBLE"),
            (DataType::Blob, "LONGBLOB"),
            (DataType::Json, "JSON"),
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

            let sql = emitter
                .emit_statement(&Statement::CreateTable(table))
                .unwrap();
            assert!(sql.contains(expected), "Expected {} in: {}", expected, sql);
        }
    }

    // ========== DEFAULT values ==========

    #[test]
    fn test_emit_default_null() {
        let emitter = MySqlEmitter::new();

        let table = Table {
            name: "t".to_string(),
            columns: vec![Column {
                name: "col".to_string(),
                data_type: DataType::String(StringType::Varchar { length: 100 }),
                nullable: true,
                default: Some(DefaultValue::Null),
                auto_increment: false,
                on_update_timestamp: false,
                comment: None,
            }],
            constraints: vec![],
            comment: None,
        };

        let sql = emitter
            .emit_statement(&Statement::CreateTable(table))
            .unwrap();
        assert!(
            sql.contains("DEFAULT NULL"),
            "Expected DEFAULT NULL in: {}",
            sql
        );
    }

    #[test]
    fn test_emit_default_string() {
        let emitter = MySqlEmitter::new();

        let table = Table {
            name: "t".to_string(),
            columns: vec![Column {
                name: "status".to_string(),
                data_type: DataType::String(StringType::Varchar { length: 20 }),
                nullable: true,
                default: Some(DefaultValue::String("pending".to_string())),
                auto_increment: false,
                on_update_timestamp: false,
                comment: None,
            }],
            constraints: vec![],
            comment: None,
        };

        let sql = emitter
            .emit_statement(&Statement::CreateTable(table))
            .unwrap();
        assert!(
            sql.contains("DEFAULT 'pending'"),
            "Expected DEFAULT 'pending' in: {}",
            sql
        );
    }

    #[test]
    fn test_emit_default_integer() {
        let emitter = MySqlEmitter::new();

        let table = Table {
            name: "t".to_string(),
            columns: vec![Column {
                name: "count".to_string(),
                data_type: DataType::Integer(IntegerType::Int),
                nullable: true,
                default: Some(DefaultValue::Integer(0)),
                auto_increment: false,
                on_update_timestamp: false,
                comment: None,
            }],
            constraints: vec![],
            comment: None,
        };

        let sql = emitter
            .emit_statement(&Statement::CreateTable(table))
            .unwrap();
        assert!(sql.contains("DEFAULT 0"), "Expected DEFAULT 0 in: {}", sql);
    }

    #[test]
    fn test_emit_default_current_timestamp() {
        let emitter = MySqlEmitter::new();

        let table = Table {
            name: "t".to_string(),
            columns: vec![Column {
                name: "created_at".to_string(),
                data_type: DataType::Temporal(TemporalType::Timestamp {
                    precision: None,
                    with_timezone: false,
                }),
                nullable: true,
                default: Some(DefaultValue::CurrentTimestamp),
                auto_increment: false,
                on_update_timestamp: false,
                comment: None,
            }],
            constraints: vec![],
            comment: None,
        };

        let sql = emitter
            .emit_statement(&Statement::CreateTable(table))
            .unwrap();
        assert!(
            sql.contains("DEFAULT CURRENT_TIMESTAMP"),
            "Expected DEFAULT CURRENT_TIMESTAMP in: {}",
            sql
        );
    }

    // ========== ON UPDATE CURRENT_TIMESTAMP ==========

    #[test]
    fn test_emit_on_update_current_timestamp() {
        let emitter = MySqlEmitter::new();

        let table = Table {
            name: "t".to_string(),
            columns: vec![Column {
                name: "updated_at".to_string(),
                data_type: DataType::Temporal(TemporalType::Timestamp {
                    precision: None,
                    with_timezone: false,
                }),
                nullable: false,
                default: Some(DefaultValue::CurrentTimestamp),
                auto_increment: false,
                on_update_timestamp: true,
                comment: None,
            }],
            constraints: vec![],
            comment: None,
        };

        let sql = emitter
            .emit_statement(&Statement::CreateTable(table))
            .unwrap();
        assert!(
            sql.contains("DEFAULT CURRENT_TIMESTAMP"),
            "Expected DEFAULT CURRENT_TIMESTAMP in: {}",
            sql
        );
        assert!(
            sql.contains("ON UPDATE CURRENT_TIMESTAMP"),
            "Expected ON UPDATE CURRENT_TIMESTAMP in: {}",
            sql
        );
    }

    // ========== AUTO_INCREMENT ==========

    #[test]
    fn test_emit_auto_increment() {
        let emitter = MySqlEmitter::new();

        let table = Table {
            name: "t".to_string(),
            columns: vec![Column {
                name: "id".to_string(),
                data_type: DataType::Integer(IntegerType::Int),
                nullable: false,
                default: None,
                auto_increment: true,
                on_update_timestamp: false,
                comment: None,
            }],
            constraints: vec![],
            comment: None,
        };

        let sql = emitter
            .emit_statement(&Statement::CreateTable(table))
            .unwrap();
        assert!(
            sql.contains("AUTO_INCREMENT"),
            "Expected AUTO_INCREMENT in: {}",
            sql
        );
    }

    // ========== Constraints ==========

    #[test]
    fn test_emit_primary_key() {
        let emitter = MySqlEmitter::new();

        let table = Table {
            name: "users".to_string(),
            columns: vec![Column {
                name: "id".to_string(),
                data_type: DataType::Integer(IntegerType::Int),
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

        let sql = emitter
            .emit_statement(&Statement::CreateTable(table))
            .unwrap();
        assert!(
            sql.contains("PRIMARY KEY"),
            "Expected PRIMARY KEY in: {}",
            sql
        );
        assert!(sql.contains("(id)"), "Expected (id) in: {}", sql);
    }

    #[test]
    fn test_emit_unique_constraint() {
        let emitter = MySqlEmitter::new();

        let table = Table {
            name: "users".to_string(),
            columns: vec![Column {
                name: "email".to_string(),
                data_type: DataType::String(StringType::Varchar { length: 255 }),
                nullable: false,
                default: None,
                auto_increment: false,
                on_update_timestamp: false,
                comment: None,
            }],
            constraints: vec![Constraint::Unique {
                name: Some("uk_email".to_string()),
                columns: vec!["email".to_string()],
            }],
            comment: None,
        };

        let sql = emitter
            .emit_statement(&Statement::CreateTable(table))
            .unwrap();
        assert!(sql.contains("UNIQUE"), "Expected UNIQUE in: {}", sql);
        assert!(sql.contains("email"), "Expected email in: {}", sql);
    }

    #[test]
    fn test_emit_foreign_key() {
        let emitter = MySqlEmitter::new();

        let table = Table {
            name: "orders".to_string(),
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
                    name: "user_id".to_string(),
                    data_type: DataType::Integer(IntegerType::Int),
                    nullable: false,
                    default: None,
                    auto_increment: false,
                    on_update_timestamp: false,
                    comment: None,
                },
            ],
            constraints: vec![Constraint::ForeignKey {
                name: None,
                columns: vec!["user_id".to_string()],
                ref_table: "users".to_string(),
                ref_columns: vec!["id".to_string()],
                on_delete: Some(ReferentialAction::Cascade),
                on_update: Some(ReferentialAction::SetNull),
            }],
            comment: None,
        };

        let sql = emitter
            .emit_statement(&Statement::CreateTable(table))
            .unwrap();
        assert!(
            sql.contains("FOREIGN KEY"),
            "Expected FOREIGN KEY in: {}",
            sql
        );
        assert!(
            sql.contains("REFERENCES users"),
            "Expected REFERENCES users in: {}",
            sql
        );
        assert!(
            sql.contains("ON DELETE CASCADE"),
            "Expected ON DELETE CASCADE in: {}",
            sql
        );
        assert!(
            sql.contains("ON UPDATE SET NULL"),
            "Expected ON UPDATE SET NULL in: {}",
            sql
        );
    }

    // ========== KEY (Index) ==========

    #[test]
    fn test_emit_key_index() {
        let emitter = MySqlEmitter::new();

        let table = Table {
            name: "t".to_string(),
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

        let sql = emitter
            .emit_statement(&Statement::CreateTable(table))
            .unwrap();
        assert!(
            sql.contains("KEY org_id (org_id)"),
            "Expected KEY index in: {}",
            sql
        );
    }

    // ========== DROP TABLE ==========

    #[test]
    fn test_emit_drop_table() {
        let emitter = MySqlEmitter::new();

        let stmt = Statement::DropTable {
            name: "users".to_string(),
            if_exists: false,
            cascade: false,
        };

        let sql = emitter.emit_statement(&stmt).unwrap();
        assert_eq!(sql, "DROP TABLE users;");
    }

    #[test]
    fn test_emit_drop_table_if_exists() {
        let emitter = MySqlEmitter::new();

        let stmt = Statement::DropTable {
            name: "users".to_string(),
            if_exists: true,
            cascade: false,
        };

        let sql = emitter.emit_statement(&stmt).unwrap();
        assert_eq!(sql, "DROP TABLE IF EXISTS users;");
    }
}

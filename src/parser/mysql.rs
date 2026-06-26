// MySQL Parser

use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use crate::ast::{
    Column, Constraint, DataType, DefaultValue, Dialect, IndexColumn, IntegerType,
    LockMode, ReferentialAction, Statement, StringType, Table, TemporalType, Value,
};
use crate::error::ParseError;
use super::SqlParser;

/// Parser for MySQL SQL dialect
pub struct MySqlParser;

impl MySqlParser {
    pub fn new() -> Self {
        Self
    }
}

impl SqlParser for MySqlParser {
    fn dialect(&self) -> Dialect {
        Dialect::MySQL
    }

    fn parse(&self, sql: &str) -> Result<Vec<Statement>, ParseError> {
        // Step 1: Use sqlparser to parse the SQL string
        let dialect = MySqlDialect {};

        // Always unwrap MySQL version-conditional comments first. Without
        // this, sqlparser sees `/*!32312 IF NOT EXISTS*/` as a regular SQL
        // comment and silently discards the conditional content — which
        // turns `CREATE DATABASE /*!32312 IF NOT EXISTS*/ foo` into a plain
        // `CREATE DATABASE foo`, and the idempotency intent is lost.
        let unwrapped = Self::unwrap_version_comments(sql);

        // sqlparser 0.52 rejects `ALTER TABLE … ADD [COLUMN] IF NOT EXISTS`;
        // drop that clause before parsing (Oracle has no equivalent anyway).
        let unwrapped = Self::strip_add_column_if_not_exists(&unwrapped);

        // Try parsing the whole file; if it fails, preprocess to strip
        // unsupported statements (CREATE USER, GRANT, etc.) and retry
        let parsed_statements = match Parser::parse_sql(&dialect, &unwrapped) {
            Ok(stmts) => stmts,
            Err(_) => {
                let cleaned = self.preprocess_sql(&unwrapped);
                Parser::parse_sql(&dialect, &cleaned)?
            }
        };

        // Step 2: Convert each sqlparser statement to our AST
        let mut results = Vec::new();
        for stmt in parsed_statements {
            let converted = self.convert_statement(stmt)?;
            results.push(converted);
        }

        Ok(results)
    }
}

// Conversion helpers
impl MySqlParser {
    /// Convert sqlparser's Statement to our neutral Statement
    fn convert_statement(
        &self,
        stmt: sqlparser::ast::Statement,
    ) -> Result<Statement, ParseError> {
        use sqlparser::ast::Statement as SpStatement;

        // Pre-compute raw SQL for passthrough statements (e.g., SET)
        let raw_sql = stmt.to_string();

        match stmt {
            SpStatement::CreateTable(create) => {
                self.convert_create_table(create)
            }
            SpStatement::Drop {
                object_type,
                if_exists,
                names,
                cascade,
                ..
            } => {
                self.convert_drop(object_type, if_exists, names, cascade)
            }
            SpStatement::CreateIndex(create_index) => {
                let name = self.strip_backticks(
                    &create_index.name
                        .map(|n| n.to_string())
                        .unwrap_or_else(|| "unnamed_idx".to_string())
                );
                let table = self.strip_backticks(&create_index.table_name.to_string());
                let if_not_exists = create_index.if_not_exists;
                let unique = create_index.unique;
                let columns: Vec<crate::ast::IndexColumn> = create_index.columns
                    .iter()
                    .map(|c| crate::ast::IndexColumn {
                        name: self.strip_backticks(&c.expr.to_string()),
                        descending: c.asc == Some(false),
                    })
                    .collect();

                Ok(Statement::CreateIndex {
                    name,
                    table,
                    columns,
                    unique,
                    if_not_exists,
                })
            }
            SpStatement::LockTables { tables } => {
                let converted: Vec<(String, LockMode)> = tables
                    .iter()
                    .map(|t| {
                        let name = self.strip_backticks(&t.table.value);
                        let mode = match &t.lock_type {
                            sqlparser::ast::LockTableType::Read { .. } => LockMode::Read,
                            sqlparser::ast::LockTableType::Write { .. } => LockMode::Write,
                        };
                        (name, mode)
                    })
                    .collect();
                Ok(Statement::LockTables { tables: converted })
            }
            SpStatement::UnlockTables => {
                Ok(Statement::UnlockTables)
            }
            SpStatement::Insert(insert) => {
                self.convert_insert(insert)
            }
            SpStatement::Use(use_expr) => {
                // Extract database name from any Use variant
                let db_name = match &use_expr {
                    sqlparser::ast::Use::Catalog(name) => name.to_string(),
                    sqlparser::ast::Use::Schema(name) => name.to_string(),
                    sqlparser::ast::Use::Database(name) => name.to_string(),
                    sqlparser::ast::Use::Warehouse(name) => name.to_string(),
                    sqlparser::ast::Use::Object(name) => name.to_string(),
                    sqlparser::ast::Use::Default => "default".to_string(),
                };
                Ok(Statement::Use {
                    database: self.strip_backticks(&db_name),
                })
            }
            SpStatement::CreateDatabase {
                db_name,
                if_not_exists,
                ..
            } => {
                Ok(Statement::CreateDatabase {
                    name: self.strip_backticks(&db_name.to_string()),
                    if_not_exists,
                })
            }
            SpStatement::Commit { .. } => Ok(Statement::Commit),
            SpStatement::AlterTable { name, operations, .. } => {
                self.convert_alter_table(
                    &self.strip_backticks(&name.to_string()),
                    operations,
                )
            }
            // DML pass-through: UPDATE, DELETE, etc.
            SpStatement::Update { .. } => {
                Ok(Statement::RawStatement { raw_sql })
            }
            SpStatement::Delete(_) => {
                Ok(Statement::RawStatement { raw_sql })
            }
            // Catch-all: SET statements pass through as raw SQL
            _ => {
                let upper = raw_sql.to_uppercase();
                if upper.starts_with("SET ") {
                    Ok(Statement::SetVariable { raw_sql })
                } else {
                    Err(ParseError::new(format!(
                        "Unsupported statement: {}",
                        raw_sql
                    )))
                }
            }
        }
    }

    /// Convert DROP statement
    fn convert_drop(
        &self,
        object_type: sqlparser::ast::ObjectType,
        if_exists: bool,
        names: Vec<sqlparser::ast::ObjectName>,
        cascade: bool,
    ) -> Result<Statement, ParseError> {
        use sqlparser::ast::ObjectType;

        match object_type {
            ObjectType::Table => {
                // Get the table name (first name in the list, strip backticks)
                let name = names
                    .first()
                    .map(|n| self.strip_backticks(&n.to_string()))
                    .ok_or_else(|| ParseError::new("DROP TABLE missing table name"))?;

                Ok(Statement::DropTable {
                    name,
                    if_exists,
                    cascade,
                })
            }
            _ => Err(ParseError::new(format!(
                "Unsupported DROP type: {:?}",
                object_type
            ))),
        }
    }

    /// Convert sqlparser's CreateTable to our Table
    fn convert_create_table(
        &self,
        create: sqlparser::ast::CreateTable,
    ) -> Result<Statement, ParseError> {
        // 1. Extract table name (strip backticks)
        let name = self.strip_backticks(&create.name.to_string());

        // Hold on to the bool early — we move other fields out below.
        let if_not_exists = create.if_not_exists;
        let mysql_table_options = Self::collect_mysql_table_options(&create);

        // 2. Convert columns
        let mut columns = Vec::new();
        for col_def in create.columns {
            let column = self.convert_column(col_def)?;
            columns.push(column);
        }

        // 3. Convert constraints
        let mut constraints = Vec::new();
        for constraint in create.constraints {
            if let Some(c) = self.convert_constraint(constraint)? {
                constraints.push(c);
            }
        }

        // 4. Build our Table
        Ok(Statement::CreateTable(Table {
            name,
            if_not_exists,
            columns,
            constraints,
            comment: None, // TODO: extract from table options
            mysql_table_options,
        }))
    }

    /// Stringify the MySQL-specific trailing table options into a single
    /// opaque clause (e.g. `ENGINE=InnoDB DEFAULT CHARSET=utf8mb3`).
    ///
    /// We deliberately don't model these as enum variants — the set is
    /// open-ended (mysqldump emits a long tail of obscure options) and the
    /// MySQL emitter is the only place that consumes them. Keeping the
    /// representation opaque preserves anything we forget to enumerate; the
    /// MySQL emitter pastes it back verbatim, and the Oracle emitter ignores
    /// it (Oracle doesn't share this options vocabulary).
    fn collect_mysql_table_options(create: &sqlparser::ast::CreateTable) -> Option<String> {
        let mut parts: Vec<String> = Vec::new();
        if let Some(engine) = &create.engine {
            // sqlparser-rs's TableEngine Display emits only the engine name
            // ("InnoDB"), not the `ENGINE=` keyword that MySQL requires.
            parts.push(format!("ENGINE={}", engine));
        }
        if let Some(charset) = &create.default_charset {
            parts.push(format!("DEFAULT CHARSET={}", charset));
        }
        if let Some(collation) = &create.collation {
            parts.push(format!("COLLATE={}", collation));
        }
        if parts.is_empty() {
            None
        } else {
            Some(parts.join(" "))
        }
    }

    /// Convert a column definition
    fn convert_column(
        &self,
        col: sqlparser::ast::ColumnDef,
    ) -> Result<Column, ParseError> {
        use sqlparser::ast::ColumnOption;

        // 1. Get column name
        let name = col.name.value.clone();

        // 2. Convert data type
        let data_type = self.convert_data_type(&col.data_type)?;

        // 3. Parse column options (NOT NULL, DEFAULT, etc.)
        let mut nullable = true; // Default is nullable
        let mut default = None;
        let mut auto_increment = false;
        let mut on_update_timestamp = false;

        for option in &col.options {
            match &option.option {
                ColumnOption::Null => nullable = true,
                ColumnOption::NotNull => nullable = false,
                ColumnOption::Default(expr) => {
                    default = Some(self.convert_default(expr)?);
                }
                ColumnOption::OnUpdate(expr) => {
                    // Check if the ON UPDATE expression is CURRENT_TIMESTAMP
                    let expr_str = expr.to_string().to_uppercase();
                    if expr_str.contains("CURRENT_TIMESTAMP") || expr_str.contains("NOW") {
                        on_update_timestamp = true;
                    }
                }
                ColumnOption::DialectSpecific(tokens) => {
                    // Check if tokens contain AUTO_INCREMENT
                    for token in tokens {
                        let token_str = token.to_string().to_uppercase();
                        if token_str.contains("AUTO_INCREMENT") {
                            auto_increment = true;
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(Column {
            name,
            data_type,
            nullable,
            default,
            auto_increment,
            on_update_timestamp,
            comment: None,
        })
    }

    /// Convert sqlparser data type to our DataType
    fn convert_data_type(
        &self,
        dt: &sqlparser::ast::DataType,
    ) -> Result<DataType, ParseError> {
        use sqlparser::ast::DataType as SpDataType;

        match dt {
            // Integers
            // TINYINT(1) is MySQL's idiomatic boolean type
            SpDataType::TinyInt(Some(1)) => Ok(DataType::Boolean),
            SpDataType::TinyInt(_) => Ok(DataType::Integer(IntegerType::TinyInt)),
            SpDataType::SmallInt(_) => Ok(DataType::Integer(IntegerType::SmallInt)),
            SpDataType::Int(_) | SpDataType::Integer(_) => Ok(DataType::Integer(IntegerType::Int)),
            SpDataType::BigInt(_) => Ok(DataType::Integer(IntegerType::BigInt)),

            // Text types
            SpDataType::Char(len) | SpDataType::Character(len) => {
                let length = self.extract_char_length(len).unwrap_or(1);
                Ok(DataType::String(StringType::Char { length }))
            }
            SpDataType::Varchar(len) | SpDataType::CharVarying(len) => {
                let length = self.extract_char_length(len).unwrap_or(255);
                Ok(DataType::String(StringType::Varchar { length }))
            }
            SpDataType::Text => Ok(DataType::String(StringType::Text { max_bytes: Some(65_535) })),

            // Temporal types
            SpDataType::Date => Ok(DataType::Temporal(TemporalType::Date)),
            SpDataType::Datetime(precision) => {
                Ok(DataType::Temporal(TemporalType::Datetime {
                    precision: precision.map(|p| p as u8),
                }))
            }
            SpDataType::Timestamp(precision, _tz) => {
                Ok(DataType::Temporal(TemporalType::Timestamp {
                    precision: precision.map(|p| p as u8),
                    with_timezone: false, // MySQL TIMESTAMP doesn't store timezone
                }))
            }

            // Decimal
            SpDataType::Decimal(info) | SpDataType::Numeric(info) => {
                let (precision, scale) = self.extract_precision_scale(info);
                Ok(DataType::Decimal {
                    precision: precision.unwrap_or(10),
                    scale: scale.unwrap_or(0),
                })
            }

            // Boolean
            SpDataType::Boolean | SpDataType::Bool => Ok(DataType::Boolean),

            // Float/Double
            SpDataType::Float(_) | SpDataType::Double | SpDataType::DoublePrecision => {
                Ok(DataType::Float)
            }

            // Blob
            SpDataType::Blob(_) => Ok(DataType::Blob),

            // JSON
            SpDataType::JSON => Ok(DataType::Json),

            // ENUM
            SpDataType::Enum(values) => {
                Ok(DataType::Enum {
                    values: values.clone(),
                })
            }

            // Binary types
            SpDataType::Varbinary(len) => {
                let length = len.map(|l| l as u32);
                Ok(DataType::Binary { length })
            }
            SpDataType::Binary(len) => {
                let length = len.map(|l| l as u32);
                Ok(DataType::Binary { length })
            }

            // MySQL-specific types that sqlparser parses as Custom
            SpDataType::Custom(name, _) => {
                let type_name = name.to_string().to_uppercase();
                match type_name.as_str() {
                    "TINYTEXT" => Ok(DataType::String(StringType::Text { max_bytes: Some(255) })),
                    "MEDIUMTEXT" => Ok(DataType::String(StringType::Text { max_bytes: Some(16_777_215) })),
                    "LONGTEXT" => Ok(DataType::String(StringType::Text { max_bytes: Some(4_294_967_295) })),
                    "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" => Ok(DataType::Blob),
                    _ => Err(ParseError::new(format!(
                        "Unsupported data type: {:?}",
                        dt
                    ))),
                }
            }

            _ => Err(ParseError::new(format!(
                "Unsupported data type: {:?}",
                dt
            ))),
        }
    }

    /// Extract length from CharacterLength
    /// e.g., VARCHAR(100) → returns Some(100)
    /// e.g., VARCHAR (no length) → returns None
    fn extract_char_length(
        &self,
        len: &Option<sqlparser::ast::CharacterLength>,
    ) -> Option<u32> {
        use sqlparser::ast::CharacterLength;

        match len {
            None => None,
            Some(char_len) => {
                match char_len {
                    CharacterLength::IntegerLength { length, .. } => {
                        Some(*length as u32)
                    }
                    CharacterLength::Max => {
                        // VARCHAR(MAX) - used in SQL Server
                        Some(u32::MAX)
                    }
                }
            }
        }
    }

    /// Extract precision and scale from DECIMAL/NUMERIC
    /// e.g., DECIMAL(10,2) → (Some(10), Some(2))
    /// e.g., DECIMAL(10) → (Some(10), None)
    /// e.g., DECIMAL → (None, None)
    fn extract_precision_scale(
        &self,
        info: &sqlparser::ast::ExactNumberInfo,
    ) -> (Option<u8>, Option<u8>) {
        use sqlparser::ast::ExactNumberInfo;

        match info {
            ExactNumberInfo::None => (None, None),
            ExactNumberInfo::Precision(p) => (Some(*p as u8), None),
            ExactNumberInfo::PrecisionAndScale(p, s) => (Some(*p as u8), Some(*s as u8)),
        }
    }

    /// Convert default value expression
    fn convert_default(
        &self,
        expr: &sqlparser::ast::Expr,
    ) -> Result<DefaultValue, ParseError> {
        use sqlparser::ast::Expr;
        use sqlparser::ast::Value;

        match expr {
            // NULL
            Expr::Value(Value::Null) => Ok(DefaultValue::Null),

            // Boolean: TRUE / FALSE
            Expr::Value(Value::Boolean(b)) => Ok(DefaultValue::Boolean(*b)),

            // Number: could be integer or float
            Expr::Value(Value::Number(n, _)) => {
                // Try parsing as integer first
                if let Ok(i) = n.parse::<i64>() {
                    Ok(DefaultValue::Integer(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(DefaultValue::Float(f))
                } else {
                    // Fallback to expression string
                    Ok(DefaultValue::Expression(n.clone()))
                }
            }

            // String: 'value'
            Expr::Value(Value::SingleQuotedString(s)) => {
                Ok(DefaultValue::String(s.clone()))
            }
            Expr::Value(Value::DoubleQuotedString(s)) => {
                Ok(DefaultValue::String(s.clone()))
            }

            // Function call: NOW(), UUID(), etc.
            Expr::Function(f) => {
                let name = f.name.to_string().to_uppercase();
                match name.as_str() {
                    "NOW" | "CURRENT_TIMESTAMP" => Ok(DefaultValue::CurrentTimestamp),
                    "CURDATE" | "CURRENT_DATE" => Ok(DefaultValue::CurrentDate),
                    "UUID" => Ok(DefaultValue::Uuid),
                    _ => Ok(DefaultValue::Expression(expr.to_string())),
                }
            }

            // Identifier: CURRENT_TIMESTAMP, NULL, TRUE, FALSE
            Expr::Identifier(id) => {
                let name = id.value.to_uppercase();
                match name.as_str() {
                    "CURRENT_TIMESTAMP" | "NOW" => Ok(DefaultValue::CurrentTimestamp),
                    "CURRENT_DATE" => Ok(DefaultValue::CurrentDate),
                    "NULL" => Ok(DefaultValue::Null),
                    "TRUE" => Ok(DefaultValue::Boolean(true)),
                    "FALSE" => Ok(DefaultValue::Boolean(false)),
                    _ => Ok(DefaultValue::Expression(expr.to_string())),
                }
            }

            // Nested expression: (UUID()), (CURRENT_TIMESTAMP), etc.
            // MySQL allows DEFAULT (expr) with parentheses
            Expr::Nested(inner) => self.convert_default(inner),

            // Anything else: store as expression string
            _ => Ok(DefaultValue::Expression(expr.to_string())),
        }
    }

    /// Convert a table constraint
    /// Returns Option because some constraints we might want to skip
    fn convert_constraint(
        &self,
        constraint: sqlparser::ast::TableConstraint,
    ) -> Result<Option<Constraint>, ParseError> {
        use sqlparser::ast::TableConstraint as SpConstraint;

        match constraint {
            // PRIMARY KEY
            SpConstraint::PrimaryKey { name, columns, .. } => {
                let constraint_name = match name {
                    Some(n) => Some(n.value),
                    None => None,
                };

                let column_names: Vec<String> = columns
                    .iter()
                    .map(|col| col.value.clone())
                    .collect();

                Ok(Some(Constraint::PrimaryKey {
                    name: constraint_name,
                    columns: column_names,
                }))
            }

            // UNIQUE
            SpConstraint::Unique { name, columns, .. } => {
                let constraint_name = match name {
                    Some(n) => Some(n.value),
                    None => None,
                };

                let column_names: Vec<String> = columns
                    .iter()
                    .map(|col| col.value.clone())
                    .collect();

                Ok(Some(Constraint::Unique {
                    name: constraint_name,
                    columns: column_names,
                }))
            }

            // FOREIGN KEY
            SpConstraint::ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                ..
            } => {
                let constraint_name = match name {
                    Some(n) => Some(n.value),
                    None => None,
                };

                let column_names: Vec<String> = columns
                    .iter()
                    .map(|col| col.value.clone())
                    .collect();

                let ref_column_names: Vec<String> = referred_columns
                    .iter()
                    .map(|col| col.value.clone())
                    .collect();

                Ok(Some(Constraint::ForeignKey {
                    name: constraint_name,
                    columns: column_names,
                    ref_table: self.strip_backticks(&foreign_table.to_string()),
                    ref_columns: ref_column_names,
                    on_delete: on_delete.as_ref().map(|a| self.convert_ref_action(a)),
                    on_update: on_update.as_ref().map(|a| self.convert_ref_action(a)),
                }))
            }

            // KEY / INDEX (non-unique index)
            SpConstraint::Index { name, columns, .. } => {
                let index_name = match name {
                    Some(n) => n.value.clone(),
                    None => "unnamed_idx".to_string(),
                };

                let index_columns: Vec<IndexColumn> = columns
                    .iter()
                    .map(|col| IndexColumn {
                        name: col.value.clone(),
                        descending: false,
                    })
                    .collect();

                Ok(Some(Constraint::Index {
                    name: index_name,
                    columns: index_columns,
                    unique: false,
                }))
            }

            // Skip unsupported constraints for now
            _ => Ok(None),
        }
    }

    /// Convert INSERT statement
    fn convert_insert(
        &self,
        insert: sqlparser::ast::Insert,
    ) -> Result<Statement, ParseError> {
        // 1. Table name
        let table = self.strip_backticks(&insert.table_name.to_string());

        // 2. Column names (optional)
        let columns = if insert.columns.is_empty() {
            None
        } else {
            Some(insert.columns.iter().map(|c| c.value.clone()).collect())
        };

        // 3. Idempotency flag — `INSERT IGNORE INTO …`. Without this,
        // seed-data INSERTs in mysqldump output fail on redeploy with
        // ERROR 1062 (duplicate key).
        let ignore = insert.ignore;

        // 4. Extract value rows from the source Query
        // INSERT INTO t VALUES (...), (...) is parsed as a Query with a SetExpr::Values body
        let values = match insert.source {
            Some(source) => self.extract_values_from_query(&source)?,
            None => Vec::new(),
        };

        Ok(Statement::Insert {
            table,
            columns,
            values,
            ignore,
        })
    }

    /// Convert ALTER TABLE statement
    fn convert_alter_table(
        &self,
        table_name: &str,
        operations: Vec<sqlparser::ast::AlterTableOperation>,
    ) -> Result<Statement, ParseError> {
        use sqlparser::ast::AlterTableOperation as SpAlterOp;

        let mut converted_ops = Vec::new();

        for op in operations {
            match op {
                SpAlterOp::AddColumn { column_def, .. } => {
                    let column = self.convert_column(column_def)?;
                    converted_ops.push(crate::ast::AlterOperation::AddColumn(column));
                }
                SpAlterOp::ModifyColumn { col_name, data_type, options, .. } => {
                    // Wrap into a ColumnDef so we can reuse convert_column
                    let column_options: Vec<sqlparser::ast::ColumnOptionDef> = options
                        .into_iter()
                        .map(|opt| sqlparser::ast::ColumnOptionDef {
                            name: None,
                            option: opt,
                        })
                        .collect();
                    let column_def = sqlparser::ast::ColumnDef {
                        name: col_name,
                        data_type,
                        collation: None,
                        options: column_options,
                    };
                    let column = self.convert_column(column_def)?;
                    converted_ops.push(crate::ast::AlterOperation::ModifyColumn(column));
                }
                SpAlterOp::DropColumn { column_name, .. } => {
                    converted_ops.push(crate::ast::AlterOperation::DropColumn {
                        name: column_name.value,
                    });
                }
                SpAlterOp::RenameColumn { old_column_name, new_column_name } => {
                    converted_ops.push(crate::ast::AlterOperation::RenameColumn {
                        old_name: old_column_name.value,
                        new_name: new_column_name.value,
                    });
                }
                _ => {
                    return Err(ParseError::new(format!(
                        "Unsupported ALTER TABLE operation: {:?}",
                        op
                    )));
                }
            }
        }

        Ok(Statement::AlterTable {
            name: table_name.to_string(),
            operations: converted_ops,
        })
    }

    /// Extract value rows from a Query (e.g., VALUES (1, 'a'), (2, 'b'))
    fn extract_values_from_query(
        &self,
        query: &sqlparser::ast::Query,
    ) -> Result<Vec<Vec<Value>>, ParseError> {
        use sqlparser::ast::SetExpr;

        match query.body.as_ref() {
            SetExpr::Values(values) => {
                let mut rows = Vec::new();
                for row in &values.rows {
                    let mut converted_row = Vec::new();
                    for expr in row {
                        let value = self.convert_value(expr)?;
                        converted_row.push(value);
                    }
                    rows.push(converted_row);
                }
                Ok(rows)
            }
            _ => Err(ParseError::new(
                "INSERT source is not a VALUES expression"
            )),
        }
    }

    /// Convert an expression to our Value type (for INSERT values)
    fn convert_value(
        &self,
        expr: &sqlparser::ast::Expr,
    ) -> Result<Value, ParseError> {
        use sqlparser::ast::Expr;
        use sqlparser::ast::Value as SpValue;

        match expr {
            Expr::Value(SpValue::Null) => Ok(Value::Null),
            Expr::Value(SpValue::Boolean(b)) => Ok(Value::Boolean(*b)),
            Expr::Value(SpValue::Number(n, _)) => {
                // Try integer first, then float
                if let Ok(i) = n.parse::<i64>() {
                    Ok(Value::Integer(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Value::Float(f))
                } else {
                    Ok(Value::Expression(n.clone()))
                }
            }
            Expr::Value(SpValue::SingleQuotedString(s)) => {
                Ok(Value::String(s.clone()))
            }
            Expr::Value(SpValue::DoubleQuotedString(s)) => {
                Ok(Value::String(s.clone()))
            }
            // Function calls like NOW(), UUID()
            Expr::Function(f) => {
                let name = f.name.to_string().to_uppercase();
                match name.as_str() {
                    "NOW" | "CURRENT_TIMESTAMP" => Ok(Value::CurrentTimestamp),
                    "UUID" => Ok(Value::Uuid),
                    _ => Ok(Value::Expression(expr.to_string())),
                }
            }
            // Anything else: store as expression string
            _ => Ok(Value::Expression(expr.to_string())),
        }
    }

    /// Preprocess SQL to remove statements that sqlparser can't handle
    /// (e.g., CREATE USER, GRANT). These are MySQL admin statements, not schema DDL.
    fn preprocess_sql(&self, sql: &str) -> String {
        sql.lines()
            .filter(|line| {
                let trimmed = line.trim().to_uppercase();
                !trimmed.starts_with("CREATE USER")
                    && !trimmed.starts_with("GRANT ")
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Strip MySQL version-conditional bang-comments, selectively keeping the
    /// contents we know sqlparser-rs can handle and dropping the rest.
    ///
    /// mysqldump output is full of `/*!VERSION ... */` for cross-version
    /// safety: `/*!32312 IF NOT EXISTS*/`, `/*!40100 DEFAULT CHARACTER SET …*/`,
    /// `/*!80016 DEFAULT ENCRYPTION=…*/`, etc. sqlparser-rs treats the whole
    /// wrapper as a regular SQL comment and silently drops the content.
    ///
    /// We can't just naively unwrap them all — sqlparser 0.52 doesn't accept
    /// `DEFAULT CHARACTER SET …` on `CREATE DATABASE`, so naive unwrap turns
    /// valid SQL into a parse error. So we do a small allowlist:
    ///
    ///   * Bang-comments whose body is `IF NOT EXISTS` (or `IF EXISTS`) are
    ///     unwrapped — that's the one form whose semantics we MUST preserve
    ///     for idempotent DDL.
    ///   * All other bang-comments are stripped wholesale (replaced with a
    ///     single space). Their contents would be dropped by sqlparser anyway;
    ///     dropping the wrapper too just avoids fooling the parser into
    ///     misreading them as bare SQL.
    pub(crate) fn unwrap_version_comments(sql: &str) -> String {
        let mut out = String::with_capacity(sql.len());
        let mut remaining = sql;
        while let Some(start) = remaining.find("/*!") {
            out.push_str(&remaining[..start]);
            let after_marker = &remaining[start + 3..];
            let body_offset = after_marker
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(after_marker.len());
            let after_version = &after_marker[body_offset..];
            match after_version.find("*/") {
                Some(end) => {
                    let body = after_version[..end].trim();
                    let upper = body.to_ascii_uppercase();
                    // Preserve idempotency-bearing forms only.
                    if upper == "IF NOT EXISTS" || upper == "IF EXISTS" {
                        out.push(' ');
                        out.push_str(body);
                        out.push(' ');
                    } else {
                        // Drop the whole wrapper. Replace with a single space
                        // so neighbouring tokens don't fuse.
                        out.push(' ');
                    }
                    remaining = &after_version[end + 2..];
                }
                None => {
                    // Unterminated bang-comment — leave verbatim.
                    out.push_str("/*!");
                    remaining = after_marker;
                }
            }
        }
        out.push_str(remaining);
        out
    }

    /// Strip the `IF NOT EXISTS` clause that follows `ADD` / `ADD COLUMN` in
    /// `ALTER TABLE` statements.
    ///
    /// MySQL/MariaDB accept `ALTER TABLE t ADD [COLUMN] IF NOT EXISTS c …` for
    /// idempotent migrations, but sqlparser 0.52's `MySqlDialect` rejects the
    /// clause and fails the whole parse (`Expected: end of statement, found:
    /// EXISTS`). Oracle has no equivalent idempotency clause on `ADD`, so —
    /// consistent with how we drop `CREATE INDEX IF NOT EXISTS` — we simply
    /// remove the clause before parsing and emit a plain `ADD`.
    ///
    /// The match is deliberately narrow: only an `IF NOT EXISTS` that directly
    /// follows `ADD` or `ADD COLUMN` is removed. Other forms (`CREATE TABLE IF
    /// NOT EXISTS`, `WHERE NOT EXISTS (…)`) are left untouched.
    pub(crate) fn strip_add_column_if_not_exists(sql: &str) -> String {
        let bytes = sql.as_bytes();

        // Collect byte ranges of word tokens ([A-Za-z0-9_] runs).
        let mut words: Vec<(usize, usize)> = Vec::new();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' {
                let start = i;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                words.push((start, i));
            } else {
                i += 1;
            }
        }

        let kw = |r: (usize, usize), k: &str| sql[r.0..r.1].eq_ignore_ascii_case(k);

        // Byte ranges (start of IF .. end of EXISTS) to delete.
        let mut deletions: Vec<(usize, usize)> = Vec::new();
        let n = words.len();
        let mut idx = 0;
        while idx < n {
            if kw(words[idx], "add") {
                // Optional COLUMN keyword between ADD and IF.
                let mut j = idx + 1;
                if j < n && kw(words[j], "column") {
                    j += 1;
                }
                if j + 2 < n && kw(words[j], "if") && kw(words[j + 1], "not") && kw(words[j + 2], "exists")
                {
                    deletions.push((words[j].0, words[j + 2].1));
                    idx = j + 3;
                    continue;
                }
            }
            idx += 1;
        }

        if deletions.is_empty() {
            return sql.to_string();
        }

        // Rebuild the string without the deleted ranges. A leftover double
        // space (e.g. "ADD COLUMN  c") is harmless to the parser.
        let mut out = String::with_capacity(sql.len());
        let mut pos = 0;
        for (s, e) in deletions {
            out.push_str(&sql[pos..s]);
            pos = e;
        }
        out.push_str(&sql[pos..]);
        out
    }

    /// Strip backticks from identifiers
    /// e.g., `users` → users, `account` → account
    fn strip_backticks(&self, name: &str) -> String {
        let trimmed = name.trim();
        if trimmed.starts_with('`') && trimmed.ends_with('`') {
            trimmed[1..trimmed.len() - 1].to_string()
        } else {
            trimmed.to_string()
        }
    }

    /// Convert referential action (CASCADE, SET NULL, etc.)
    fn convert_ref_action(&self, action: &sqlparser::ast::ReferentialAction) -> ReferentialAction {
        use sqlparser::ast::ReferentialAction as SpAction;

        match action {
            SpAction::Cascade => ReferentialAction::Cascade,
            SpAction::SetNull => ReferentialAction::SetNull,
            SpAction::SetDefault => ReferentialAction::SetDefault,
            SpAction::Restrict => ReferentialAction::Restrict,
            SpAction::NoAction => ReferentialAction::NoAction,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::*;

    #[test]
    fn test_parse_simple_create_table() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE users (id INT);";

        let result = parser.parse(sql);
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);

        match &statements[0] {
            Statement::CreateTable(table) => {
                assert_eq!(table.name, "users");
                assert_eq!(table.columns.len(), 1);
                assert_eq!(table.columns[0].name, "id");
            }
            _ => panic!("Expected CreateTable statement"),
        }
    }

    #[test]
    fn test_parse_table_with_varchar() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE users (name VARCHAR(100));";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert_eq!(table.columns[0].name, "name");
        match &table.columns[0].data_type {
            DataType::String(StringType::Varchar { length }) => {
                assert_eq!(*length, 100);
            }
            _ => panic!("Expected VARCHAR type"),
        }
    }

    #[test]
    fn test_parse_table_with_not_null() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE users (id INT NOT NULL);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert_eq!(table.columns[0].nullable, false);
    }

    #[test]
    fn test_parse_table_with_primary_key() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE users (id INT, PRIMARY KEY (id));";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        let has_pk = table.constraints.iter().any(|c| {
            matches!(c, Constraint::PrimaryKey { columns, .. } if columns == &vec!["id".to_string()])
        });
        assert!(has_pk, "Expected PRIMARY KEY constraint");
    }

    #[test]
    fn test_parse_tinyint() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE t (age TINYINT);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert!(
            matches!(table.columns[0].data_type, DataType::Integer(IntegerType::TinyInt)),
            "Expected TINYINT, got {:?}",
            table.columns[0].data_type
        );
    }

    #[test]
    fn test_parse_smallint() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE t (count SMALLINT);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert!(
            matches!(table.columns[0].data_type, DataType::Integer(IntegerType::SmallInt)),
            "Expected SMALLINT, got {:?}",
            table.columns[0].data_type
        );
    }

    #[test]
    fn test_parse_bigint() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE t (big_id BIGINT);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert!(
            matches!(table.columns[0].data_type, DataType::Integer(IntegerType::BigInt)),
            "Expected BIGINT, got {:?}",
            table.columns[0].data_type
        );
    }

    // ========== Text types ==========

    #[test]
    fn test_parse_char() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE t (code CHAR(10));";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        match &table.columns[0].data_type {
            DataType::String(StringType::Char { length }) => {
                assert_eq!(*length, 10);
            }
            _ => panic!("Expected CHAR, got {:?}", table.columns[0].data_type),
        }
    }

    #[test]
    fn test_parse_text() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE t (content TEXT);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert!(
            matches!(table.columns[0].data_type, DataType::String(StringType::Text { .. })),
            "Expected TEXT, got {:?}",
            table.columns[0].data_type
        );
    }

    // ========== Temporal types ==========

    #[test]
    fn test_parse_date() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE t (birth_date DATE);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert!(
            matches!(table.columns[0].data_type, DataType::Temporal(TemporalType::Date)),
            "Expected DATE, got {:?}",
            table.columns[0].data_type
        );
    }

    #[test]
    fn test_parse_datetime() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE t (created_at DATETIME);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert!(
            matches!(table.columns[0].data_type, DataType::Temporal(TemporalType::Datetime { .. })),
            "Expected DATETIME/Datetime, got {:?}",
            table.columns[0].data_type
        );
    }

    #[test]
    fn test_parse_timestamp() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE t (updated_at TIMESTAMP);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert!(
            matches!(table.columns[0].data_type, DataType::Temporal(TemporalType::Timestamp { .. })),
            "Expected TIMESTAMP, got {:?}",
            table.columns[0].data_type
        );
    }

    // ========== Decimal ==========

    #[test]
    fn test_parse_decimal() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE t (price DECIMAL(10,2));";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        match &table.columns[0].data_type {
            DataType::Decimal { precision, scale } => {
                assert_eq!(*precision, 10);
                assert_eq!(*scale, 2);
            }
            _ => panic!("Expected DECIMAL, got {:?}", table.columns[0].data_type),
        }
    }

    // ========== Boolean ==========

    #[test]
    fn test_parse_boolean() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE t (active BOOLEAN);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert!(
            matches!(table.columns[0].data_type, DataType::Boolean),
            "Expected BOOLEAN, got {:?}",
            table.columns[0].data_type
        );
    }

    // ========== More types ==========

    #[test]
    fn test_parse_blob() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE t (data BLOB);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert!(
            matches!(table.columns[0].data_type, DataType::Blob),
            "Expected BLOB, got {:?}",
            table.columns[0].data_type
        );
    }

    #[test]
    fn test_parse_json() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE t (metadata JSON);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert!(
            matches!(table.columns[0].data_type, DataType::Json),
            "Expected JSON, got {:?}",
            table.columns[0].data_type
        );
    }

    #[test]
    fn test_parse_float() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE t (score FLOAT);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert!(
            matches!(table.columns[0].data_type, DataType::Float),
            "Expected FLOAT, got {:?}",
            table.columns[0].data_type
        );
    }

    #[test]
    fn test_parse_double() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE t (amount DOUBLE);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert!(
            matches!(table.columns[0].data_type, DataType::Float),
            "Expected DOUBLE/Float, got {:?}",
            table.columns[0].data_type
        );
    }

    #[test]
    fn test_parse_enum() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE t (status ENUM('active', 'inactive', 'pending'));";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        match &table.columns[0].data_type {
            DataType::Enum { values } => {
                assert_eq!(values.len(), 3);
                assert_eq!(values[0], "active");
                assert_eq!(values[1], "inactive");
                assert_eq!(values[2], "pending");
            }
            _ => panic!("Expected ENUM, got {:?}", table.columns[0].data_type),
        }
    }

    // ========== DEFAULT values ==========

    #[test]
    fn test_parse_default_null() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE t (name VARCHAR(100) DEFAULT NULL);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert_eq!(table.columns[0].default, Some(DefaultValue::Null));
    }

    #[test]
    fn test_parse_default_string() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE t (status VARCHAR(20) DEFAULT 'pending');";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert_eq!(
            table.columns[0].default,
            Some(DefaultValue::String("pending".to_string()))
        );
    }

    #[test]
    fn test_parse_default_integer() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE t (count INT DEFAULT 0);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert_eq!(table.columns[0].default, Some(DefaultValue::Integer(0)));
    }

    #[test]
    fn test_parse_default_boolean() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE t (active BOOLEAN DEFAULT TRUE);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert_eq!(table.columns[0].default, Some(DefaultValue::Boolean(true)));
    }

    #[test]
    fn test_parse_default_current_timestamp() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE t (created_at DATETIME DEFAULT CURRENT_TIMESTAMP);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert_eq!(table.columns[0].default, Some(DefaultValue::CurrentTimestamp));
    }

    // ========== ON UPDATE CURRENT_TIMESTAMP ==========

    #[test]
    fn test_parse_on_update_current_timestamp() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE t (updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert!(
            table.columns[0].on_update_timestamp,
            "Expected on_update_timestamp to be true"
        );
        assert_eq!(
            table.columns[0].default,
            Some(DefaultValue::CurrentTimestamp),
            "Should also have DEFAULT CURRENT_TIMESTAMP"
        );
    }

    #[test]
    fn test_parse_default_uuid_parenthesized() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE t (id VARCHAR(36) DEFAULT (UUID()));";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert_eq!(
            table.columns[0].default,
            Some(DefaultValue::Uuid),
            "DEFAULT (UUID()) should be recognized as Uuid, got {:?}",
            table.columns[0].default
        );
    }

    // ========== AUTO_INCREMENT ==========

    #[test]
    fn test_parse_auto_increment() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE t (id INT AUTO_INCREMENT);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert!(
            table.columns[0].auto_increment,
            "Expected auto_increment to be true"
        );
    }

    // ========== More constraints ==========

    #[test]
    fn test_parse_unique_constraint() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE t (email VARCHAR(100), UNIQUE (email));";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        let has_unique = table.constraints.iter().any(|c| {
            matches!(c, Constraint::Unique { columns, .. } if columns == &vec!["email".to_string()])
        });
        assert!(has_unique, "Expected UNIQUE constraint");
    }

    #[test]
    fn test_parse_key_index() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE t (
            org_id VARCHAR(255),
            KEY org_id (org_id)
        );";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        let has_index = table.constraints.iter().any(|c| {
            match c {
                Constraint::Index { name, columns, unique } => {
                    name == "org_id"
                        && columns.len() == 1
                        && columns[0].name == "org_id"
                        && !unique
                }
                _ => false,
            }
        });
        assert!(has_index, "Expected KEY index constraint, got: {:?}", table.constraints);
    }

    #[test]
    fn test_parse_foreign_key() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE orders (
            id INT,
            user_id INT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        let has_fk = table.constraints.iter().any(|c| {
            match c {
                Constraint::ForeignKey { columns, ref_table, ref_columns, .. } => {
                    columns == &vec!["user_id".to_string()]
                        && ref_table == "users"
                        && ref_columns == &vec!["id".to_string()]
                }
                _ => false,
            }
        });
        assert!(has_fk, "Expected FOREIGN KEY constraint");
    }

    #[test]
    fn test_parse_foreign_key_with_actions() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE orders (
            id INT,
            user_id INT,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE ON UPDATE SET NULL
        );";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        let fk = table.constraints.iter().find_map(|c| {
            match c {
                Constraint::ForeignKey { on_delete, on_update, .. } => Some((on_delete, on_update)),
                _ => None,
            }
        });

        assert!(fk.is_some(), "Expected FOREIGN KEY constraint");
        let (on_delete, on_update) = fk.unwrap();
        assert_eq!(*on_delete, Some(ReferentialAction::Cascade));
        assert_eq!(*on_update, Some(ReferentialAction::SetNull));
    }

    // ========== TINYINT(1) as Boolean ==========

    #[test]
    fn test_parse_tinyint_1_as_boolean() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE t (active TINYINT(1) DEFAULT NULL);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert!(
            matches!(table.columns[0].data_type, DataType::Boolean),
            "TINYINT(1) should map to Boolean, got {:?}",
            table.columns[0].data_type
        );
    }

    #[test]
    fn test_parse_tinyint_without_width_stays_tinyint() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE t (age TINYINT);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert!(
            matches!(table.columns[0].data_type, DataType::Integer(IntegerType::TinyInt)),
            "TINYINT (no width) should stay as TinyInt, got {:?}",
            table.columns[0].data_type
        );
    }

    // ========== Backtick stripping ==========

    #[test]
    fn test_parse_backtick_table_name() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE `users` (`id` INT);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert_eq!(table.name, "users", "Backticks should be stripped from table name");
        assert_eq!(table.columns[0].name, "id", "Backticks should be stripped from column name");
    }

    #[test]
    fn test_parse_backtick_foreign_key_ref() {
        let parser = MySqlParser::new();
        let sql = "CREATE TABLE `orders` (
            `id` INT,
            `user_id` INT,
            FOREIGN KEY (`user_id`) REFERENCES `users`(`id`)
        );";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        let fk = table.constraints.iter().find_map(|c| {
            match c {
                Constraint::ForeignKey { ref_table, .. } => Some(ref_table.clone()),
                _ => None,
            }
        });

        assert_eq!(fk, Some("users".to_string()), "Backticks should be stripped from FK ref table");
    }

    // ========== DROP TABLE ==========

    #[test]
    fn test_parse_drop_table() {
        let parser = MySqlParser::new();
        let sql = "DROP TABLE users;";

        let result = parser.parse(sql).unwrap();

        match &result[0] {
            Statement::DropTable { name, if_exists, cascade } => {
                assert_eq!(name, "users");
                assert_eq!(*if_exists, false);
                assert_eq!(*cascade, false);
            }
            _ => panic!("Expected DropTable statement"),
        }
    }

    // ========== CREATE DATABASE ==========

    // ========== LOCK/UNLOCK TABLES ==========

    #[test]
    fn test_parse_lock_tables() {
        let parser = MySqlParser::new();
        let sql = "LOCK TABLES org_template WRITE;";

        let result = parser.parse(sql).unwrap();

        match &result[0] {
            Statement::LockTables { tables } => {
                assert_eq!(tables.len(), 1);
                assert_eq!(tables[0].0, "org_template");
                assert_eq!(tables[0].1, LockMode::Write);
            }
            _ => panic!("Expected LockTables statement, got {:?}", result[0]),
        }
    }

    #[test]
    fn test_parse_unlock_tables() {
        let parser = MySqlParser::new();
        let sql = "UNLOCK TABLES;";

        let result = parser.parse(sql).unwrap();

        assert!(
            matches!(result[0], Statement::UnlockTables),
            "Expected UnlockTables, got {:?}",
            result[0]
        );
    }

    // ========== INSERT ==========

    #[test]
    fn test_parse_insert_simple() {
        let parser = MySqlParser::new();
        let sql = "INSERT INTO t VALUES ('hello', 42, NULL);";

        let result = parser.parse(sql).unwrap();

        match &result[0] {
            Statement::Insert { table, columns, values, .. } => {
                assert_eq!(table, "t");
                assert!(columns.is_none(), "No column list in this INSERT");
                assert_eq!(values.len(), 1, "One row of values");
                assert_eq!(values[0].len(), 3, "Three values in the row");
            }
            _ => panic!("Expected Insert statement, got {:?}", result[0]),
        }
    }

    #[test]
    fn test_parse_insert_multi_row() {
        let parser = MySqlParser::new();
        let sql = "INSERT INTO t VALUES ('a', 1), ('b', 2);";

        let result = parser.parse(sql).unwrap();

        match &result[0] {
            Statement::Insert { table, values, .. } => {
                assert_eq!(table, "t");
                assert_eq!(values.len(), 2, "Two rows of values");
            }
            _ => panic!("Expected Insert statement, got {:?}", result[0]),
        }
    }

    // ========== USE ==========

    #[test]
    fn test_parse_use_database() {
        let parser = MySqlParser::new();
        let sql = "USE `account`;";

        let result = parser.parse(sql).unwrap();

        match &result[0] {
            Statement::Use { database } => {
                assert_eq!(database, "account");
            }
            _ => panic!("Expected Use statement, got {:?}", result[0]),
        }
    }

    // ========== CREATE DATABASE ==========

    #[test]
    fn test_parse_create_database() {
        let parser = MySqlParser::new();
        let sql = "CREATE DATABASE IF NOT EXISTS account;";

        let result = parser.parse(sql).unwrap();

        match &result[0] {
            Statement::CreateDatabase { name, if_not_exists } => {
                assert_eq!(name, "account");
                assert_eq!(*if_not_exists, true);
            }
            _ => panic!("Expected CreateDatabase statement"),
        }
    }

    #[test]
    fn test_parse_create_database_backticks() {
        let parser = MySqlParser::new();
        let sql = "CREATE DATABASE `mydb`;";

        let result = parser.parse(sql).unwrap();

        match &result[0] {
            Statement::CreateDatabase { name, if_not_exists } => {
                assert_eq!(name, "mydb");
                assert_eq!(*if_not_exists, false);
            }
            _ => panic!("Expected CreateDatabase statement"),
        }
    }

    #[test]
    fn test_parse_drop_table_if_exists() {
        let parser = MySqlParser::new();
        let sql = "DROP TABLE IF EXISTS users;";

        let result = parser.parse(sql).unwrap();

        match &result[0] {
            Statement::DropTable { name, if_exists, .. } => {
                assert_eq!(name, "users");
                assert_eq!(*if_exists, true);
            }
            _ => panic!("Expected DropTable statement"),
        }
    }

    #[test]
    fn test_parse_alter_add_column_if_not_exists() {
        let parser = MySqlParser::new();
        // MySQL/MariaDB idempotent migration form that sqlparser 0.52 rejects.
        let sql = "ALTER TABLE wa_channels ADD COLUMN IF NOT EXISTS waba_id varchar(36);";

        let result = parser.parse(sql);
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);

        match &statements[0] {
            Statement::AlterTable { name, operations } => {
                assert_eq!(name, "wa_channels");
                assert_eq!(operations.len(), 1);
                match &operations[0] {
                    AlterOperation::AddColumn(col) => assert_eq!(col.name, "waba_id"),
                    other => panic!("Expected AddColumn, got {:?}", other),
                }
            }
            other => panic!("Expected AlterTable, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_alter_add_if_not_exists_no_column_keyword() {
        // MariaDB also allows `ADD IF NOT EXISTS` without the COLUMN keyword.
        let parser = MySqlParser::new();
        let sql = "alter table t add if not exists c int;";
        let result = parser.parse(sql);
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    }

    #[test]
    fn test_strip_add_column_if_not_exists_leaves_other_forms_untouched() {
        // CREATE TABLE IF NOT EXISTS and WHERE NOT EXISTS must NOT be altered.
        let create = "CREATE TABLE IF NOT EXISTS t (id INT);";
        assert_eq!(MySqlParser::strip_add_column_if_not_exists(create), create);

        let subquery = "DELETE FROM t WHERE NOT EXISTS (SELECT 1 FROM u);";
        assert_eq!(MySqlParser::strip_add_column_if_not_exists(subquery), subquery);

        // The ADD COLUMN form should have its IF NOT EXISTS removed.
        let add = "ALTER TABLE t ADD COLUMN IF NOT EXISTS c int;";
        let stripped = MySqlParser::strip_add_column_if_not_exists(add);
        assert!(
            !stripped.to_ascii_uppercase().contains("IF NOT EXISTS"),
            "IF NOT EXISTS should be stripped: {:?}",
            stripped
        );
        assert!(stripped.to_ascii_uppercase().contains("ADD COLUMN"));
    }
}

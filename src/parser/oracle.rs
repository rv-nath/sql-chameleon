// Oracle Parser

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use super::SqlParser;
use crate::ast::{
    AlterOperation, Column, Constraint, DataType, DefaultValue, Dialect, IndexColumn, IntegerType,
    ReferentialAction, Statement, StringType, Table, TemporalType, Value,
};
use crate::error::ParseError;

/// Parser for Oracle SQL dialect
pub struct OracleParser;

impl OracleParser {
    pub fn new() -> Self {
        Self
    }
}

impl SqlParser for OracleParser {
    fn dialect(&self) -> Dialect {
        Dialect::Oracle
    }

    fn parse(&self, sql: &str) -> Result<Vec<Statement>, ParseError> {
        // Two-pass approach:
        // 1. Extract constructs that GenericDialect can't handle (PL/SQL triggers,
        //    CREATE SYNONYM, SET DEFINE OFF, ALTER SESSION, GRANT/REVOKE)
        // 2. Parse the cleaned SQL with sqlparser, then merge extracted statements
        let (cleaned_sql, mut extracted) = self.preprocess_sql(sql);

        if cleaned_sql.trim().is_empty() {
            return Ok(extracted);
        }

        let dialect = GenericDialect {};
        let parsed_statements = match Parser::parse_sql(&dialect, &cleaned_sql) {
            Ok(stmts) => stmts,
            Err(e) => {
                return Err(ParseError::new(format!(
                    "Failed to parse cleaned Oracle SQL: {}",
                    e
                )));
            }
        };

        let mut results = Vec::new();
        for stmt in parsed_statements {
            match self.convert_statement(stmt) {
                Ok(converted) => results.push(converted),
                Err(_) => {
                    // Skip statements that can't be converted rather than failing
                    // (Oracle SQL has many constructs we don't support yet)
                }
            }
        }

        // Extracted statements (triggers, synonyms, etc.) come after parsed ones
        results.append(&mut extracted);

        Ok(results)
    }
}

// Statement conversion
impl OracleParser {
    /// Convert sqlparser's Statement to our neutral Statement
    fn convert_statement(&self, stmt: sqlparser::ast::Statement) -> Result<Statement, ParseError> {
        use sqlparser::ast::Statement as SpStatement;

        let raw_sql = stmt.to_string();

        match stmt {
            SpStatement::CreateTable(create) => self.convert_create_table(create),
            SpStatement::Drop {
                object_type,
                if_exists,
                names,
                cascade,
                ..
            } => self.convert_drop(object_type, if_exists, names, cascade),
            SpStatement::CreateIndex(create_index) => {
                let name = self.strip_quotes(
                    &create_index
                        .name
                        .map(|n| n.to_string())
                        .unwrap_or_else(|| "unnamed_idx".to_string()),
                );
                let table = self.strip_quotes(&create_index.table_name.to_string());
                let columns: Vec<IndexColumn> = create_index
                    .columns
                    .iter()
                    .map(|c| IndexColumn {
                        name: self.strip_quotes(&c.expr.to_string()),
                        descending: c.asc == Some(false),
                    })
                    .collect();

                Ok(Statement::CreateIndex {
                    name,
                    table,
                    columns,
                    unique: create_index.unique,
                })
            }
            SpStatement::Insert(insert) => self.convert_insert(insert),
            SpStatement::AlterTable {
                name, operations, ..
            } => self.convert_alter_table(&self.strip_quotes(&name.to_string()), operations),
            SpStatement::CreateSequence { name, .. } => {
                self.convert_sequence_from_raw(&raw_sql, &name.to_string())
            }
            SpStatement::CreateView {
                name,
                or_replace,
                columns,
                query,
                ..
            } => self.convert_view(name, or_replace, columns, query),
            SpStatement::Grant { .. } => Ok(Statement::Grant { raw_sql }),
            SpStatement::Revoke { .. } => Ok(Statement::Revoke { raw_sql }),
            SpStatement::Commit { .. } => Ok(Statement::Commit),
            SpStatement::Update { .. } | SpStatement::Delete(_) => {
                Ok(Statement::RawStatement { raw_sql })
            }
            _ => {
                let upper = raw_sql.to_uppercase();
                if upper.starts_with("SET ") {
                    Ok(Statement::SetVariable { raw_sql })
                } else {
                    Err(ParseError::new(format!(
                        "Unsupported Oracle statement: {}",
                        &raw_sql[..raw_sql.len().min(80)]
                    )))
                }
            }
        }
    }

    /// Convert CREATE TABLE
    fn convert_create_table(
        &self,
        create: sqlparser::ast::CreateTable,
    ) -> Result<Statement, ParseError> {
        let name = self.strip_quotes(&create.name.to_string());

        let mut columns = Vec::new();
        for col_def in create.columns {
            let column = self.convert_column(col_def)?;
            columns.push(column);
        }

        let mut constraints = Vec::new();
        for constraint in create.constraints {
            if let Some(c) = self.convert_constraint(constraint)? {
                constraints.push(c);
            }
        }

        Ok(Statement::CreateTable(Table {
            name,
            columns,
            constraints,
            comment: None,
        }))
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
                let name = names
                    .first()
                    .map(|n| self.strip_quotes(&n.to_string()))
                    .ok_or_else(|| ParseError::new("DROP TABLE missing table name"))?;

                Ok(Statement::DropTable {
                    name,
                    if_exists,
                    cascade,
                })
            }
            ObjectType::Sequence => {
                // DROP SEQUENCE — not in our AST, pass as raw
                let name = names.first().map(|n| n.to_string()).unwrap_or_default();
                Ok(Statement::RawStatement {
                    raw_sql: format!("DROP SEQUENCE {}", name),
                })
            }
            _ => Err(ParseError::new(format!(
                "Unsupported DROP type: {:?}",
                object_type
            ))),
        }
    }

    /// Convert INSERT statement
    fn convert_insert(&self, insert: sqlparser::ast::Insert) -> Result<Statement, ParseError> {
        let table = self.strip_quotes(&insert.table_name.to_string());

        let columns = if insert.columns.is_empty() {
            None
        } else {
            Some(
                insert
                    .columns
                    .iter()
                    .map(|c| self.strip_quotes(&c.value))
                    .collect(),
            )
        };

        let values = match insert.source {
            Some(source) => self.extract_values_from_query(&source)?,
            None => Vec::new(),
        };

        Ok(Statement::Insert {
            table,
            columns,
            values,
        })
    }

    /// Convert ALTER TABLE
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
                    converted_ops.push(AlterOperation::AddColumn(column));
                }
                SpAlterOp::ModifyColumn {
                    col_name,
                    data_type,
                    options,
                    ..
                } => {
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
                    converted_ops.push(AlterOperation::ModifyColumn(column));
                }
                SpAlterOp::DropColumn { column_name, .. } => {
                    converted_ops.push(AlterOperation::DropColumn {
                        name: self.strip_quotes(&column_name.value),
                    });
                }
                SpAlterOp::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => {
                    converted_ops.push(AlterOperation::RenameColumn {
                        old_name: self.strip_quotes(&old_column_name.value),
                        new_name: self.strip_quotes(&new_column_name.value),
                    });
                }
                SpAlterOp::AddConstraint(tc) => {
                    if let Some(c) = self.convert_constraint(tc)? {
                        converted_ops.push(AlterOperation::AddConstraint(c));
                    }
                }
                _ => {
                    // Skip unsupported ALTER TABLE operations
                }
            }
        }

        Ok(Statement::AlterTable {
            name: table_name.to_string(),
            operations: converted_ops,
        })
    }

    /// Convert CREATE SEQUENCE from raw SQL + parsed name
    fn convert_sequence_from_raw(
        &self,
        raw_sql: &str,
        name: &str,
    ) -> Result<Statement, ParseError> {
        let name = self.strip_quotes(name);
        let upper = raw_sql.to_uppercase();

        let start_with = self.extract_sequence_param(&upper, "START WITH");
        let increment_by = self.extract_sequence_param(&upper, "INCREMENT BY");
        let min_value = self.extract_sequence_param(&upper, "MINVALUE");
        let max_value = self.extract_sequence_param(&upper, "MAXVALUE");

        let no_cache = upper.contains("NOCACHE");
        let cache = if !no_cache {
            self.extract_sequence_param(&upper, "CACHE")
                .map(|v| v as u64)
        } else {
            None
        };

        let cycle = upper.contains("CYCLE") && !upper.contains("NOCYCLE");

        Ok(Statement::CreateSequence {
            name,
            start_with,
            increment_by,
            min_value,
            max_value,
            cache,
            no_cache,
            cycle,
        })
    }

    /// Extract a numeric parameter from sequence DDL, e.g. "START WITH 1"
    fn extract_sequence_param(&self, upper_sql: &str, keyword: &str) -> Option<i64> {
        if let Some(pos) = upper_sql.find(keyword) {
            let after = &upper_sql[pos + keyword.len()..];
            let trimmed = after.trim_start();
            let num_str: String = trimmed
                .chars()
                .take_while(|c| c.is_ascii_digit() || *c == '-')
                .collect();
            num_str.parse().ok()
        } else {
            None
        }
    }

    /// Convert CREATE VIEW
    fn convert_view(
        &self,
        name: sqlparser::ast::ObjectName,
        or_replace: bool,
        columns: Vec<sqlparser::ast::ViewColumnDef>,
        query: Box<sqlparser::ast::Query>,
    ) -> Result<Statement, ParseError> {
        let view_name = self.strip_quotes(&name.to_string());
        let col_names = if columns.is_empty() {
            None
        } else {
            Some(
                columns
                    .iter()
                    .map(|c| self.strip_quotes(&c.name.value))
                    .collect(),
            )
        };

        Ok(Statement::CreateView {
            name: view_name,
            or_replace,
            columns: col_names,
            query: query.to_string(),
        })
    }
}

// Column & type conversion
impl OracleParser {
    /// Convert a column definition
    fn convert_column(&self, col: sqlparser::ast::ColumnDef) -> Result<Column, ParseError> {
        use sqlparser::ast::ColumnOption;

        let name = self.strip_quotes(&col.name.value);
        let data_type = self.convert_data_type(&col.data_type)?;

        let mut nullable = true;
        let mut default = None;
        let auto_increment = false; // Oracle uses sequences, not auto_increment

        for option in &col.options {
            match &option.option {
                ColumnOption::Null => nullable = true,
                ColumnOption::NotNull => nullable = false,
                ColumnOption::Default(expr) => {
                    default = Some(self.convert_default(expr)?);
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
            on_update_timestamp: false, // Oracle doesn't have this
            comment: None,
        })
    }

    /// Convert Oracle data type to our intermediate AST DataType
    fn convert_data_type(&self, dt: &sqlparser::ast::DataType) -> Result<DataType, ParseError> {
        use sqlparser::ast::DataType as SpDataType;

        match dt {
            // NUMBER(p,s) — Oracle's universal numeric type
            SpDataType::Numeric(info) | SpDataType::Decimal(info) => self.convert_number_type(info),

            // Integer types (Oracle supports these in SQL but they're aliases)
            SpDataType::Int(_) | SpDataType::Integer(_) => Ok(DataType::Integer(IntegerType::Int)),
            SpDataType::SmallInt(_) => Ok(DataType::Integer(IntegerType::SmallInt)),
            SpDataType::BigInt(_) => Ok(DataType::Integer(IntegerType::BigInt)),
            SpDataType::TinyInt(_) => Ok(DataType::Integer(IntegerType::TinyInt)),

            // VARCHAR2(n CHAR) / VARCHAR(n)
            SpDataType::Varchar(len) | SpDataType::CharVarying(len) => {
                Ok(self.convert_varchar2(len))
            }

            // CHAR(n)
            SpDataType::Char(len) | SpDataType::Character(len) => {
                let length = self.extract_char_length(len).unwrap_or(1);
                Ok(DataType::String(StringType::Char { length }))
            }

            // NVARCHAR2 / NCHAR — treat like VARCHAR/CHAR
            SpDataType::Nvarchar(len) => {
                let length = self.extract_char_length(len).unwrap_or(255);
                Ok(DataType::String(StringType::Varchar { length }))
            }

            // CLOB / NCLOB → Text
            SpDataType::Clob(_) => Ok(DataType::String(StringType::Text { max_bytes: None })),

            // BLOB
            SpDataType::Blob(_) => Ok(DataType::Blob),

            // DATE
            SpDataType::Date => Ok(DataType::Temporal(TemporalType::Date)),

            // TIMESTAMP / TIMESTAMP WITH TIME ZONE
            SpDataType::Timestamp(precision, tz_info) => {
                let with_timezone = matches!(
                    tz_info,
                    sqlparser::ast::TimezoneInfo::WithTimeZone | sqlparser::ast::TimezoneInfo::Tz
                );
                Ok(DataType::Temporal(TemporalType::Timestamp {
                    precision: precision.map(|p| p as u8),
                    with_timezone,
                }))
            }

            // FLOAT / DOUBLE
            SpDataType::Float(_) | SpDataType::Double | SpDataType::DoublePrecision => {
                Ok(DataType::Float)
            }

            // BOOLEAN (Oracle 23c+ has native BOOLEAN, older via NUMBER(1))
            SpDataType::Boolean | SpDataType::Bool => Ok(DataType::Boolean),

            // BINARY / RAW
            SpDataType::Binary(len) => self.convert_raw(len.map(|l| l as u32)),
            SpDataType::Varbinary(len) => self.convert_raw(len.map(|l| l as u32)),

            // Custom types — VARCHAR2, RAW, NUMBER, NCLOB, etc.
            SpDataType::Custom(name, params) => {
                let type_name = name.to_string().to_uppercase();
                self.convert_custom_type(&type_name, params)
            }

            _ => Err(ParseError::new(format!(
                "Unsupported Oracle data type: {:?}",
                dt
            ))),
        }
    }

    /// Convert Oracle NUMBER(p,s) to AST type using precision/scale heuristics
    fn convert_number_type(
        &self,
        info: &sqlparser::ast::ExactNumberInfo,
    ) -> Result<DataType, ParseError> {
        use sqlparser::ast::ExactNumberInfo;

        match info {
            // NUMBER (no args) → Float
            ExactNumberInfo::None => Ok(DataType::Float),

            // NUMBER(p) — integer by precision
            ExactNumberInfo::Precision(p) => {
                let p = *p as u8;
                Ok(match p {
                    1 => DataType::Boolean,
                    2..=3 => DataType::Integer(IntegerType::TinyInt),
                    4..=5 => DataType::Integer(IntegerType::SmallInt),
                    6..=10 => DataType::Integer(IntegerType::Int),
                    _ => DataType::Integer(IntegerType::BigInt),
                })
            }

            // NUMBER(p,s) — if scale > 0, it's decimal
            ExactNumberInfo::PrecisionAndScale(p, s) => {
                let p = *p as u8;
                let s = *s as u8;
                if s == 0 {
                    // Integer by precision
                    Ok(match p {
                        1 => DataType::Boolean,
                        2..=3 => DataType::Integer(IntegerType::TinyInt),
                        4..=5 => DataType::Integer(IntegerType::SmallInt),
                        6..=10 => DataType::Integer(IntegerType::Int),
                        _ => DataType::Integer(IntegerType::BigInt),
                    })
                } else {
                    Ok(DataType::Decimal {
                        precision: p,
                        scale: s,
                    })
                }
            }
        }
    }

    /// Convert VARCHAR2(n CHAR) length
    fn convert_varchar2(&self, len: &Option<sqlparser::ast::CharacterLength>) -> DataType {
        let length = self.extract_char_length(len).unwrap_or(255);
        DataType::String(StringType::Varchar { length })
    }

    /// Convert RAW(n) — RAW(16) is typically UUID
    fn convert_raw(&self, length: Option<u32>) -> Result<DataType, ParseError> {
        match length {
            Some(16) => Ok(DataType::Uuid),
            Some(n) => Ok(DataType::Binary { length: Some(n) }),
            None => Ok(DataType::Blob),
        }
    }

    /// Convert Oracle custom types parsed by sqlparser as Custom(name, params)
    fn convert_custom_type(
        &self,
        type_name: &str,
        params: &[String],
    ) -> Result<DataType, ParseError> {
        match type_name {
            "VARCHAR2" => {
                // Extract length from params like ["100 CHAR"] or ["100"]
                let length = self.extract_custom_length(params).unwrap_or(255);
                Ok(DataType::String(StringType::Varchar { length }))
            }
            "NVARCHAR2" => {
                let length = self.extract_custom_length(params).unwrap_or(255);
                Ok(DataType::String(StringType::Varchar { length }))
            }
            "RAW" => {
                let length = self.extract_custom_length(params);
                self.convert_raw(length)
            }
            "NUMBER" => {
                // Parse params: ["10"] or ["10","2"] or []
                self.convert_custom_number(params)
            }
            "CLOB" | "NCLOB" => Ok(DataType::String(StringType::Text { max_bytes: None })),
            "BLOB" => Ok(DataType::Blob),
            "LONG" => Ok(DataType::String(StringType::Text { max_bytes: None })),
            "FLOAT" | "BINARY_FLOAT" | "BINARY_DOUBLE" => Ok(DataType::Float),
            _ => Err(ParseError::new(format!(
                "Unsupported Oracle custom type: {}",
                type_name
            ))),
        }
    }

    /// Extract numeric length from custom type params like ["100 CHAR"] or ["100"]
    fn extract_custom_length(&self, params: &[String]) -> Option<u32> {
        params.first().and_then(|p| {
            let num_str: String = p.chars().take_while(|c| c.is_ascii_digit()).collect();
            num_str.parse().ok()
        })
    }

    /// Convert NUMBER from custom params
    fn convert_custom_number(&self, params: &[String]) -> Result<DataType, ParseError> {
        if params.is_empty() {
            return Ok(DataType::Float);
        }

        let first: u8 = params[0].trim().parse().unwrap_or(38);

        if params.len() >= 2 {
            let scale: u8 = params[1].trim().parse().unwrap_or(0);
            if scale > 0 {
                return Ok(DataType::Decimal {
                    precision: first,
                    scale,
                });
            }
        }

        // Integer by precision
        Ok(match first {
            1 => DataType::Boolean,
            2..=3 => DataType::Integer(IntegerType::TinyInt),
            4..=5 => DataType::Integer(IntegerType::SmallInt),
            6..=10 => DataType::Integer(IntegerType::Int),
            _ => DataType::Integer(IntegerType::BigInt),
        })
    }

    /// Extract length from CharacterLength
    fn extract_char_length(&self, len: &Option<sqlparser::ast::CharacterLength>) -> Option<u32> {
        use sqlparser::ast::CharacterLength;

        match len {
            None => None,
            Some(char_len) => match char_len {
                CharacterLength::IntegerLength { length, .. } => Some(*length as u32),
                CharacterLength::Max => Some(u32::MAX),
            },
        }
    }

    /// Convert default value expression (Oracle-specific)
    fn convert_default(&self, expr: &sqlparser::ast::Expr) -> Result<DefaultValue, ParseError> {
        use sqlparser::ast::Expr;
        use sqlparser::ast::Value as SpValue;

        match expr {
            Expr::Value(SpValue::Null) => Ok(DefaultValue::Null),
            Expr::Value(SpValue::Boolean(b)) => Ok(DefaultValue::Boolean(*b)),
            Expr::Value(SpValue::Number(n, _)) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(DefaultValue::Integer(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(DefaultValue::Float(f))
                } else {
                    Ok(DefaultValue::Expression(n.clone()))
                }
            }
            Expr::Value(SpValue::SingleQuotedString(s)) => Ok(DefaultValue::String(s.clone())),
            Expr::Value(SpValue::DoubleQuotedString(s)) => Ok(DefaultValue::String(s.clone())),

            Expr::Function(f) => {
                let name = f.name.to_string().to_uppercase();
                match name.as_str() {
                    "SYSTIMESTAMP" => Ok(DefaultValue::CurrentTimestamp),
                    "SYSDATE" => Ok(DefaultValue::CurrentDate),
                    "SYS_GUID" => Ok(DefaultValue::Uuid),
                    "CURRENT_TIMESTAMP" => Ok(DefaultValue::CurrentTimestamp),
                    "CURRENT_DATE" => Ok(DefaultValue::CurrentDate),
                    _ => Ok(DefaultValue::Expression(expr.to_string())),
                }
            }

            Expr::Identifier(id) => {
                let name = id.value.to_uppercase();
                match name.as_str() {
                    "SYSTIMESTAMP" => Ok(DefaultValue::CurrentTimestamp),
                    "SYSDATE" => Ok(DefaultValue::CurrentDate),
                    "NULL" => Ok(DefaultValue::Null),
                    _ => Ok(DefaultValue::Expression(expr.to_string())),
                }
            }

            Expr::Nested(inner) => self.convert_default(inner),

            _ => Ok(DefaultValue::Expression(expr.to_string())),
        }
    }

    /// Convert a table constraint
    fn convert_constraint(
        &self,
        constraint: sqlparser::ast::TableConstraint,
    ) -> Result<Option<Constraint>, ParseError> {
        use sqlparser::ast::TableConstraint as SpConstraint;

        match constraint {
            SpConstraint::PrimaryKey { name, columns, .. } => {
                let constraint_name = name.map(|n| self.strip_quotes(&n.value));
                let column_names: Vec<String> = columns
                    .iter()
                    .map(|col| self.strip_quotes(&col.value))
                    .collect();

                Ok(Some(Constraint::PrimaryKey {
                    name: constraint_name,
                    columns: column_names,
                }))
            }

            SpConstraint::Unique { name, columns, .. } => {
                let constraint_name = name.map(|n| self.strip_quotes(&n.value));
                let column_names: Vec<String> = columns
                    .iter()
                    .map(|col| self.strip_quotes(&col.value))
                    .collect();

                Ok(Some(Constraint::Unique {
                    name: constraint_name,
                    columns: column_names,
                }))
            }

            SpConstraint::ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                ..
            } => {
                let constraint_name = name.map(|n| self.strip_quotes(&n.value));
                let column_names: Vec<String> = columns
                    .iter()
                    .map(|col| self.strip_quotes(&col.value))
                    .collect();
                let ref_column_names: Vec<String> = referred_columns
                    .iter()
                    .map(|col| self.strip_quotes(&col.value))
                    .collect();

                Ok(Some(Constraint::ForeignKey {
                    name: constraint_name,
                    columns: column_names,
                    ref_table: self.strip_quotes(&foreign_table.to_string()),
                    ref_columns: ref_column_names,
                    on_delete: on_delete.as_ref().map(|a| self.convert_ref_action(a)),
                    on_update: on_update.as_ref().map(|a| self.convert_ref_action(a)),
                }))
            }

            SpConstraint::Check { name, expr, .. } => {
                let constraint_name = name.map(|n| self.strip_quotes(&n.value));
                Ok(Some(Constraint::Check {
                    name: constraint_name,
                    expression: expr.to_string(),
                }))
            }

            SpConstraint::Index { name, columns, .. } => {
                let index_name = match name {
                    Some(n) => self.strip_quotes(&n.value),
                    None => "unnamed_idx".to_string(),
                };
                let index_columns: Vec<IndexColumn> = columns
                    .iter()
                    .map(|col| IndexColumn {
                        name: self.strip_quotes(&col.value),
                        descending: false,
                    })
                    .collect();

                Ok(Some(Constraint::Index {
                    name: index_name,
                    columns: index_columns,
                    unique: false,
                }))
            }

            _ => Ok(None),
        }
    }

    /// Convert value expression (for INSERT)
    fn convert_value(&self, expr: &sqlparser::ast::Expr) -> Result<Value, ParseError> {
        use sqlparser::ast::Expr;
        use sqlparser::ast::Value as SpValue;

        match expr {
            Expr::Value(SpValue::Null) => Ok(Value::Null),
            Expr::Value(SpValue::Boolean(b)) => Ok(Value::Boolean(*b)),
            Expr::Value(SpValue::Number(n, _)) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(Value::Integer(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Value::Float(f))
                } else {
                    Ok(Value::Expression(n.clone()))
                }
            }
            Expr::Value(SpValue::SingleQuotedString(s)) => Ok(Value::String(s.clone())),
            Expr::Value(SpValue::DoubleQuotedString(s)) => Ok(Value::String(s.clone())),
            Expr::Function(f) => {
                let name = f.name.to_string().to_uppercase();
                match name.as_str() {
                    "SYSTIMESTAMP" | "CURRENT_TIMESTAMP" => Ok(Value::CurrentTimestamp),
                    "SYS_GUID" => Ok(Value::Uuid),
                    _ => Ok(Value::Expression(expr.to_string())),
                }
            }
            Expr::Identifier(id) => {
                let name = id.value.to_uppercase();
                match name.as_str() {
                    "SYSTIMESTAMP" => Ok(Value::CurrentTimestamp),
                    "SYSDATE" => Ok(Value::CurrentTimestamp),
                    "NULL" => Ok(Value::Null),
                    _ => Ok(Value::Expression(expr.to_string())),
                }
            }
            _ => Ok(Value::Expression(expr.to_string())),
        }
    }

    /// Extract value rows from a Query
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
            _ => Err(ParseError::new("INSERT source is not a VALUES expression")),
        }
    }

    /// Convert referential action
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

// Preprocessing & helpers
impl OracleParser {
    /// Preprocess Oracle SQL to extract constructs GenericDialect can't parse.
    /// Returns (cleaned_sql, extracted_statements).
    fn preprocess_sql(&self, sql: &str) -> (String, Vec<Statement>) {
        let mut extracted = Vec::new();
        let mut cleaned_lines: Vec<String> = Vec::new();
        let lines: Vec<&str> = sql.lines().collect();
        let mut i = 0;

        while i < lines.len() {
            let trimmed = lines[i].trim();
            let upper = trimmed.to_uppercase();

            // Skip SET DEFINE OFF
            if upper.starts_with("SET DEFINE OFF") || upper.starts_with("SET DEFINE ON") {
                i += 1;
                continue;
            }

            // Skip ALTER SESSION
            if upper.starts_with("ALTER SESSION") {
                if let Some(schema) = self.extract_schema_name(trimmed) {
                    extracted.push(Statement::Use { database: schema });
                }
                i += 1;
                continue;
            }

            // Extract CREATE [OR REPLACE] TRIGGER ... (multi-line PL/SQL block ending with /)
            if upper.starts_with("CREATE TRIGGER") || upper.starts_with("CREATE OR REPLACE TRIGGER")
            {
                let (trigger_stmt, end_idx) = self.extract_trigger_block(&lines, i);
                if let Some(stmt) = trigger_stmt {
                    extracted.push(stmt);
                }
                i = end_idx;
                continue;
            }

            // Extract CREATE SEQUENCE (GenericDialect can't parse Oracle-specific options)
            if upper.starts_with("CREATE SEQUENCE") {
                let full_line = trimmed.trim_end_matches(';');
                if let Ok(stmt) = self.convert_sequence_from_raw(
                    full_line,
                    &self.extract_name_after(full_line, "SEQUENCE"),
                ) {
                    extracted.push(stmt);
                }
                i += 1;
                continue;
            }

            // Extract CREATE [PUBLIC] SYNONYM
            if upper.starts_with("CREATE SYNONYM") || upper.starts_with("CREATE PUBLIC SYNONYM") {
                if let Some(stmt) = self.parse_synonym(trimmed) {
                    extracted.push(stmt);
                }
                i += 1;
                continue;
            }

            // Extract GRANT
            if upper.starts_with("GRANT ") {
                let raw = trimmed.trim_end_matches(';').to_string();
                extracted.push(Statement::Grant { raw_sql: raw });
                i += 1;
                continue;
            }

            // Extract REVOKE
            if upper.starts_with("REVOKE ") {
                let raw = trimmed.trim_end_matches(';').to_string();
                extracted.push(Statement::Revoke { raw_sql: raw });
                i += 1;
                continue;
            }

            // Skip PL/SQL block terminators (standalone /)
            if trimmed == "/" {
                i += 1;
                continue;
            }

            // Skip standalone BEGIN/END blocks (PL/SQL anonymous blocks)
            if upper == "BEGIN" || (upper.starts_with("BEGIN") && !upper.contains("CREATE")) {
                let end_idx = self.skip_plsql_block(&lines, i);
                i = end_idx;
                continue;
            }

            // Keep everything else for sqlparser
            cleaned_lines.push(lines[i].to_string());
            i += 1;
        }

        (cleaned_lines.join("\n"), extracted)
    }

    /// Extract schema name from ALTER SESSION SET CURRENT_SCHEMA = name;
    fn extract_schema_name(&self, line: &str) -> Option<String> {
        let upper = line.to_uppercase();
        if let Some(pos) = upper.find("CURRENT_SCHEMA") {
            let after = &line[pos + "CURRENT_SCHEMA".len()..];
            let after = after.trim_start();
            // Skip '='
            let after = if let Some(stripped) = after.strip_prefix('=') {
                stripped
            } else {
                after
            };
            let name = after.trim().trim_end_matches(';').trim().trim_matches('"');
            if !name.is_empty() {
                return Some(name.to_string());
            }
        }
        None
    }

    /// Extract a PL/SQL trigger block starting at line index `start`.
    /// Returns (Option<Statement>, next_line_index).
    fn extract_trigger_block(&self, lines: &[&str], start: usize) -> (Option<Statement>, usize) {
        let mut body = String::new();
        let mut i = start;

        // Collect lines until we find a standalone '/' terminator
        while i < lines.len() {
            let trimmed = lines[i].trim();
            if trimmed == "/" && i > start {
                break;
            }
            if !body.is_empty() {
                body.push('\n');
            }
            body.push_str(lines[i]);
            i += 1;
        }

        // Move past the '/' terminator
        if i < lines.len() && lines[i].trim() == "/" {
            i += 1;
        }

        // Parse trigger name and table from the body
        let (name, table) = self.parse_trigger_header(&body);

        let stmt = Statement::CreateTrigger {
            name,
            table,
            body: body.clone(),
        };

        (Some(stmt), i)
    }

    /// Parse trigger name and table from CREATE TRIGGER header
    fn parse_trigger_header(&self, body: &str) -> (String, String) {
        let upper = body.to_uppercase();

        // Extract trigger name: after TRIGGER keyword, before next keyword
        let name = if let Some(pos) = upper.find("TRIGGER") {
            let after = &body[pos + 7..]; // len("TRIGGER") == 7
            let name_part = after.split_whitespace().next().unwrap_or("unknown");
            self.strip_quotes(name_part)
        } else {
            "unknown".to_string()
        };

        // Extract table name: after ON keyword
        let table = if let Some(pos) = upper.find(" ON ") {
            let after = &body[pos + 4..]; // len(" ON ") == 4
            let table_part = after.split_whitespace().next().unwrap_or("unknown");
            self.strip_quotes(table_part)
        } else {
            "unknown".to_string()
        };

        (name, table)
    }

    /// Parse CREATE [PUBLIC] SYNONYM line
    fn parse_synonym(&self, line: &str) -> Option<Statement> {
        let upper = line.to_uppercase();
        let is_public = upper.contains("PUBLIC");

        // Pattern: CREATE [PUBLIC] SYNONYM name FOR target;
        let after_synonym = if let Some(pos) = upper.find("SYNONYM") {
            &line[pos + 7..] // len("SYNONYM") == 7
        } else {
            return None;
        };

        let parts: Vec<&str> = after_synonym
            .trim()
            .trim_end_matches(';')
            .splitn(3, char::is_whitespace)
            .filter(|s| !s.is_empty())
            .collect();

        // Expect: [name, "FOR", target]
        if parts.len() >= 3 && parts[1].to_uppercase() == "FOR" {
            Some(Statement::CreateSynonym {
                name: self.strip_quotes(parts[0]),
                target: self.strip_quotes(parts[2]),
                is_public,
            })
        } else {
            None
        }
    }

    /// Skip a PL/SQL anonymous block (BEGIN...END;) and return the next line index
    fn skip_plsql_block(&self, lines: &[&str], start: usize) -> usize {
        let mut i = start;
        let mut depth = 0;

        while i < lines.len() {
            let trimmed = lines[i].trim().to_uppercase();

            if trimmed.starts_with("BEGIN") {
                depth += 1;
            }
            if trimmed.starts_with("END") && trimmed.ends_with(';') {
                depth -= 1;
                if depth <= 0 {
                    i += 1;
                    // Skip trailing '/'
                    if i < lines.len() && lines[i].trim() == "/" {
                        i += 1;
                    }
                    return i;
                }
            }
            if trimmed == "/" && depth <= 0 {
                i += 1;
                return i;
            }

            i += 1;
        }

        i
    }

    /// Extract the first word after a keyword in a SQL statement
    fn extract_name_after(&self, sql: &str, keyword: &str) -> String {
        let upper = sql.to_uppercase();
        if let Some(pos) = upper.find(keyword) {
            let after = &sql[pos + keyword.len()..];
            after
                .split_whitespace()
                .next()
                .unwrap_or("unnamed")
                .trim_end_matches(';')
                .to_string()
        } else {
            "unnamed".to_string()
        }
    }

    /// Strip Oracle double-quotes from identifiers
    fn strip_quotes(&self, name: &str) -> String {
        let trimmed = name.trim();
        if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
            trimmed[1..trimmed.len() - 1].to_string()
        } else {
            trimmed.to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::*;

    // ========== CREATE TABLE ==========

    #[test]
    fn test_parse_simple_create_table() {
        let parser = OracleParser::new();
        let sql = "CREATE TABLE users (id NUMBER(10) NOT NULL, name VARCHAR2(100));";

        let result = parser.parse(sql);
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);

        match &statements[0] {
            Statement::CreateTable(table) => {
                assert_eq!(table.name, "users");
                assert_eq!(table.columns.len(), 2);
                assert_eq!(table.columns[0].name, "id");
                assert!(!table.columns[0].nullable);
                assert_eq!(table.columns[1].name, "name");
            }
            _ => panic!("Expected CreateTable statement"),
        }
    }

    #[test]
    fn test_parse_table_with_primary_key() {
        let parser = OracleParser::new();
        let sql = "CREATE TABLE users (
            id NUMBER(19) NOT NULL,
            CONSTRAINT pk_users PRIMARY KEY (id)
        );";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        let has_pk = table.constraints.iter().any(|c| {
            matches!(c, Constraint::PrimaryKey { name, columns, .. }
                if name.as_deref() == Some("pk_users") && columns == &vec!["id".to_string()])
        });
        assert!(has_pk, "Expected PRIMARY KEY constraint");
    }

    #[test]
    fn test_parse_table_with_foreign_key() {
        let parser = OracleParser::new();
        let sql = "CREATE TABLE orders (
            id NUMBER(19) NOT NULL,
            user_id NUMBER(19),
            CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        let has_fk = table.constraints.iter().any(|c| match c {
            Constraint::ForeignKey {
                name,
                columns,
                ref_table,
                on_delete,
                ..
            } => {
                name.as_deref() == Some("fk_user")
                    && columns == &vec!["user_id".to_string()]
                    && ref_table == "users"
                    && *on_delete == Some(ReferentialAction::Cascade)
            }
            _ => false,
        });
        assert!(
            has_fk,
            "Expected FOREIGN KEY constraint with ON DELETE CASCADE"
        );
    }

    // ========== Data type mapping ==========

    #[test]
    fn test_parse_number_to_boolean() {
        let parser = OracleParser::new();
        let sql = "CREATE TABLE t (active NUMBER(1));";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert!(
            matches!(table.columns[0].data_type, DataType::Boolean),
            "NUMBER(1) should map to Boolean, got {:?}",
            table.columns[0].data_type
        );
    }

    #[test]
    fn test_parse_number_to_tinyint() {
        let parser = OracleParser::new();
        let sql = "CREATE TABLE t (val NUMBER(3));";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert!(
            matches!(
                table.columns[0].data_type,
                DataType::Integer(IntegerType::TinyInt)
            ),
            "NUMBER(3) should map to TinyInt, got {:?}",
            table.columns[0].data_type
        );
    }

    #[test]
    fn test_parse_number_to_smallint() {
        let parser = OracleParser::new();
        let sql = "CREATE TABLE t (val NUMBER(5));";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert!(
            matches!(
                table.columns[0].data_type,
                DataType::Integer(IntegerType::SmallInt)
            ),
            "NUMBER(5) should map to SmallInt, got {:?}",
            table.columns[0].data_type
        );
    }

    #[test]
    fn test_parse_number_to_int() {
        let parser = OracleParser::new();
        let sql = "CREATE TABLE t (val NUMBER(10));";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert!(
            matches!(
                table.columns[0].data_type,
                DataType::Integer(IntegerType::Int)
            ),
            "NUMBER(10) should map to Int, got {:?}",
            table.columns[0].data_type
        );
    }

    #[test]
    fn test_parse_number_to_bigint() {
        let parser = OracleParser::new();
        let sql = "CREATE TABLE t (val NUMBER(19));";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert!(
            matches!(
                table.columns[0].data_type,
                DataType::Integer(IntegerType::BigInt)
            ),
            "NUMBER(19) should map to BigInt, got {:?}",
            table.columns[0].data_type
        );
    }

    #[test]
    fn test_parse_number_decimal() {
        let parser = OracleParser::new();
        let sql = "CREATE TABLE t (price NUMBER(10,2));";

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
            _ => panic!("Expected Decimal, got {:?}", table.columns[0].data_type),
        }
    }

    #[test]
    fn test_parse_number_no_args_is_float() {
        let parser = OracleParser::new();
        let sql = "CREATE TABLE t (val NUMBER);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert!(
            matches!(table.columns[0].data_type, DataType::Float),
            "NUMBER (no args) should map to Float, got {:?}",
            table.columns[0].data_type
        );
    }

    #[test]
    fn test_parse_varchar2() {
        let parser = OracleParser::new();
        let sql = "CREATE TABLE t (name VARCHAR2(100));";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        match &table.columns[0].data_type {
            DataType::String(StringType::Varchar { length }) => {
                assert_eq!(*length, 100);
            }
            _ => panic!("Expected VARCHAR, got {:?}", table.columns[0].data_type),
        }
    }

    #[test]
    fn test_parse_clob() {
        let parser = OracleParser::new();
        let sql = "CREATE TABLE t (content CLOB);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert!(
            matches!(
                table.columns[0].data_type,
                DataType::String(StringType::Text { .. })
            ),
            "CLOB should map to Text, got {:?}",
            table.columns[0].data_type
        );
    }

    #[test]
    fn test_parse_blob() {
        let parser = OracleParser::new();
        let sql = "CREATE TABLE t (data BLOB);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert!(
            matches!(table.columns[0].data_type, DataType::Blob),
            "BLOB should map to Blob, got {:?}",
            table.columns[0].data_type
        );
    }

    #[test]
    fn test_parse_date() {
        let parser = OracleParser::new();
        let sql = "CREATE TABLE t (created DATE);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert!(
            matches!(
                table.columns[0].data_type,
                DataType::Temporal(TemporalType::Date)
            ),
            "DATE should map to Date, got {:?}",
            table.columns[0].data_type
        );
    }

    #[test]
    fn test_parse_timestamp() {
        let parser = OracleParser::new();
        let sql = "CREATE TABLE t (created_at TIMESTAMP);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert!(
            matches!(
                table.columns[0].data_type,
                DataType::Temporal(TemporalType::Timestamp {
                    with_timezone: false,
                    ..
                })
            ),
            "TIMESTAMP should map to Timestamp, got {:?}",
            table.columns[0].data_type
        );
    }

    #[test]
    fn test_parse_timestamp_with_timezone() {
        let parser = OracleParser::new();
        let sql = "CREATE TABLE t (created_at TIMESTAMP WITH TIME ZONE);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert!(
            matches!(
                table.columns[0].data_type,
                DataType::Temporal(TemporalType::Timestamp {
                    with_timezone: true,
                    ..
                })
            ),
            "TIMESTAMP WITH TIME ZONE should map to Timestamp with tz, got {:?}",
            table.columns[0].data_type
        );
    }

    // ========== Default values ==========

    #[test]
    fn test_parse_default_null() {
        let parser = OracleParser::new();
        let sql = "CREATE TABLE t (name VARCHAR2(100) DEFAULT NULL);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert_eq!(table.columns[0].default, Some(DefaultValue::Null));
    }

    #[test]
    fn test_parse_default_string() {
        let parser = OracleParser::new();
        let sql = "CREATE TABLE t (status VARCHAR2(20) DEFAULT 'active');";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert_eq!(
            table.columns[0].default,
            Some(DefaultValue::String("active".to_string()))
        );
    }

    #[test]
    fn test_parse_default_number() {
        let parser = OracleParser::new();
        let sql = "CREATE TABLE t (count NUMBER(10) DEFAULT 0);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert_eq!(table.columns[0].default, Some(DefaultValue::Integer(0)));
    }

    #[test]
    fn test_parse_default_systimestamp() {
        let parser = OracleParser::new();
        let sql = "CREATE TABLE t (created_at TIMESTAMP DEFAULT SYSTIMESTAMP);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert_eq!(
            table.columns[0].default,
            Some(DefaultValue::CurrentTimestamp)
        );
    }

    #[test]
    fn test_parse_default_sysdate() {
        let parser = OracleParser::new();
        let sql = "CREATE TABLE t (created DATE DEFAULT SYSDATE);";

        let result = parser.parse(sql).unwrap();
        let table = match &result[0] {
            Statement::CreateTable(t) => t,
            _ => panic!("Expected CreateTable"),
        };

        assert_eq!(table.columns[0].default, Some(DefaultValue::CurrentDate));
    }

    // ========== Preprocessing ==========

    #[test]
    fn test_preprocess_alter_session() {
        let parser = OracleParser::new();
        let sql = "ALTER SESSION SET CURRENT_SCHEMA = myschema;\nCREATE TABLE t (id NUMBER(10));";

        let result = parser.parse(sql).unwrap();

        // Should have both: CreateTable and Use
        let has_table = result
            .iter()
            .any(|s| matches!(s, Statement::CreateTable(_)));
        let has_use = result.iter().any(|s| matches!(s, Statement::Use { .. }));

        assert!(has_table, "Should have CreateTable");
        assert!(has_use, "Should have Use statement from ALTER SESSION");
    }

    #[test]
    fn test_preprocess_set_define_off() {
        let parser = OracleParser::new();
        let sql = "SET DEFINE OFF;\nCREATE TABLE t (id NUMBER(10));";

        let result = parser.parse(sql).unwrap();

        // SET DEFINE OFF should be silently stripped
        let has_table = result
            .iter()
            .any(|s| matches!(s, Statement::CreateTable(_)));
        assert!(
            has_table,
            "Should have CreateTable after stripping SET DEFINE OFF"
        );
    }

    #[test]
    fn test_preprocess_trigger() {
        let parser = OracleParser::new();
        let sql = "CREATE OR REPLACE TRIGGER trg_test\n\
                    BEFORE INSERT ON test_table\n\
                    FOR EACH ROW\n\
                    BEGIN\n\
                      :NEW.id := test_seq.NEXTVAL;\n\
                    END;\n\
                    /";

        let result = parser.parse(sql).unwrap();

        let trigger = result
            .iter()
            .find(|s| matches!(s, Statement::CreateTrigger { .. }));
        assert!(trigger.is_some(), "Should have CreateTrigger");

        match trigger.unwrap() {
            Statement::CreateTrigger { name, table, .. } => {
                assert_eq!(name, "trg_test");
                assert_eq!(table, "test_table");
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_preprocess_synonym() {
        let parser = OracleParser::new();
        let sql = "CREATE PUBLIC SYNONYM my_syn FOR schema1.my_table;";

        let result = parser.parse(sql).unwrap();

        let synonym = result
            .iter()
            .find(|s| matches!(s, Statement::CreateSynonym { .. }));
        assert!(synonym.is_some(), "Should have CreateSynonym");

        match synonym.unwrap() {
            Statement::CreateSynonym {
                name,
                target,
                is_public,
            } => {
                assert_eq!(name, "my_syn");
                assert_eq!(target, "schema1.my_table");
                assert!(is_public);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_preprocess_grant() {
        let parser = OracleParser::new();
        let sql = "GRANT SELECT ON my_table TO my_user;";

        let result = parser.parse(sql).unwrap();

        let grant = result.iter().find(|s| matches!(s, Statement::Grant { .. }));
        assert!(grant.is_some(), "Should have Grant statement");
    }

    // ========== DROP TABLE ==========

    #[test]
    fn test_parse_drop_table() {
        let parser = OracleParser::new();
        let sql = "DROP TABLE users;";

        let result = parser.parse(sql).unwrap();

        match &result[0] {
            Statement::DropTable {
                name,
                if_exists,
                cascade,
            } => {
                assert_eq!(name, "users");
                assert!(!if_exists);
                assert!(!cascade);
            }
            _ => panic!("Expected DropTable statement"),
        }
    }

    #[test]
    fn test_parse_drop_table_cascade() {
        let parser = OracleParser::new();
        let sql = "DROP TABLE users CASCADE;";

        let result = parser.parse(sql).unwrap();

        match &result[0] {
            Statement::DropTable { name, cascade, .. } => {
                assert_eq!(name, "users");
                assert!(cascade);
            }
            _ => panic!("Expected DropTable statement"),
        }
    }

    // ========== INSERT ==========

    #[test]
    fn test_parse_insert() {
        let parser = OracleParser::new();
        let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice');";

        let result = parser.parse(sql).unwrap();

        match &result[0] {
            Statement::Insert {
                table,
                columns,
                values,
            } => {
                assert_eq!(table, "users");
                assert_eq!(columns, &Some(vec!["id".to_string(), "name".to_string()]));
                assert_eq!(values.len(), 1);
                assert_eq!(values[0].len(), 2);
            }
            _ => panic!("Expected Insert statement"),
        }
    }

    // ========== CREATE INDEX ==========

    #[test]
    fn test_parse_create_index() {
        let parser = OracleParser::new();
        let sql = "CREATE INDEX idx_user_email ON users (email);";

        let result = parser.parse(sql).unwrap();

        match &result[0] {
            Statement::CreateIndex {
                name,
                table,
                columns,
                unique,
            } => {
                assert_eq!(name, "idx_user_email");
                assert_eq!(table, "users");
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].name, "email");
                assert!(!unique);
            }
            _ => panic!("Expected CreateIndex statement"),
        }
    }

    #[test]
    fn test_parse_create_unique_index() {
        let parser = OracleParser::new();
        let sql = "CREATE UNIQUE INDEX uq_email ON users (email);";

        let result = parser.parse(sql).unwrap();

        match &result[0] {
            Statement::CreateIndex { unique, .. } => {
                assert!(unique);
            }
            _ => panic!("Expected CreateIndex statement"),
        }
    }

    // ========== Double-quote stripping ==========

    #[test]
    fn test_strip_double_quotes() {
        let parser = OracleParser::new();
        assert_eq!(parser.strip_quotes("\"my_table\""), "my_table");
        assert_eq!(parser.strip_quotes("my_table"), "my_table");
        assert_eq!(parser.strip_quotes("\"COMMENT\""), "COMMENT");
    }
}

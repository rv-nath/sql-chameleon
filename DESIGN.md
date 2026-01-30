# SQL Transpiler Design Document

A generic, dialect-agnostic SQL transpiler that converts SQL between database dialects (MySQL, Oracle, PostgreSQL, SQLite, SQL Server) using AST-based transformation with `sqlparser-rs`.

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Dialect-Neutral AST](#dialect-neutral-ast)
4. [Parser and Emitter Traits](#parser-and-emitter-traits)
5. [Type Mapping Matrix](#type-mapping-matrix)
6. [Dialect Implementations](#dialect-implementations)
7. [Main Transpiler API](#main-transpiler-api)
8. [Adding New Dialects](#adding-new-dialects)
9. [Project Structure](#project-structure)

---

## Overview

### Problem

Converting SQL between dialects using regex fails because:

- SQL has nested structures (subqueries, expressions)
- Context matters (keywords inside strings shouldn't transform)
- Edge cases multiply exponentially

### Solution

```
Source SQL  →  Parse  →  Neutral AST  →  Emit  →  Target SQL
   (MySQL)      │           │              │        (Oracle)
                │           │              │
           sqlparser    Transform      Generate
              -rs       to neutral    dialect SQL
```

### Dependencies

```toml
[dependencies]
sqlparser = "0.52"
```

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        SQL Transpiler                            │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐            │
│  │ MySQL       │   │ Oracle      │   │ PostgreSQL  │   Parsers  │
│  │ Parser      │   │ Parser      │   │ Parser      │   (input)  │
│  └──────┬──────┘   └──────┬──────┘   └──────┬──────┘            │
│         │                 │                 │                    │
│         └────────────────┬┴─────────────────┘                    │
│                          ▼                                       │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                 Dialect-Neutral AST                      │    │
│  │                                                         │    │
│  │  • Generic data types (Integer, String, Timestamp...)   │    │
│  │  • Normalized constraints                               │    │
│  │  • Normalized default values                            │    │
│  │  • No dialect-specific syntax                           │    │
│  └─────────────────────────────────────────────────────────┘    │
│                          │                                       │
│         ┌────────────────┼──────────────────┐                    │
│         ▼                ▼                  ▼                    │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐            │
│  │ MySQL       │   │ Oracle      │   │ PostgreSQL  │   Emitters │
│  │ Emitter     │   │ Emitter     │   │ Emitter     │   (output) │
│  └─────────────┘   └─────────────┘   └─────────────┘            │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

---

## Dialect-Neutral AST

The AST captures **semantics** (what the SQL means), not **syntax** (how it's written).

### Data Types

```rust
/// Generic integer types - normalized across dialects
#[derive(Debug, Clone, PartialEq)]
pub enum IntegerType {
    TinyInt,    // ~1 byte
    SmallInt,   // ~2 bytes
    Int,        // ~4 bytes
    BigInt,     // ~8 bytes
}

/// Generic string types
#[derive(Debug, Clone, PartialEq)]
pub enum StringType {
    Char { length: u32 },
    Varchar { length: u32 },
    Text,  // Unbounded text
}

/// Generic temporal types
#[derive(Debug, Clone, PartialEq)]
pub enum TemporalType {
    Date,
    Time { precision: Option<u8> },
    Timestamp { precision: Option<u8>, with_timezone: bool },
}

/// Unified data type enum
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    // Numeric
    Integer(IntegerType),
    Decimal { precision: u8, scale: u8 },
    Float,
    Boolean,

    // String
    String(StringType),

    // Binary
    Binary { length: Option<u32> },
    Blob,

    // Temporal
    Temporal(TemporalType),

    // Structured
    Json,

    // Special
    Enum { values: Vec<String> },
    Uuid,
}
```

### Default Values

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum DefaultValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
    CurrentTimestamp,
    CurrentDate,
    Uuid,
    Expression(String),  // Fallback for complex expressions
}
```

### Column Definition

```rust
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default: Option<DefaultValue>,
    pub auto_increment: bool,
    pub on_update_timestamp: bool,
    pub comment: Option<String>,
}
```

### Constraints

```rust
#[derive(Debug, Clone)]
pub enum Constraint {
    PrimaryKey {
        name: Option<String>,
        columns: Vec<String>,
    },
    Unique {
        name: Option<String>,
        columns: Vec<String>,
    },
    ForeignKey {
        name: Option<String>,
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
    Check {
        name: Option<String>,
        expression: String,
    },
    Index {
        name: String,
        columns: Vec<IndexColumn>,
        unique: bool,
    },
}

#[derive(Debug, Clone)]
pub enum ReferentialAction {
    Cascade,
    SetNull,
    SetDefault,
    Restrict,
    NoAction,
}

#[derive(Debug, Clone)]
pub struct IndexColumn {
    pub name: String,
    pub descending: bool,
}
```

### Table Definition

```rust
#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub constraints: Vec<Constraint>,
    pub comment: Option<String>,
}
```

### Statements

```rust
#[derive(Debug, Clone)]
pub enum Statement {
    CreateTable(Table),
    DropTable {
        name: String,
        if_exists: bool,
        cascade: bool,
    },
    AlterTable {
        name: String,
        operations: Vec<AlterOperation>,
    },
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Value>>,
    },
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<IndexColumn>,
        unique: bool,
    },
}

#[derive(Debug, Clone)]
pub enum AlterOperation {
    AddColumn(Column),
    DropColumn { name: String },
    ModifyColumn(Column),
    RenameColumn { old_name: String, new_name: String },
    AddConstraint(Constraint),
    DropConstraint { name: String },
}

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
    CurrentTimestamp,
    Uuid,
    Expression(String),
}
```

---

## Parser and Emitter Traits

### Dialect Enum

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Dialect {
    MySQL,
    Oracle,
    PostgreSQL,
    SQLite,
    SQLServer,
}
```

### Parser Trait

```rust
pub trait SqlParser: Send + Sync {
    /// Parse SQL string into dialect-neutral AST
    fn parse(&self, sql: &str) -> Result<Vec<Statement>, ParseError>;

    /// What dialect does this parser handle?
    fn dialect(&self) -> Dialect;
}
```

### Emitter Trait

```rust
pub trait SqlEmitter: Send + Sync {
    /// Convert dialect-neutral AST to SQL string
    fn emit(&self, statements: &[Statement]) -> Result<String, EmitError>;

    /// Emit a single statement
    fn emit_statement(&self, stmt: &Statement) -> Result<String, EmitError>;

    /// What dialect does this emitter produce?
    fn dialect(&self) -> Dialect;

    /// Get supplementary statements (triggers, sequences, etc.)
    fn supplementary_statements(&self) -> Vec<String>;

    /// Reset internal state (call between conversions)
    fn reset(&self);
}
```

---

## Type Mapping Matrix

### Data Types

| Neutral Type | MySQL | Oracle | PostgreSQL | SQLite | SQL Server |
|--------------|-------|--------|------------|--------|------------|
| TinyInt | `TINYINT` | `NUMBER(3)` | `SMALLINT` | `INTEGER` | `TINYINT` |
| SmallInt | `SMALLINT` | `NUMBER(5)` | `SMALLINT` | `INTEGER` | `SMALLINT` |
| Int | `INT` | `NUMBER(10)` | `INTEGER` | `INTEGER` | `INT` |
| BigInt | `BIGINT` | `NUMBER(19)` | `BIGINT` | `INTEGER` | `BIGINT` |
| Decimal(p,s) | `DECIMAL(p,s)` | `NUMBER(p,s)` | `DECIMAL(p,s)` | `REAL` | `DECIMAL(p,s)` |
| Float | `DOUBLE` | `NUMBER` | `DOUBLE PRECISION` | `REAL` | `FLOAT` |
| Boolean | `TINYINT(1)` | `NUMBER(1)` | `BOOLEAN` | `INTEGER` | `BIT` |
| Char(n) | `CHAR(n)` | `CHAR(n CHAR)` | `CHAR(n)` | `TEXT` | `CHAR(n)` |
| Varchar(n) | `VARCHAR(n)` | `VARCHAR2(n CHAR)` | `VARCHAR(n)` | `TEXT` | `VARCHAR(n)` |
| Text | `LONGTEXT` | `CLOB` | `TEXT` | `TEXT` | `VARCHAR(MAX)` |
| Binary(n) | `VARBINARY(n)` | `RAW(n)` | `BYTEA` | `BLOB` | `VARBINARY(n)` |
| Blob | `LONGBLOB` | `BLOB` | `BYTEA` | `BLOB` | `VARBINARY(MAX)` |
| Date | `DATE` | `DATE` | `DATE` | `TEXT` | `DATE` |
| Time | `TIME` | `TIMESTAMP` | `TIME` | `TEXT` | `TIME` |
| Timestamp | `DATETIME` | `TIMESTAMP` | `TIMESTAMP` | `TEXT` | `DATETIME2` |
| Timestamp+TZ | `DATETIME` | `TIMESTAMP WITH TIME ZONE` | `TIMESTAMPTZ` | `TEXT` | `DATETIMEOFFSET` |
| Json | `JSON` | `CLOB` | `JSONB` | `TEXT` | `NVARCHAR(MAX)` |
| Uuid | `CHAR(36)` | `RAW(16)` | `UUID` | `TEXT` | `UNIQUEIDENTIFIER` |
| Enum | `ENUM(...)` | `VARCHAR2 + CHECK` | `CREATE TYPE` | `TEXT + CHECK` | `VARCHAR + CHECK` |

### Default Value Functions

| Neutral | MySQL | Oracle | PostgreSQL | SQLite | SQL Server |
|---------|-------|--------|------------|--------|------------|
| CurrentTimestamp | `CURRENT_TIMESTAMP` | `SYSTIMESTAMP` | `CURRENT_TIMESTAMP` | `CURRENT_TIMESTAMP` | `GETDATE()` |
| CurrentDate | `CURRENT_DATE` | `SYSDATE` | `CURRENT_DATE` | `DATE('now')` | `GETDATE()` |
| Uuid | `UUID()` | `SYS_GUID()` | `gen_random_uuid()` | N/A | `NEWID()` |

### Auto-Increment Implementation

| Dialect | Implementation |
|---------|----------------|
| MySQL | `AUTO_INCREMENT` keyword |
| Oracle | `SEQUENCE` + `TRIGGER` (or `IDENTITY` in 12c+) |
| PostgreSQL | `SERIAL` / `BIGSERIAL` or `IDENTITY` |
| SQLite | `INTEGER PRIMARY KEY` (implicit) |
| SQL Server | `IDENTITY(1,1)` |

### Features Not Supported in All Dialects

| Feature | MySQL | Oracle | PostgreSQL | Notes |
|---------|-------|--------|------------|-------|
| `IF NOT EXISTS` | ✓ | ✗ | ✓ | Oracle needs PL/SQL block |
| `ON UPDATE CASCADE` (FK) | ✓ | ✗ | ✓ | Oracle needs trigger |
| `ON UPDATE CURRENT_TIMESTAMP` | ✓ | ✗ | ✗ | Oracle/PG need trigger |
| `ENUM` type | ✓ (native) | ✗ | ✓ (custom type) | Oracle uses CHECK constraint |
| `DROP TABLE IF EXISTS` | ✓ | ✗ | ✓ | Oracle needs PL/SQL block |
| Table comments | `COMMENT=` | `COMMENT ON` | `COMMENT ON` | Different syntax |
| Column comments | Inline | `COMMENT ON` | `COMMENT ON` | Different syntax |

---

## Dialect Implementations

### MySQL Parser

```rust
pub struct MySqlParser;

impl SqlParser for MySqlParser {
    fn dialect(&self) -> Dialect {
        Dialect::MySQL
    }

    fn parse(&self, sql: &str) -> Result<Vec<Statement>, ParseError> {
        let dialect = sqlparser::dialect::MySqlDialect {};
        let statements = sqlparser::parser::Parser::parse_sql(&dialect, sql)?;

        statements
            .into_iter()
            .filter_map(|stmt| self.convert_statement(stmt))
            .collect()
    }
}

impl MySqlParser {
    fn convert_data_type(&self, dt: sqlparser::ast::DataType) -> DataType {
        match dt {
            sqlparser::ast::DataType::BigInt(_) => DataType::Integer(IntegerType::BigInt),
            sqlparser::ast::DataType::Int(_) => DataType::Integer(IntegerType::Int),
            sqlparser::ast::DataType::SmallInt(_) => DataType::Integer(IntegerType::SmallInt),
            sqlparser::ast::DataType::TinyInt(_) => DataType::Integer(IntegerType::TinyInt),
            sqlparser::ast::DataType::Varchar(len) => {
                let length = extract_char_length(len).unwrap_or(255);
                DataType::String(StringType::Varchar { length })
            }
            sqlparser::ast::DataType::Text
            | sqlparser::ast::DataType::LongText
            | sqlparser::ast::DataType::MediumText => DataType::String(StringType::Text),
            sqlparser::ast::DataType::Datetime(prec) => {
                DataType::Temporal(TemporalType::Timestamp {
                    precision: prec.map(|p| p as u8),
                    with_timezone: false,
                })
            }
            sqlparser::ast::DataType::JSON => DataType::Json,
            sqlparser::ast::DataType::Boolean => DataType::Boolean,
            sqlparser::ast::DataType::Enum(values, _) => DataType::Enum { values },
            _ => todo!("Handle MySQL type: {:?}", dt),
        }
    }

    fn convert_default(&self, expr: &sqlparser::ast::Expr) -> DefaultValue {
        match expr {
            sqlparser::ast::Expr::Value(Value::Number(n, _)) => {
                n.parse().map(DefaultValue::Integer).unwrap_or_else(|_|
                    n.parse().map(DefaultValue::Float).unwrap_or_else(|_|
                        DefaultValue::Expression(n.clone())
                    )
                )
            }
            sqlparser::ast::Expr::Value(Value::SingleQuotedString(s)) => {
                DefaultValue::String(s.clone())
            }
            sqlparser::ast::Expr::Value(Value::Boolean(b)) => DefaultValue::Boolean(*b),
            sqlparser::ast::Expr::Value(Value::Null) => DefaultValue::Null,
            sqlparser::ast::Expr::Function(f)
                if f.name.to_string().eq_ignore_ascii_case("NOW") =>
            {
                DefaultValue::CurrentTimestamp
            }
            sqlparser::ast::Expr::Function(f)
                if f.name.to_string().eq_ignore_ascii_case("UUID") =>
            {
                DefaultValue::Uuid
            }
            sqlparser::ast::Expr::Identifier(id)
                if id.value.eq_ignore_ascii_case("CURRENT_TIMESTAMP") =>
            {
                DefaultValue::CurrentTimestamp
            }
            _ => DefaultValue::Expression(expr.to_string()),
        }
    }
}
```

### Oracle Parser

```rust
pub struct OracleParser;

impl SqlParser for OracleParser {
    fn dialect(&self) -> Dialect {
        Dialect::Oracle
    }

    fn parse(&self, sql: &str) -> Result<Vec<Statement>, ParseError> {
        // sqlparser doesn't have Oracle-specific dialect, use Generic
        let dialect = sqlparser::dialect::GenericDialect {};
        let statements = sqlparser::parser::Parser::parse_sql(&dialect, sql)?;

        statements
            .into_iter()
            .filter_map(|stmt| self.convert_statement(stmt))
            .collect()
    }
}

impl OracleParser {
    fn convert_data_type(&self, dt: sqlparser::ast::DataType) -> DataType {
        match dt {
            // NUMBER(p,s) - infer type from precision
            sqlparser::ast::DataType::Custom(name, args)
                if name.to_string().eq_ignore_ascii_case("NUMBER") =>
            {
                self.parse_oracle_number(&args)
            }
            // VARCHAR2(n CHAR)
            sqlparser::ast::DataType::Custom(name, args)
                if name.to_string().eq_ignore_ascii_case("VARCHAR2") =>
            {
                let length = args
                    .first()
                    .and_then(|s| s.trim_end_matches(" CHAR").parse().ok())
                    .unwrap_or(255);
                DataType::String(StringType::Varchar { length })
            }
            sqlparser::ast::DataType::Clob(_) => DataType::String(StringType::Text),
            sqlparser::ast::DataType::Blob(_) => DataType::Blob,
            sqlparser::ast::DataType::Timestamp(prec, tz) => {
                DataType::Temporal(TemporalType::Timestamp {
                    precision: prec.map(|p| p as u8),
                    with_timezone: matches!(tz, sqlparser::ast::TimezoneInfo::WithTimeZone),
                })
            }
            _ => todo!("Handle Oracle type: {:?}", dt),
        }
    }

    fn parse_oracle_number(&self, args: &[String]) -> DataType {
        match args.as_slice() {
            [] => DataType::Float,
            [p] => {
                let precision: u8 = p.parse().unwrap_or(10);
                match precision {
                    1 => DataType::Boolean,
                    2..=3 => DataType::Integer(IntegerType::TinyInt),
                    4..=5 => DataType::Integer(IntegerType::SmallInt),
                    6..=10 => DataType::Integer(IntegerType::Int),
                    _ => DataType::Integer(IntegerType::BigInt),
                }
            }
            [p, s] => {
                let precision: u8 = p.parse().unwrap_or(10);
                let scale: u8 = s.parse().unwrap_or(0);
                if scale == 0 {
                    match precision {
                        1 => DataType::Boolean,
                        2..=3 => DataType::Integer(IntegerType::TinyInt),
                        4..=5 => DataType::Integer(IntegerType::SmallInt),
                        6..=10 => DataType::Integer(IntegerType::Int),
                        _ => DataType::Integer(IntegerType::BigInt),
                    }
                } else {
                    DataType::Decimal { precision, scale }
                }
            }
            _ => DataType::Float,
        }
    }

    fn convert_default(&self, expr: &sqlparser::ast::Expr) -> DefaultValue {
        match expr {
            sqlparser::ast::Expr::Identifier(id)
                if id.value.eq_ignore_ascii_case("SYSTIMESTAMP") =>
            {
                DefaultValue::CurrentTimestamp
            }
            sqlparser::ast::Expr::Identifier(id)
                if id.value.eq_ignore_ascii_case("SYSDATE") =>
            {
                DefaultValue::CurrentDate
            }
            sqlparser::ast::Expr::Function(f)
                if f.name.to_string().eq_ignore_ascii_case("SYS_GUID") =>
            {
                DefaultValue::Uuid
            }
            _ => DefaultValue::Expression(expr.to_string()),
        }
    }
}
```

### MySQL Emitter

```rust
pub struct MySqlEmitter;

impl SqlEmitter for MySqlEmitter {
    fn dialect(&self) -> Dialect {
        Dialect::MySQL
    }

    fn emit_statement(&self, stmt: &Statement) -> Result<String, EmitError> {
        match stmt {
            Statement::CreateTable(table) => self.emit_create_table(table),
            Statement::DropTable { name, if_exists, cascade } => {
                self.emit_drop_table(name, *if_exists, *cascade)
            }
            Statement::Insert { table, columns, values } => {
                self.emit_insert(table, columns, values)
            }
            _ => todo!(),
        }
    }

    fn supplementary_statements(&self) -> Vec<String> {
        vec![]  // MySQL handles AUTO_INCREMENT natively
    }

    fn reset(&self) {}
}

impl MySqlEmitter {
    fn emit_data_type(&self, dt: &DataType) -> String {
        match dt {
            DataType::Integer(IntegerType::TinyInt) => "TINYINT".into(),
            DataType::Integer(IntegerType::SmallInt) => "SMALLINT".into(),
            DataType::Integer(IntegerType::Int) => "INT".into(),
            DataType::Integer(IntegerType::BigInt) => "BIGINT".into(),
            DataType::Decimal { precision, scale } => format!("DECIMAL({},{})", precision, scale),
            DataType::Float => "DOUBLE".into(),
            DataType::Boolean => "TINYINT(1)".into(),
            DataType::String(StringType::Char { length }) => format!("CHAR({})", length),
            DataType::String(StringType::Varchar { length }) => format!("VARCHAR({})", length),
            DataType::String(StringType::Text) => "LONGTEXT".into(),
            DataType::Binary { length: Some(l) } => format!("VARBINARY({})", l),
            DataType::Binary { length: None } | DataType::Blob => "LONGBLOB".into(),
            DataType::Temporal(TemporalType::Date) => "DATE".into(),
            DataType::Temporal(TemporalType::Time { .. }) => "TIME".into(),
            DataType::Temporal(TemporalType::Timestamp { precision, .. }) => {
                precision.map_or("DATETIME".into(), |p| format!("DATETIME({})", p))
            }
            DataType::Json => "JSON".into(),
            DataType::Uuid => "CHAR(36)".into(),
            DataType::Enum { values } => {
                let quoted: Vec<_> = values.iter().map(|v| format!("'{}'", v)).collect();
                format!("ENUM({})", quoted.join(", "))
            }
        }
    }

    fn emit_default(&self, default: &DefaultValue) -> String {
        match default {
            DefaultValue::Null => "NULL".into(),
            DefaultValue::Boolean(true) => "TRUE".into(),
            DefaultValue::Boolean(false) => "FALSE".into(),
            DefaultValue::Integer(i) => i.to_string(),
            DefaultValue::Float(f) => f.to_string(),
            DefaultValue::String(s) => format!("'{}'", s.replace('\'', "''")),
            DefaultValue::CurrentTimestamp => "CURRENT_TIMESTAMP".into(),
            DefaultValue::CurrentDate => "CURRENT_DATE".into(),
            DefaultValue::Uuid => "(UUID())".into(),
            DefaultValue::Expression(e) => e.clone(),
        }
    }
}
```

### Oracle Emitter

```rust
pub struct OracleEmitter {
    auto_increment_cols: RefCell<Vec<(String, String)>>,
    on_update_cols: RefCell<Vec<(String, String)>>,
}

impl SqlEmitter for OracleEmitter {
    fn dialect(&self) -> Dialect {
        Dialect::Oracle
    }

    fn emit_statement(&self, stmt: &Statement) -> Result<String, EmitError> {
        match stmt {
            Statement::CreateTable(table) => self.emit_create_table(table),
            Statement::DropTable { name, if_exists, cascade } => {
                self.emit_drop_table(name, *if_exists, *cascade)
            }
            _ => todo!(),
        }
    }

    fn supplementary_statements(&self) -> Vec<String> {
        let mut stmts = Vec::new();

        // Sequences and triggers for AUTO_INCREMENT
        for (table, column) in self.auto_increment_cols.borrow().iter() {
            stmts.push(format!(
                "CREATE SEQUENCE {}_seq START WITH 1 INCREMENT BY 1 NOCACHE;",
                table
            ));
            stmts.push(format!(
                r#"CREATE OR REPLACE TRIGGER trg_{table}_ai
BEFORE INSERT ON {table}
FOR EACH ROW
WHEN (NEW.{column} IS NULL)
BEGIN
  :NEW.{column} := {table}_seq.NEXTVAL;
END;
/"#
            ));
        }

        // Triggers for ON UPDATE CURRENT_TIMESTAMP
        for (table, column) in self.on_update_cols.borrow().iter() {
            stmts.push(format!(
                r#"CREATE OR REPLACE TRIGGER trg_{table}_upd
BEFORE UPDATE ON {table}
FOR EACH ROW
BEGIN
  :NEW.{column} := SYSTIMESTAMP;
END;
/"#
            ));
        }

        stmts
    }

    fn reset(&self) {
        self.auto_increment_cols.borrow_mut().clear();
        self.on_update_cols.borrow_mut().clear();
    }
}

impl OracleEmitter {
    fn emit_data_type(&self, dt: &DataType) -> String {
        match dt {
            DataType::Integer(IntegerType::TinyInt) => "NUMBER(3)".into(),
            DataType::Integer(IntegerType::SmallInt) => "NUMBER(5)".into(),
            DataType::Integer(IntegerType::Int) => "NUMBER(10)".into(),
            DataType::Integer(IntegerType::BigInt) => "NUMBER(19)".into(),
            DataType::Decimal { precision, scale } => format!("NUMBER({},{})", precision, scale),
            DataType::Float => "NUMBER".into(),
            DataType::Boolean => "NUMBER(1)".into(),
            DataType::String(StringType::Char { length }) => format!("CHAR({} CHAR)", length),
            DataType::String(StringType::Varchar { length }) => format!("VARCHAR2({} CHAR)", length),
            DataType::String(StringType::Text) => "CLOB".into(),
            DataType::Binary { length: Some(l) } => format!("RAW({})", l),
            DataType::Binary { length: None } | DataType::Blob => "BLOB".into(),
            DataType::Temporal(TemporalType::Date) => "DATE".into(),
            DataType::Temporal(TemporalType::Time { precision }) => {
                precision.map_or("TIMESTAMP".into(), |p| format!("TIMESTAMP({})", p))
            }
            DataType::Temporal(TemporalType::Timestamp { precision, with_timezone }) => {
                let base = precision.map_or("TIMESTAMP".into(), |p| format!("TIMESTAMP({})", p));
                if *with_timezone {
                    format!("{} WITH TIME ZONE", base)
                } else {
                    base
                }
            }
            DataType::Json => "CLOB".into(),
            DataType::Uuid => "RAW(16)".into(),
            DataType::Enum { values } => {
                let max_len = values.iter().map(|v| v.len()).max().unwrap_or(10);
                format!("VARCHAR2({})", max_len.max(10))
            }
        }
    }

    fn emit_default(&self, default: &DefaultValue) -> String {
        match default {
            DefaultValue::Null => "NULL".into(),
            DefaultValue::Boolean(true) => "1".into(),
            DefaultValue::Boolean(false) => "0".into(),
            DefaultValue::Integer(i) => i.to_string(),
            DefaultValue::Float(f) => f.to_string(),
            DefaultValue::String(s) => format!("'{}'", s.replace('\'', "''")),
            DefaultValue::CurrentTimestamp => "SYSTIMESTAMP".into(),
            DefaultValue::CurrentDate => "SYSDATE".into(),
            DefaultValue::Uuid => "SYS_GUID()".into(),
            DefaultValue::Expression(e) => e.clone(),
        }
    }

    fn emit_drop_table(&self, name: &str, if_exists: bool, cascade: bool) -> Result<String, EmitError> {
        let cascade_str = if cascade { " CASCADE CONSTRAINTS" } else { "" };

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

    fn emit_column(&self, col: &Column, table_name: &str) -> String {
        let mut parts = vec![
            Self::quote_if_reserved(&col.name),
            self.emit_data_type(&col.data_type),
        ];

        // Track auto-increment columns for trigger generation
        if col.auto_increment {
            self.auto_increment_cols
                .borrow_mut()
                .push((table_name.to_string(), col.name.clone()));
        }

        // Track on-update timestamp columns for trigger generation
        if col.on_update_timestamp {
            self.on_update_cols
                .borrow_mut()
                .push((table_name.to_string(), col.name.clone()));
        }

        // DEFAULT before NOT NULL (Oracle convention)
        if let Some(default) = &col.default {
            parts.push(format!("DEFAULT {}", self.emit_default(default)));
        }

        if !col.nullable {
            parts.push("NOT NULL".into());
        }

        parts.join(" ")
    }

    fn quote_if_reserved(name: &str) -> String {
        const RESERVED: &[&str] = &[
            "DATE", "NUMBER", "LEVEL", "SIZE", "TYPE", "USER", "UID", "ROW",
            "ROWID", "ROWNUM", "COMMENT", "START", "END", "FILE", "MODE",
            "RESOURCE", "INDEX", "TABLE", "VIEW", "ORDER", "GROUP",
        ];

        if RESERVED.contains(&name.to_uppercase().as_str()) {
            format!("\"{}\"", name)
        } else {
            name.to_string()
        }
    }
}
```

---

## Main Transpiler API

```rust
use std::collections::HashMap;

pub struct SqlTranspiler {
    parsers: HashMap<Dialect, Box<dyn SqlParser>>,
    emitters: HashMap<Dialect, Box<dyn SqlEmitter>>,
}

impl SqlTranspiler {
    pub fn new() -> Self {
        let mut parsers: HashMap<Dialect, Box<dyn SqlParser>> = HashMap::new();
        let mut emitters: HashMap<Dialect, Box<dyn SqlEmitter>> = HashMap::new();

        // Register parsers
        parsers.insert(Dialect::MySQL, Box::new(MySqlParser));
        parsers.insert(Dialect::Oracle, Box::new(OracleParser));

        // Register emitters
        emitters.insert(Dialect::MySQL, Box::new(MySqlEmitter::new()));
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
        let parser = self
            .parsers
            .get(&from)
            .ok_or(TranspileError::UnsupportedDialect(from))?;

        let emitter = self
            .emitters
            .get(&to)
            .ok_or(TranspileError::UnsupportedDialect(to))?;

        // Reset emitter state
        emitter.reset();

        // Parse → Neutral AST
        let statements = parser.parse(sql)?;

        // Emit target SQL
        let mut output = emitter.emit(&statements)?;

        // Append supplementary statements
        for stmt in emitter.supplementary_statements() {
            output.push_str("\n\n");
            output.push_str(&stmt);
        }

        Ok(output)
    }
}

#[derive(Debug)]
pub enum TranspileError {
    UnsupportedDialect(Dialect),
    ParseError(String),
    EmitError(String),
}
```

### Usage

```rust
fn main() -> Result<(), TranspileError> {
    let transpiler = SqlTranspiler::new();

    // MySQL to Oracle
    let mysql = r#"
        CREATE TABLE users (
            id BIGINT NOT NULL AUTO_INCREMENT,
            email VARCHAR(255) NOT NULL,
            active BOOLEAN DEFAULT FALSE,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id)
        ) ENGINE=InnoDB;
    "#;

    let oracle = transpiler.convert(mysql, Dialect::MySQL, Dialect::Oracle)?;
    println!("{}", oracle);

    // Oracle to MySQL
    let oracle = r#"
        CREATE TABLE products (
            id NUMBER(19) NOT NULL,
            name VARCHAR2(200 CHAR) NOT NULL,
            price NUMBER(10,2)
        );
    "#;

    let mysql = transpiler.convert(oracle, Dialect::Oracle, Dialect::MySQL)?;
    println!("{}", mysql);

    Ok(())
}
```

---

## Adding New Dialects

### Steps

1. **Add to `Dialect` enum**
2. **Implement `SqlParser` trait** - convert sqlparser AST to neutral AST
3. **Implement `SqlEmitter` trait** - convert neutral AST to SQL string
4. **Register in `SqlTranspiler::new()`**

### PostgreSQL Example

```rust
// Parser
pub struct PostgresParser;

impl SqlParser for PostgresParser {
    fn dialect(&self) -> Dialect { Dialect::PostgreSQL }

    fn parse(&self, sql: &str) -> Result<Vec<Statement>, ParseError> {
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        // ... convert to neutral AST
    }
}

impl PostgresParser {
    fn convert_data_type(&self, dt: sqlparser::ast::DataType) -> DataType {
        match dt {
            sqlparser::ast::DataType::Serial => DataType::Integer(IntegerType::Int),
            sqlparser::ast::DataType::BigSerial => DataType::Integer(IntegerType::BigInt),
            sqlparser::ast::DataType::Text => DataType::String(StringType::Text),
            sqlparser::ast::DataType::Bytea => DataType::Blob,
            sqlparser::ast::DataType::Jsonb => DataType::Json,
            sqlparser::ast::DataType::Uuid => DataType::Uuid,
            _ => todo!(),
        }
    }
}

// Emitter
pub struct PostgresEmitter;

impl SqlEmitter for PostgresEmitter {
    fn dialect(&self) -> Dialect { Dialect::PostgreSQL }
    // ...
}

impl PostgresEmitter {
    fn emit_data_type(&self, dt: &DataType) -> String {
        match dt {
            DataType::Integer(IntegerType::TinyInt) => "SMALLINT".into(),
            DataType::Boolean => "BOOLEAN".into(),  // Native!
            DataType::Json => "JSONB".into(),
            DataType::Uuid => "UUID".into(),  // Native!
            DataType::Blob => "BYTEA".into(),
            // ...
        }
    }

    fn emit_default(&self, default: &DefaultValue) -> String {
        match default {
            DefaultValue::Uuid => "gen_random_uuid()".into(),
            // ...
        }
    }
}
```

---

## Project Structure

```
sql-transpiler/
├── Cargo.toml
├── DESIGN.md
├── src/
│   ├── lib.rs              # Public API exports
│   ├── ast.rs              # Dialect-neutral AST types
│   ├── error.rs            # Error types
│   ├── transpiler.rs       # Main SqlTranspiler
│   ├── parser/
│   │   ├── mod.rs          # SqlParser trait
│   │   ├── mysql.rs
│   │   ├── oracle.rs
│   │   └── postgres.rs
│   └── emitter/
│       ├── mod.rs          # SqlEmitter trait
│       ├── mysql.rs
│       ├── oracle.rs
│       └── postgres.rs
├── tests/
│   ├── mysql_to_oracle.rs
│   ├── oracle_to_mysql.rs
│   └── roundtrip.rs
└── examples/
    └── convert.rs
```

---

## Understanding sqlparser AST

The `sqlparser-rs` crate parses SQL into an Abstract Syntax Tree (AST). Understanding this structure is key to writing parsers.

### Example SQL

```sql
CREATE TABLE users (
    id INT NOT NULL DEFAULT 0,
    name VARCHAR(100) DEFAULT 'unknown',
    PRIMARY KEY (id)
);
```

### AST Structure Visualization

```
                    ┌──────────────────────────────────────────┐
                    │     Vec<Statement>  (list of statements) │
                    └──────────────────────────────────────────┘
                                        │
                                        ▼
                    ┌──────────────────────────────────────────┐
                    │  Statement::CreateTable(CreateTable)     │
                    └──────────────────────────────────────────┘
                                        │
            ┌───────────────────────────┼───────────────────────────┐
            ▼                           ▼                           ▼
    ┌───────────────┐          ┌───────────────┐          ┌─────────────────┐
    │ name: "users" │          │ columns: Vec  │          │ constraints:Vec │
    └───────────────┘          └───────────────┘          └─────────────────┘
                                       │                           │
                        ┌──────────────┴──────────────┐            │
                        ▼                             ▼            ▼
               ┌─────────────────┐          ┌─────────────────┐  ┌──────────────┐
               │   ColumnDef     │          │   ColumnDef     │  │  PrimaryKey  │
               │  name: "id"     │          │  name: "name"   │  │ columns:[id] │
               │  data_type: Int │          │  data_type:     │  └──────────────┘
               │  options: [...] │          │   Varchar(100)  │
               └─────────────────┘          │  options: [...] │
                        │                   └─────────────────┘
                        ▼
           ┌────────────────────────────┐
           │  options: Vec<ColumnOption>│
           └────────────────────────────┘
                        │
            ┌───────────┴───────────┐
            ▼                       ▼
    ┌───────────────┐      ┌────────────────────┐
    │ NotNull       │      │ Default(Expr)      │
    └───────────────┘      └────────────────────┘
                                    │
                                    ▼
                           ┌────────────────────┐
                           │ Expr::Value(       │
                           │   Value::Number(0) │
                           │ )                  │
                           └────────────────────┘
```

### Key Navigation Paths

| What we want | Code path |
|--------------|-----------|
| Table name | `create.name.to_string()` |
| Column name | `column.name.value` |
| Column type | `column.data_type` → match on `Int`, `Varchar`, etc. |
| NOT NULL? | `column.options` → look for `ColumnOption::NotNull` |
| DEFAULT value | `column.options` → look for `ColumnOption::Default(Expr)` |
| PRIMARY KEY | `create.constraints` → look for `TableConstraint::PrimaryKey` |

### The Expr Enum

`Expr` (Expression) is used for DEFAULT values because defaults can be complex:

| SQL Default | sqlparser AST |
|-------------|---------------|
| `DEFAULT 0` | `Expr::Value(Value::Number("0"))` |
| `DEFAULT 'hi'` | `Expr::Value(Value::SingleQuotedString("hi"))` |
| `DEFAULT TRUE` | `Expr::Value(Value::Boolean(true))` |
| `DEFAULT NULL` | `Expr::Value(Value::Null)` |
| `DEFAULT NOW()` | `Expr::Function(...)` |
| `DEFAULT CURRENT_TIMESTAMP` | `Expr::Identifier(...)` |
| `DEFAULT (a + b)` | `Expr::BinaryOp(...)` (nested) |

### Debugging Tip

To see the raw AST for any SQL, use:

```rust
let ast = Parser::parse_sql(&MySqlDialect {}, sql).unwrap();
println!("{:#?}", ast);
```

// Dialect-neutral AST types

/// Supported SQL dialects
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Dialect {
    MySQL,
    Oracle,
    PostgreSQL,
    SQLite,
    SQLServer,
}

impl std::fmt::Display for Dialect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Dialect::MySQL => write!(f, "MySQL"),
            Dialect::Oracle => write!(f, "Oracle"),
            Dialect::PostgreSQL => write!(f, "PostgreSQL"),
            Dialect::SQLite => write!(f, "SQLite"),
            Dialect::SQLServer => write!(f, "SQL Server"),
        }
    }
}

/// Generic integer types - normalized across dialects
#[derive(Debug, Clone, PartialEq)]
pub enum IntegerType {
    TinyInt,  // ~1 byte
    SmallInt, // ~2 bytes
    Int,      // ~4 bytes
    BigInt,   // ~8 bytes
}

/// Generic string types
#[derive(Debug, Clone, PartialEq)]
pub enum StringType {
    Char { length: u32 },
    Varchar { length: u32 },
    Text { max_bytes: Option<u64> }, // Unbounded text with optional size hint
}

/// Generic temporal types
#[derive(Debug, Clone, PartialEq)]
pub enum TemporalType {
    Date,
    Time { precision: Option<u8> },
    /// MySQL DATETIME (or Oracle's TIMESTAMP without timezone). 8 bytes,
    /// year range 1000–9999, no auto-update on row change.
    Datetime { precision: Option<u8> },
    /// MySQL TIMESTAMP. 4 bytes, year range 1970–2038, can auto-update on
    /// row change. Semantically distinct from DATETIME — they have different
    /// storage size, range, and default-on-update behavior, so we preserve
    /// the distinction through MySQL round-trips. Oracle has no DATETIME,
    /// so both variants emit `TIMESTAMP` in Oracle output.
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

/// Default values for columns
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
    Expression(String), // Fallback for complex expressions
}

/// Column definition
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

/// Referential actions for foreign keys
#[derive(Debug, Clone, PartialEq)]
pub enum ReferentialAction {
    Cascade,
    SetNull,
    SetDefault,
    Restrict,
    NoAction,
}

/// Index column with optional ordering
#[derive(Debug, Clone)]
pub struct IndexColumn {
    pub name: String,
    pub descending: bool,
}

/// Table constraints
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

/// Table definition
#[derive(Debug, Clone, Default)]
pub struct Table {
    pub name: String,
    /// True if the parsed SQL was `CREATE TABLE IF NOT EXISTS …`. Preserved
    /// across the round-trip so idempotent re-deploys don't accidentally drop
    /// data by re-creating a table that already exists.
    pub if_not_exists: bool,
    pub columns: Vec<Column>,
    pub constraints: Vec<Constraint>,
    pub comment: Option<String>,
    /// Trailing table options as a single opaque string (e.g.
    /// `ENGINE=InnoDB DEFAULT CHARSET=utf8mb3`). MySQL-specific; carried
    /// through MySQL ↔ MySQL round-trips and silently dropped when emitting
    /// to Oracle or other dialects that don't share the same options vocabulary.
    pub mysql_table_options: Option<String>,
}

/// Alter table operations
#[derive(Debug, Clone)]
pub enum AlterOperation {
    AddColumn(Column),
    DropColumn { name: String },
    ModifyColumn(Column),
    RenameColumn { old_name: String, new_name: String },
    AddConstraint(Constraint),
    DropConstraint { name: String },
}

/// Literal values for INSERT statements
#[derive(Debug, Clone, PartialEq)]
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

/// SQL statements
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
        /// `INSERT IGNORE INTO …`. Used in seed-data INSERTs that need to be
        /// idempotent on redeploy (skip rows whose PRIMARY/UNIQUE key already
        /// exists rather than erroring out).
        ignore: bool,
    },
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<IndexColumn>,
        unique: bool,
        /// `CREATE INDEX IF NOT EXISTS …`. Preserved so re-running a deploy
        /// doesn't fail with `Duplicate key name`.
        if_not_exists: bool,
    },
    CreateDatabase {
        name: String,
        if_not_exists: bool,
    },
    Use {
        database: String,
    },
    LockTables {
        tables: Vec<(String, LockMode)>,
    },
    UnlockTables,
    Commit,
    SetVariable {
        raw_sql: String,
    },
    /// Pass-through for DML and other statements where the syntax
    /// is identical (or close enough) across dialects (e.g., UPDATE, DELETE)
    RawStatement {
        raw_sql: String,
    },
}

/// Lock mode for LOCK TABLES
#[derive(Debug, Clone, PartialEq)]
pub enum LockMode {
    Read,
    Write,
}

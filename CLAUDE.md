# Project Context

## What This Is
Rust-based SQL transpiler that converts MySQL DDL/DML to Oracle SQL. Uses `sqlparser` crate for parsing.

## Architecture
- `src/ast.rs` — Intermediate AST (Statement, Column, DataType, Constraint, Value, etc.)
- `src/parser/mysql.rs` — MySQL sqlparser AST → intermediate AST
- `src/emitter/mysql.rs` — Intermediate AST → MySQL SQL text
- `src/emitter/oracle.rs` — Intermediate AST → Oracle SQL text
- `src/transpiler.rs` — Orchestrates parse → emit pipeline
- `src/main.rs` — CLI entry point

## Build & Run
```
cargo build
cargo test
cargo run -- input.sql -f mysql -t oracle [-o output.sql]
```

## Batch Conversion
```
utils/convert_all.sh <source_dir> <target_dir> -f mysql -t oracle
```

## Current Status
- Conversion works for 40/40 MySQL files
- Oracle execution: 31/40 clean, 9 with warnings
- **Pending fix: Oracle reserved word quoting (ORA-00904)**
  - Column names that are Oracle reserved words need double-quoting in emitted SQL
  - Affected names: `comment`, `desc`, `rowid`, `number`, `mode`, `user`, `3rdparty_id` (starts with digit)
  - Fix location: `src/emitter/oracle.rs` — quote identifiers that clash with Oracle reserved words
- Other Oracle execution issues (lower priority):
  - ORA-02261: UNIQUE constraint on column already declared as PRIMARY KEY (redundant)
  - ORA-02270: FK references column that isn't PK/UNIQUE in referenced table (source schema issue)
  - ORA-00955: Duplicate table names across different SQL files sharing a schema
  - ORA-01442: Redundant ALTER TABLE MODIFY NOT NULL on already NOT NULL column

## Testing Against Oracle
Oracle XE runs in Docker container `oracle-xe` (gvenzl/oracle-xe:latest), port 1521, password `yourpass`, pluggable DB `XEPDB1`.
Project-specific scripts (user creation, SQL execution, etc.) are in `custom/` (gitignored).

## Schemas
19 Oracle users/schemas extracted from converted files — list in `custom/schemas.txt`.

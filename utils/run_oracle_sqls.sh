#!/bin/bash
# Recursively find and execute all initialize.sql files against Oracle XE via Docker
#
# Usage: ./run_oracle_sqls.sh [sql_dir] [container_name]
#   sql_dir        - root directory containing Oracle SQL files (default: output/oracle)
#   container_name - Docker container name (default: oracle-xe)

SQL_DIR="${1:-output/oracle}"
CONTAINER="${2:-oracle-xe}"
ORACLE_PWD="${ORACLE_PWD:-yourpass}"
DB="${ORACLE_DB:-XEPDB1}"
LOG_FILE="oracle-exec.log"

if [ ! -d "$SQL_DIR" ]; then
    echo "ERROR: directory '$SQL_DIR' not found"
    exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
    echo "ERROR: container '$CONTAINER' is not running"
    exit 1
fi

> "$LOG_FILE"

TOTAL=0
OK=0
FAIL=0

while IFS= read -r sql_file; do
    rel_path="${sql_file#$SQL_DIR/}"
    TOTAL=$((TOTAL + 1))

    echo "--- $rel_path ---" >> "$LOG_FILE"

    output=$(docker exec -i "$CONTAINER" sqlplus -s "sys/${ORACLE_PWD}@//localhost:1521/${DB} as sysdba" <<EOF
WHENEVER SQLERROR CONTINUE
SET ECHO ON
SET FEEDBACK ON
$(cat "$sql_file")
EOF
2>&1)

    echo "$output" >> "$LOG_FILE"
    echo "" >> "$LOG_FILE"

    # Check for ORA- errors in output
    if echo "$output" | grep -q "ORA-"; then
        FAIL=$((FAIL + 1))
        errors=$(echo "$output" | grep "ORA-" | head -3)
        echo " WARN  $rel_path"
        echo "       $errors"
    else
        OK=$((OK + 1))
        echo "  OK   $rel_path"
    fi

done < <(find "$SQL_DIR" -name "*.sql" -type f | sort)

echo ""
echo "Done. $OK/$TOTAL clean, $FAIL with warnings. Full log: $LOG_FILE"

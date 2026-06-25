#!/bin/bash
# Run a SQL file against the Oracle XE docker container
# Usage: ./test/run_oracle.sh <sql_file>

SQL_FILE="${1:?Usage: $0 <sql_file>}"

if [ ! -f "$SQL_FILE" ]; then
    echo "File not found: $SQL_FILE"
    exit 1
fi

# Copy the SQL file into the container
docker cp "$SQL_FILE" oracle-xe:/tmp/input.sql

# Run it with sqlplus
docker exec oracle-xe sqlplus -s system/yourpass@//localhost:1521/XEPDB1 @/tmp/input.sql

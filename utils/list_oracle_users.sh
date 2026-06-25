#!/bin/bash
# List all non-system Oracle users in the Oracle XE docker container
# Usage: ./test/list_oracle_users.sh

docker exec -i oracle-xe sqlplus -s system/yourpass@//localhost:1521/XEPDB1 <<EOF
SET PAGESIZE 100
SET LINESIZE 80
COLUMN USERNAME FORMAT A30
COLUMN CREATED FORMAT A20
SELECT USERNAME, CREATED FROM ALL_USERS
WHERE ORACLE_MAINTAINED = 'N'
ORDER BY CREATED;
EOF

#!/bin/bash
# Drop an Oracle user/schema and all its objects from the Oracle XE docker container
# Usage: ./test/drop_oracle_user.sh <username>

USER="${1:?Usage: $0 <username>}"

docker exec -i oracle-xe sqlplus -s system/yourpass@//localhost:1521/XEPDB1 <<EOF
DROP USER ${USER} CASCADE;
EOF

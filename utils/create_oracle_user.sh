#!/bin/bash
# Create an Oracle user/schema in the Oracle XE docker container
# Usage: ./test/create_oracle_user.sh <username> [password]
#   password defaults to username if not provided

USER="${1:?Usage: $0 <username> [password]}"
PASS="${2:-$USER}"

docker exec -i oracle-xe sqlplus -s system/yourpass@//localhost:1521/XEPDB1 <<EOF
CREATE USER ${USER} IDENTIFIED BY ${PASS};
GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE TO ${USER};
EOF

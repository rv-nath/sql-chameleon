#!/bin/bash
# Drop Oracle users/schemas listed in schemas.txt (CASCADE to remove all objects)
#
# Usage: ./drop_users.sh [schemas_file] [container_name]
#   schemas_file   - file with one username per line (default: schemas.txt)
#   container_name - Docker container name (default: oracle-xe)

SCHEMAS_FILE="${1:-schemas.txt}"
CONTAINER="${2:-oracle-xe}"
ORACLE_PWD="${ORACLE_PWD:-yourpass}"
DB="${ORACLE_DB:-XEPDB1}"

if [ ! -f "$SCHEMAS_FILE" ]; then
    echo "ERROR: schemas file '$SCHEMAS_FILE' not found"
    exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
    echo "ERROR: container '$CONTAINER' is not running"
    exit 1
fi

TOTAL=0
OK=0
SKIP=0
FAIL=0

while IFS= read -r user; do
    user=$(echo "$user" | xargs)
    [ -z "$user" ] && continue
    [[ "$user" == \#* ]] && continue

    TOTAL=$((TOTAL + 1))
    upper_user=$(echo "$user" | tr '[:lower:]' '[:upper:]')

    sql="
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM all_users WHERE username = '${upper_user}';
    IF v_count > 0 THEN
        EXECUTE IMMEDIATE 'DROP USER ${user} CASCADE';
        DBMS_OUTPUT.PUT_LINE('DROPPED: ${user}');
    ELSE
        DBMS_OUTPUT.PUT_LINE('NOTFOUND: ${user}');
    END IF;
END;
/
"

    output=$(docker exec -i "$CONTAINER" sqlplus -s "sys/${ORACLE_PWD}@//localhost:1521/${DB} as sysdba" <<EOF
SET SERVEROUTPUT ON
SET FEEDBACK OFF
${sql}
EOF
2>&1)

    if echo "$output" | grep -q "DROPPED:"; then
        OK=$((OK + 1))
        echo "  DROPPED   $user"
    elif echo "$output" | grep -q "NOTFOUND:"; then
        SKIP=$((SKIP + 1))
        echo "  NOTFOUND  $user"
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL      $user"
        echo "            $output" | head -5
    fi

done < "$SCHEMAS_FILE"

echo ""
echo "Done. $TOTAL users processed: $OK dropped, $SKIP not found, $FAIL failed."

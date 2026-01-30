#!/bin/bash
# Batch convert all SQL files from source dialect to target dialect
# Replicates the folder structure of the source directory into the target directory
#
# Usage: ./convert_all.sh <source_dir> <target_dir> [-f from_dialect] [-t to_dialect]
#   -f  source dialect (default: mysql)
#   -t  target dialect (default: oracle)
#
# Example:
#   ./convert_all.sh /path/to/mysql /path/to/oracle -f mysql -t oracle

SRC_DIR="${1:?Usage: $0 <source_dir> <target_dir> [-f from] [-t to]}"
TGT_DIR="${2:?Usage: $0 <source_dir> <target_dir> [-f from] [-t to]}"
shift 2

FROM="mysql"
TO="oracle"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -f) FROM="$2"; shift 2 ;;
        -t) TO="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Resolve paths
SRC_DIR="$(realpath "$SRC_DIR")"
TGT_DIR="$(realpath -m "$TGT_DIR")"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BINARY="$SCRIPT_DIR/target/debug/schema-conv"

if [ ! -f "$BINARY" ]; then
    echo "Binary not found. Building..."
    cargo build --manifest-path "$SCRIPT_DIR/Cargo.toml"
fi

TOTAL=0
OK=0
FAIL=0

# Find all .sql files and convert each one
while IFS= read -r src_file; do
    # Get path relative to source dir
    rel_path="${src_file#$SRC_DIR/}"
    tgt_file="$TGT_DIR/$rel_path"

    # Create target directory
    mkdir -p "$(dirname "$tgt_file")"

    # Convert
    TOTAL=$((TOTAL + 1))
    output=$("$BINARY" "$src_file" -f "$FROM" -t "$TO" -o "$tgt_file" 2>&1)

    if [ $? -eq 0 ]; then
        OK=$((OK + 1))
        echo "  OK  $rel_path"
    else
        FAIL=$((FAIL + 1))
        echo " FAIL $rel_path"
        echo "      $output"
    fi
done < <(find "$SRC_DIR" -name "*.sql" -type f | sort)

echo ""
echo "Done. $OK/$TOTAL succeeded, $FAIL failed."

#!/bin/bash
set -e

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_FILE="$BASE_DIR/pdcd_config.ini"
SQL_FILE="$BASE_DIR/initialize_pdcd_functions_v2.sql"

# Function to read value from a given section
get_value () {
  local section="$1"
  local key="$2"

  awk -F= -v section="$section" -v key="$key" '
    $0=="["section"]" {found=1; next}
    /^\[/ {found=0}
    found && $1~key {
      gsub(/^[ \t]+|[ \t]+$/, "", $2)
      print $2
      exit
    }
  ' "$CONFIG_FILE"
}

# Get all database sections
DATABASE_SECTIONS=$(grep -o '^\[database_[^]]*]' "$CONFIG_FILE" | tr -d '[]')

for SECTION in $DATABASE_SECTIONS
do
  echo "--------------------------------------------"
  echo "Deploying for config section: [$SECTION]"

  PG_HOST=$(get_value "$SECTION" host)
  PG_PORT=$(get_value "$SECTION" port)
  PG_DB=$(get_value "$SECTION" dbname)
  PG_USER=$(get_value "$SECTION" user)
  PG_PASS=$(get_value "$SECTION" password)
  SCHEMA_NAME=$(get_value "$SECTION" schema_name)

  export PGPASSWORD="$PG_PASS"

  echo "Host   : $PG_HOST"
  echo "DB     : $PG_DB"
  echo "User   : $PG_USER"
  echo "Schema : $SCHEMA_NAME"

  psql \
    -h "$PG_HOST" \
    -p "$PG_PORT" \
    -U "$PG_USER" \
    -d "$PG_DB" \
    -v schema_name="$SCHEMA_NAME" \
    -f "$SQL_FILE"

  echo " Deployment completed for [$SECTION]"
done

echo " All database deployments completed successfully."
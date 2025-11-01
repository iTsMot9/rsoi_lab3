#!/usr/bin/env bash
set -e

# TODO для создания баз прописать свой вариант
export VARIANT="v3"
export SCRIPT_PATH=/docker-entrypoint-initdb.d/
export PGPASSWORD=postgres
psql --dbname template1 -f "$SCRIPT_PATH/scripts/db-$VARIANT.sql"
psql -U program -d cars -f /schemas/schema-3.sql
psql -U program -d rentals -f /schemas/schema-3.sql
psql -U program -d payments -f /schemas/schema-3.sql

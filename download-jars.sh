#!/usr/bin/env bash
set -euo pipefail

JARS_DIR="$(cd "$(dirname "$0")" && pwd)/jars"
mkdir -p "$JARS_DIR"

echo "Downloading PostgreSQL JDBC driver..."
curl -fSL -o "$JARS_DIR/postgresql-42.7.1.jar" \
  "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar"

echo "Downloading ClickHouse JDBC driver..."
curl -fSL -o "$JARS_DIR/clickhouse-jdbc-0.6.0-all.jar" \
  "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.0/clickhouse-jdbc-0.6.0-all.jar"

echo "Done. JARs downloaded to $JARS_DIR:"
ls -lh "$JARS_DIR"

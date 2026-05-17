#!/bin/bash
# Скрипт загрузки JDBC-коннекторов для Spark

DRIVER_DIR="$(dirname "$(realpath "$0")")/drivers"
mkdir -p "$DRIVER_DIR"

PG_VERSION="42.7.1"
CH_VERSION="0.6.0"

echo "[*] Загрузка PostgreSQL JDBC ($PG_VERSION)..."
wget -q -O "$DRIVER_DIR/pg-jdbc-${PG_VERSION}.jar" \
  "https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_VERSION}/postgresql-${PG_VERSION}.jar"

echo "[*] Загрузка ClickHouse JDBC ($CH_VERSION)..."
wget -q -O "$DRIVER_DIR/ch-jdbc-${CH_VERSION}-all.jar" \
  "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/${CH_VERSION}/clickhouse-jdbc-${CH_VERSION}-all.jar"

echo "[+] Драйверы загружены:"
ls -la "$DRIVER_DIR"

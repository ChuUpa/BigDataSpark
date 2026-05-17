# Лабораторная работа №2 — ETL на Apache Spark

## Описание

Реализация ETL-пайплайна, который:
1. Загружает 10 000 строк из CSV → PostgreSQL (плоская таблица `mock_data`)
2. Трансформирует данные в модель «звезда» (5 измерений + 1 факт) средствами Spark
3. Формирует 18 аналитических отчётов (6 витрин × 3 отчёта) и записывает в ClickHouse

## Предварительные требования

- Docker и Docker Compose
- Bash-совместимая оболочка (или Git Bash на Windows)

## Быстрый старт

### 1. Загрузить JDBC-драйверы

```bash
chmod +x get_drivers.sh && ./get_drivers.sh
```

### 2. Скопировать CSV-данные

Положить все файлы `MOCK_DATA*.csv` в папку `data/`:

```bash
mkdir -p data
cp ../исходные\ данные/* data/
```

### 3. Поднять инфраструктуру

```bash
docker-compose up -d
```

Дождаться готовности (~30–40 секунд). Проверка:

```bash
docker exec lab2-pg psql -U etl_user -d salesdb -c "SELECT count(*) FROM mock_data;"
```

Ожидаемый результат: 10000 строк.

### 4. Запуск ETL: raw → star schema (PostgreSQL)

```bash
docker exec lab2-runner spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/drivers/pg-jdbc-42.7.1.jar \
  /opt/app/star_schema_etl.py
```

После выполнения в PostgreSQL появятся таблицы:
`dim_customers`, `dim_sellers`, `dim_products`, `dim_stores`, `dim_suppliers`, `fact_sales`

### 5. Запуск ETL: star schema → отчёты ClickHouse

```bash
docker exec lab2-runner spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/drivers/pg-jdbc-42.7.1.jar,/opt/drivers/ch-jdbc-0.6.0-all.jar \
  /opt/app/clickhouse_reports.py
```

### 6. Проверка результатов в ClickHouse

```bash
# Список таблиц
docker exec lab2-ch clickhouse-client --user analyst --password ch_secure_456 \
  --query "SHOW TABLES FROM analytics"

# Примеры запросов к витринам:
docker exec lab2-ch clickhouse-client --user analyst --password ch_secure_456 \
  --query "SELECT * FROM analytics.rpt_product_top10"

docker exec lab2-ch clickhouse-client --user analyst --password ch_secure_456 \
  --query "SELECT * FROM analytics.rpt_customer_top10"

docker exec lab2-ch clickhouse-client --user analyst --password ch_secure_456 \
  --query "SELECT * FROM analytics.rpt_time_monthly ORDER BY yr, mn"

docker exec lab2-ch clickhouse-client --user analyst --password ch_secure_456 \
  --query "SELECT * FROM analytics.rpt_store_top5"

docker exec lab2-ch clickhouse-client --user analyst --password ch_secure_456 \
  --query "SELECT * FROM analytics.rpt_supplier_top5"

docker exec lab2-ch clickhouse-client --user analyst --password ch_secure_456 \
  --query "SELECT * FROM analytics.rpt_quality_extremes LIMIT 10"
```

## Подключение через GUI

| БД | Host | Port | Database | User | Password |
|----|------|------|----------|------|----------|
| PostgreSQL | localhost | 5433 | salesdb | etl_user | etl_pass_2024 |
| ClickHouse | localhost | 8124 | analytics | analyst | ch_secure_456 |

## Остановка

```bash
docker-compose down        # остановить
docker-compose down -v     # остановить + удалить volumes
```

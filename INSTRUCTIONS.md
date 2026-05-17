## Шаг 1: Скачать JDBC-драйверы

```bash
bash download-jars.sh
```

## Шаг 2: Запустить инфраструктуру

```bash
docker-compose up -d
```

Подождать ~30 сек, пока поднимутся все сервисы. PostgreSQL сам создаст таблицу `mock_data` и загрузит 10 CSV-файлов (10 000 строк).

Проверка:
```bash
docker exec bigdata-postgres psql -U spark -d bigdata -c "SELECT COUNT(*) FROM mock_data;"
```

## Шаг 3: ETL — трансформация в звезду (PostgreSQL)

```bash
docker exec bigdata-spark-submit /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark-jars/postgresql-42.7.1.jar \
  /app/etl_to_star_schema.py
```

После выполнения в PostgreSQL появятся таблицы `dim_customer`, `dim_seller`, `dim_product`, `dim_store`, `dim_supplier` и `fact_sales`.

## Шаг 4: ETL — отчёты в ClickHouse

```bash
docker exec bigdata-spark-submit /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark-jars/postgresql-42.7.1.jar,/opt/spark-jars/clickhouse-jdbc-0.6.0-all.jar \
  /app/etl_reports_to_clickhouse.py
```

В ClickHouse появятся 18 таблиц — по 3 на каждую витрину.

## Шаг 5: Проверка

```bash
docker exec bigdata-clickhouse clickhouse-client --password clickhouse123 --query "SHOW TABLES"

# Витрина продуктов
docker exec bigdata-clickhouse clickhouse-client --password clickhouse123 --query "SELECT * FROM product_top10"
docker exec bigdata-clickhouse clickhouse-client --password clickhouse123 --query "SELECT * FROM product_revenue_by_category"
docker exec bigdata-clickhouse clickhouse-client --password clickhouse123 --query "SELECT * FROM product_rating_reviews LIMIT 10"

# Витрина клиентов
docker exec bigdata-clickhouse clickhouse-client --password clickhouse123 --query "SELECT * FROM customer_top10"
docker exec bigdata-clickhouse clickhouse-client --password clickhouse123 --query "SELECT * FROM customer_by_country"
docker exec bigdata-clickhouse clickhouse-client --password clickhouse123 --query "SELECT * FROM customer_avg_check LIMIT 10"

# Витрина времени
docker exec bigdata-clickhouse clickhouse-client --password clickhouse123 --query "SELECT * FROM time_monthly_trends ORDER BY sale_year, sale_month"
docker exec bigdata-clickhouse clickhouse-client --password clickhouse123 --query "SELECT * FROM time_yearly_comparison"
docker exec bigdata-clickhouse clickhouse-client --password clickhouse123 --query "SELECT * FROM time_avg_order_by_month ORDER BY sale_year, sale_month"

# Витрина магазинов
docker exec bigdata-clickhouse clickhouse-client --password clickhouse123 --query "SELECT * FROM store_top5"
docker exec bigdata-clickhouse clickhouse-client --password clickhouse123 --query "SELECT * FROM store_sales_by_location LIMIT 10"
docker exec bigdata-clickhouse clickhouse-client --password clickhouse123 --query "SELECT * FROM store_avg_check LIMIT 10"

# Витрина поставщиков
docker exec bigdata-clickhouse clickhouse-client --password clickhouse123 --query "SELECT * FROM supplier_top5"
docker exec bigdata-clickhouse clickhouse-client --password clickhouse123 --query "SELECT * FROM supplier_avg_price LIMIT 10"
docker exec bigdata-clickhouse clickhouse-client --password clickhouse123 --query "SELECT * FROM supplier_by_country"

# Витрина качества
docker exec bigdata-clickhouse clickhouse-client --password clickhouse123 --query "SELECT * FROM quality_best_worst LIMIT 10"
docker exec bigdata-clickhouse clickhouse-client --password clickhouse123 --query "SELECT * FROM quality_rating_vs_sales LIMIT 10"
docker exec bigdata-clickhouse clickhouse-client --password clickhouse123 --query "SELECT * FROM quality_most_reviewed LIMIT 10"
```

Также можно подключиться через DataGrip:
- **PostgreSQL**: host=localhost, port=5432, database=bigdata, user=spark, password=spark123
- **ClickHouse**: host=localhost, port=8123, database=default, user=default, password=clickhouse123

## Остановка

```bash
docker-compose down
```

Для удаления volumes:
```bash
docker-compose down -v
```

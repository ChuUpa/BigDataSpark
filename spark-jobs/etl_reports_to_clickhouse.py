from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, count, avg, round as _round,
    year, month, corr,
)
from pyspark.sql.types import StringType, DecimalType, DoubleType, FloatType


PG_URL = "jdbc:postgresql://postgres:5432/bigdata"
PG_PROPERTIES = {
    "user": "spark",
    "password": "spark123",
    "driver": "org.postgresql.Driver",
}

CH_URL = "jdbc:ch://clickhouse:8123/default?compress=0"
CH_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("ETL Reports to ClickHouse")
        .config(
            "spark.jars",
            "/opt/spark-jars/postgresql-42.7.1.jar,"
            "/opt/spark-jars/clickhouse-jdbc-0.6.0-all.jar",
        )
        .config("spark.executor.memory", "512m")
        .config("spark.executor.cores", "1")
        .config("spark.driver.memory", "512m")
        .getOrCreate()
    )


def read_pg_table(spark, table_name):
    return (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", table_name)
        .option("user", PG_PROPERTIES["user"])
        .option("password", PG_PROPERTIES["password"])
        .option("driver", PG_PROPERTIES["driver"])
        .load()
    )


def write_to_clickhouse(df, table_name, order_by="tuple()"):
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.fillna({field.name: ""})
        elif isinstance(field.dataType, (DecimalType, DoubleType, FloatType)):
            df = df.fillna({field.name: 0.0})
        else:
            df = df.fillna({field.name: 0})
    (
        df.write.format("jdbc")
        .option("url", CH_URL)
        .option("dbtable", table_name)
        .option("driver", CH_DRIVER)
        .option("user", "default")
        .option("password", "clickhouse123")
        .option("createTableOptions", f"ENGINE = MergeTree() ORDER BY {order_by}")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )


def product_top10(fact_sales, dim_product):
    joined = fact_sales.join(dim_product, "product_sk")
    return (
        joined.groupBy(col("name").alias("product_name"))
        .agg(
            _sum("sale_quantity").alias("total_quantity"),
            _sum("sale_total_price").alias("total_revenue"),
        )
        .orderBy(col("total_quantity").desc())
        .limit(10)
    )


def product_revenue_by_category(fact_sales, dim_product):
    joined = fact_sales.join(dim_product, "product_sk")
    return (
        joined.groupBy(col("category"))
        .agg(_sum("sale_total_price").alias("total_revenue"))
        .orderBy(col("total_revenue").desc())
    )


def product_rating_reviews(fact_sales, dim_product):
    joined = fact_sales.join(dim_product, "product_sk")
    return (
        joined.groupBy(
            col("name").alias("product_name"),
            col("category"),
        ).agg(
            _round(avg("rating"), 2).alias("avg_rating"),
            _sum("reviews").alias("total_reviews"),
        )
        .orderBy(col("avg_rating").desc())
    )


def customer_top10(fact_sales, dim_customer):
    joined = fact_sales.join(dim_customer, "customer_sk")
    return (
        joined.groupBy("customer_id", "first_name", "last_name")
        .agg(_sum("sale_total_price").alias("total_spent"))
        .orderBy(col("total_spent").desc())
        .limit(10)
    )


def customer_by_country(fact_sales, dim_customer):
    joined = fact_sales.join(dim_customer, "customer_sk")
    return (
        joined.groupBy("country")
        .agg(
            count("customer_id").alias("customer_count"),
            _sum("sale_total_price").alias("total_revenue"),
        )
        .orderBy(col("total_revenue").desc())
    )


def customer_avg_check(fact_sales, dim_customer):
    joined = fact_sales.join(dim_customer, "customer_sk")
    return (
        joined.groupBy("customer_id", "first_name", "last_name")
        .agg(
            _sum("sale_total_price").alias("total_spent"),
            count("*").alias("order_count"),
        )
        .withColumn("avg_check", _round(col("total_spent") / col("order_count"), 2))
        .orderBy(col("avg_check").desc())
    )


def time_monthly_trends(fact_sales):
    return (
        fact_sales
        .withColumn("sale_year", year("sale_date"))
        .withColumn("sale_month", month("sale_date"))
        .groupBy("sale_year", "sale_month")
        .agg(
            _sum("sale_total_price").alias("total_revenue"),
            _sum("sale_quantity").alias("total_quantity"),
            count("*").alias("order_count"),
        )
        .orderBy("sale_year", "sale_month")
    )


def time_yearly_comparison(fact_sales):
    return (
        fact_sales
        .withColumn("sale_year", year("sale_date"))
        .groupBy("sale_year")
        .agg(
            _sum("sale_total_price").alias("total_revenue"),
            count("*").alias("order_count"),
        )
        .orderBy("sale_year")
    )


def time_avg_order_by_month(fact_sales):
    return (
        fact_sales
        .withColumn("sale_year", year("sale_date"))
        .withColumn("sale_month", month("sale_date"))
        .groupBy("sale_year", "sale_month")
        .agg(
            _round(avg("sale_total_price"), 2).alias("avg_order_size"),
            count("*").alias("order_count"),
        )
        .orderBy("sale_year", "sale_month")
    )


def store_top5(fact_sales, dim_store):
    joined = fact_sales.join(dim_store, "store_sk")
    return (
        joined.groupBy("store_name")
        .agg(_sum("sale_total_price").alias("total_revenue"))
        .orderBy(col("total_revenue").desc())
        .limit(5)
    )


def store_sales_by_location(fact_sales, dim_store):
    joined = fact_sales.join(dim_store, "store_sk")
    return (
        joined.groupBy("store_city", "store_country")
        .agg(
            _sum("sale_total_price").alias("total_revenue"),
            count("*").alias("order_count"),
        )
        .orderBy(col("total_revenue").desc())
    )


def store_avg_check(fact_sales, dim_store):
    joined = fact_sales.join(dim_store, "store_sk")
    return (
        joined.groupBy("store_name")
        .agg(
            _sum("sale_total_price").alias("total_revenue"),
            count("*").alias("order_count"),
        )
        .withColumn("avg_check", _round(col("total_revenue") / col("order_count"), 2))
        .orderBy(col("avg_check").desc())
    )


def supplier_top5(fact_sales, dim_supplier):
    joined = fact_sales.join(dim_supplier, "supplier_sk")
    return (
        joined.groupBy("supplier_name")
        .agg(_sum("sale_total_price").alias("total_revenue"))
        .orderBy(col("total_revenue").desc())
        .limit(5)
    )


def supplier_avg_price(fact_sales, dim_supplier, dim_product):
    joined = (
        fact_sales
        .join(dim_supplier, "supplier_sk")
        .join(dim_product, "product_sk")
    )
    return (
        joined.groupBy("supplier_name")
        .agg(_round(avg("product_price"), 2).alias("avg_product_price"))
        .orderBy(col("avg_product_price").desc())
    )


def supplier_by_country(fact_sales, dim_supplier):
    joined = fact_sales.join(dim_supplier, "supplier_sk")
    return (
        joined.groupBy("supplier_country")
        .agg(
            _sum("sale_total_price").alias("total_revenue"),
            count("*").alias("order_count"),
        )
        .orderBy(col("total_revenue").desc())
    )


def quality_best_worst(fact_sales, dim_product):
    joined = fact_sales.join(dim_product, "product_sk")
    return (
        joined.groupBy(col("name").alias("product_name"), "category")
        .agg(
            _round(avg("rating"), 2).alias("avg_rating"),
            _sum("sale_quantity").alias("total_sold"),
        )
        .orderBy(col("avg_rating").desc())
    )


def quality_rating_vs_sales(fact_sales, dim_product):
    joined = fact_sales.join(dim_product, "product_sk")
    grouped = joined.groupBy(col("name").alias("product_name")).agg(
        _round(avg("rating"), 2).alias("avg_rating"),
        _sum("sale_quantity").alias("total_sold"),
    )
    correlation = grouped.select(
        _round(corr("avg_rating", "total_sold"), 4).alias("correlation")
    )
    corr_val = correlation.collect()[0]["correlation"] or 0.0
    return grouped.withColumn("rating_sales_corr", _round(col("avg_rating") * 0 + corr_val, 4))


def quality_most_reviewed(fact_sales, dim_product):
    joined = fact_sales.join(dim_product, "product_sk")
    return (
        joined.groupBy(col("name").alias("product_name"), "category")
        .agg(
            _sum("reviews").alias("total_reviews"),
            _sum("sale_quantity").alias("total_sold"),
        )
        .orderBy(col("total_reviews").desc())
    )


def main():
    spark = create_spark_session()

    print("Reading star schema tables from PostgreSQL...")
    fact_sales = read_pg_table(spark, "fact_sales")
    dim_customer = read_pg_table(spark, "dim_customer")
    dim_product = read_pg_table(spark, "dim_product")
    dim_store = read_pg_table(spark, "dim_store")
    dim_supplier = read_pg_table(spark, "dim_supplier")

    print("Витрина 1: продукты...")
    write_to_clickhouse(product_top10(fact_sales, dim_product), "product_top10", "product_name")
    write_to_clickhouse(product_revenue_by_category(fact_sales, dim_product), "product_revenue_by_category", "category")
    write_to_clickhouse(product_rating_reviews(fact_sales, dim_product), "product_rating_reviews", "product_name")

    print("Витрина 2: клиенты...")
    write_to_clickhouse(customer_top10(fact_sales, dim_customer), "customer_top10", "customer_id")
    write_to_clickhouse(customer_by_country(fact_sales, dim_customer), "customer_by_country", "country")
    write_to_clickhouse(customer_avg_check(fact_sales, dim_customer), "customer_avg_check", "customer_id")

    print("Витрина 3: время...")
    write_to_clickhouse(time_monthly_trends(fact_sales), "time_monthly_trends", "(sale_year, sale_month)")
    write_to_clickhouse(time_yearly_comparison(fact_sales), "time_yearly_comparison", "sale_year")
    write_to_clickhouse(time_avg_order_by_month(fact_sales), "time_avg_order_by_month", "(sale_year, sale_month)")

    print("Витрина 4: магазины...")
    write_to_clickhouse(store_top5(fact_sales, dim_store), "store_top5", "store_name")
    write_to_clickhouse(store_sales_by_location(fact_sales, dim_store), "store_sales_by_location", "(store_country, store_city)")
    write_to_clickhouse(store_avg_check(fact_sales, dim_store), "store_avg_check", "store_name")

    print("Витрина 5: поставщики...")
    write_to_clickhouse(supplier_top5(fact_sales, dim_supplier), "supplier_top5", "supplier_name")
    write_to_clickhouse(supplier_avg_price(fact_sales, dim_supplier, dim_product), "supplier_avg_price", "supplier_name")
    write_to_clickhouse(supplier_by_country(fact_sales, dim_supplier), "supplier_by_country", "supplier_country")

    print("Витрина 6: качество...")
    write_to_clickhouse(quality_best_worst(fact_sales, dim_product), "quality_best_worst", "product_name")
    write_to_clickhouse(quality_rating_vs_sales(fact_sales, dim_product), "quality_rating_vs_sales", "product_name")
    write_to_clickhouse(quality_most_reviewed(fact_sales, dim_product), "quality_most_reviewed", "product_name")

    print("All reports written to ClickHouse.")
    spark.stop()


if __name__ == "__main__":
    main()

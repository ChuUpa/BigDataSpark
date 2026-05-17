"""
строим витрины из star schema и пишем в ClickHouse.
18 таблиц.
"""
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DecimalType, DoubleType, FloatType

sys.path.insert(0, "/opt/app")
from config import PostgresConfig, ClickHouseConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("ch_reports")


def get_spark():
    return (
        SparkSession.builder
        .appName("Lab2_ClickHouseReports")
        .config(
            "spark.jars",
            "/opt/drivers/pg-jdbc-42.7.1.jar,"
            "/opt/drivers/ch-jdbc-0.6.0-all.jar",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )


def read_pg(spark, table):
    opts = PostgresConfig.jdbc_options()
    return (
        spark.read.format("jdbc")
        .option("url", opts["url"])
        .option("dbtable", table)
        .option("user", opts["user"])
        .option("password", opts["password"])
        .option("driver", opts["driver"])
        .load()
    )


def save_ch(df, table, order_by="tuple()"):
    """Запись DataFrame в ClickHouse с автоматической обработкой NULL."""
    for fld in df.schema.fields:
        if isinstance(fld.dataType, StringType):
            df = df.fillna({fld.name: ""})
        elif isinstance(fld.dataType, (DecimalType, DoubleType, FloatType)):
            df = df.fillna({fld.name: 0.0})
        else:
            df = df.fillna({fld.name: 0})

    opts = ClickHouseConfig.jdbc_options()
    (
        df.write.format("jdbc")
        .option("url", opts["url"])
        .option("dbtable", table)
        .option("driver", opts["driver"])
        .option("user", opts["user"])
        .option("password", opts["password"])
        .option("createTableOptions", f"ENGINE = MergeTree() ORDER BY {order_by}")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )
    log.info(f"    ClickHouse <- {table}")


# ---------- Витрина 1: Продукты ----------

def report_products_bestsellers(facts, products):
    """Топ-10 товаров по количеству проданных штук"""
    merged = facts.join(products, "product_key")
    return (
        merged
        .groupBy(F.col("title").alias("product"))
        .agg(
            F.sum("qty").alias("units_sold"),
            F.sum("revenue").alias("gross_revenue"),
        )
        .orderBy(F.desc("units_sold"))
        .limit(10)
    )


def report_products_category_revenue(facts, products):
    merged = facts.join(products, "product_key")
    return (
        merged
        .groupBy("category")
        .agg(F.sum("revenue").alias("category_revenue"))
        .orderBy(F.desc("category_revenue"))
    )


def report_products_ratings(facts, products):
    merged = facts.join(products, "product_key")
    return (
        merged
        .groupBy(F.col("title").alias("product"), "category")
        .agg(
            F.round(F.avg("rating"), 2).alias("mean_rating"),
            F.sum("num_reviews").alias("review_count"),
        )
        .orderBy(F.desc("mean_rating"))
    )


# ---------- Витрина 2: Клиенты ----------

def report_customers_top_spenders(facts, customers):
    merged = facts.join(customers, "customer_key")
    return (
        merged
        .groupBy("customer_nk", "fname", "lname")
        .agg(F.sum("revenue").alias("lifetime_value"))
        .orderBy(F.desc("lifetime_value"))
        .limit(10)
    )


def report_customers_geography(facts, customers):
    """Сколько уникальных клиентов и выручки по каждой стране"""
    merged = facts.join(customers, "customer_key")
    return (
        merged
        .groupBy("country")
        .agg(
            F.countDistinct("customer_nk").alias("unique_customers"),
            F.sum("revenue").alias("country_revenue"),
        )
        .orderBy(F.desc("country_revenue"))
    )


def report_customers_avg_order(facts, customers):
    merged = facts.join(customers, "customer_key")
    return (
        merged
        .groupBy("customer_nk", "fname", "lname")
        .agg(
            F.sum("revenue").alias("total_spent"),
            F.count("*").alias("num_orders"),
        )
        .withColumn("avg_order_value", F.round(F.col("total_spent") / F.col("num_orders"), 2))
        .orderBy(F.desc("avg_order_value"))
    )


# ---------- Витрина 3: Время ----------

def report_time_monthly(facts):
    return (
        facts
        .withColumn("yr", F.year("sale_date"))
        .withColumn("mn", F.month("sale_date"))
        .groupBy("yr", "mn")
        .agg(
            F.sum("revenue").alias("monthly_revenue"),
            F.sum("qty").alias("monthly_units"),
            F.count("*").alias("num_transactions"),
        )
        .orderBy("yr", "mn")
    )


def report_time_yearly(facts):
    return (
        facts
        .withColumn("yr", F.year("sale_date"))
        .groupBy("yr")
        .agg(
            F.sum("revenue").alias("annual_revenue"),
            F.count("*").alias("annual_orders"),
        )
        .orderBy("yr")
    )


def report_time_avg_basket(facts):
    """Средний чек помесячно"""
    return (
        facts
        .withColumn("yr", F.year("sale_date"))
        .withColumn("mn", F.month("sale_date"))
        .groupBy("yr", "mn")
        .agg(
            F.round(F.avg("revenue"), 2).alias("avg_basket"),
            F.count("*").alias("orders"),
        )
        .orderBy("yr", "mn")
    )


# ---------- Витрина 4: Магазины ----------

def report_stores_top(facts, stores):
    merged = facts.join(stores, "store_key")
    return (
        merged
        .groupBy(F.col("name").alias("store"))
        .agg(F.sum("revenue").alias("store_revenue"))
        .orderBy(F.desc("store_revenue"))
        .limit(5)
    )


def report_stores_geography(facts, stores):
    merged = facts.join(stores, "store_key")
    return (
        merged
        .groupBy("city", "country")
        .agg(
            F.sum("revenue").alias("location_revenue"),
            F.count("*").alias("transactions"),
        )
        .orderBy(F.desc("location_revenue"))
    )


def report_stores_avg_check(facts, stores):
    merged = facts.join(stores, "store_key")
    return (
        merged
        .groupBy(F.col("name").alias("store"))
        .agg(
            F.sum("revenue").alias("total_rev"),
            F.count("*").alias("orders"),
        )
        .withColumn("avg_check", F.round(F.col("total_rev") / F.col("orders"), 2))
        .orderBy(F.desc("avg_check"))
    )


# ---------- Витрина 5: Поставщики ----------

def report_suppliers_top(facts, suppliers):
    merged = facts.join(suppliers, "supplier_key")
    return (
        merged
        .groupBy(F.col("name").alias("supplier"))
        .agg(F.sum("revenue").alias("supplier_revenue"))
        .orderBy(F.desc("supplier_revenue"))
        .limit(5)
    )


def report_suppliers_avg_price(facts, suppliers, products):
    """Средняя цена товаров у каждого поставщика"""
    prod_prices = products.select("product_key", F.col("unit_price").alias("catalog_price"))
    merged = (
        facts
        .join(suppliers, "supplier_key")
        .join(prod_prices, "product_key")
    )
    return (
        merged
        .groupBy(F.col("name").alias("supplier"))
        .agg(F.round(F.avg("catalog_price"), 2).alias("mean_price"))
        .orderBy(F.desc("mean_price"))
    )


def report_suppliers_countries(facts, suppliers):
    merged = facts.join(suppliers, "supplier_key")
    return (
        merged
        .groupBy(F.col("country").alias("supplier_country"))
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.count("*").alias("num_orders"),
        )
        .orderBy(F.desc("total_revenue"))
    )


# ---------- Витрина 6: Качество ----------

def report_quality_extremes(facts, products):
    """Лучшие и худшие по рейтингу (сортировка desc, можно смотреть с двух концов)"""
    merged = facts.join(products, "product_key")
    return (
        merged
        .groupBy(F.col("title").alias("product"), "category")
        .agg(
            F.round(F.avg("rating"), 2).alias("avg_rating"),
            F.sum("qty").alias("total_units"),
        )
        .orderBy(F.desc("avg_rating"))
    )


def report_quality_correlation(facts, products):
    merged = facts.join(products, "product_key")
    agg_df = merged.groupBy(F.col("title").alias("product")).agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.sum("qty").alias("units_sold"),
    )
    corr_value = agg_df.select(
        F.round(F.corr("avg_rating", "units_sold"), 4).alias("r")
    ).collect()[0]["r"] or 0.0

    return agg_df.withColumn("pearson_r", F.lit(corr_value).cast("double"))


def report_quality_most_reviewed(facts, products):
    merged = facts.join(products, "product_key")
    return (
        merged
        .groupBy(F.col("title").alias("product"), "category")
        .agg(
            F.sum("num_reviews").alias("reviews_total"),
            F.sum("qty").alias("units_sold"),
        )
        .orderBy(F.desc("reviews_total"))
    )


def run():
    spark = get_spark()
    log.info("=== Старт: формирование витрин для ClickHouse ===")
    
    facts = read_pg(spark, "fact_sales")
    customers = read_pg(spark, "dim_customers")
    products = read_pg(spark, "dim_products")
    stores = read_pg(spark, "dim_stores")
    suppliers = read_pg(spark, "dim_suppliers")

    # --- Витрина 1: Продукты ---
    log.info("[1/6] Витрина продуктов")
    save_ch(report_products_bestsellers(facts, products), "rpt_product_top10", "product")
    save_ch(report_products_category_revenue(facts, products), "rpt_product_by_category", "category")
    save_ch(report_products_ratings(facts, products), "rpt_product_ratings", "product")

    # --- Витрина 2: Клиенты ---
    log.info("[2/6] Витрина клиентов")
    save_ch(report_customers_top_spenders(facts, customers), "rpt_customer_top10", "customer_nk")
    save_ch(report_customers_geography(facts, customers), "rpt_customer_geo", "country")
    save_ch(report_customers_avg_order(facts, customers), "rpt_customer_avg_order", "customer_nk")

    # --- Витрина 3: Время ---
    log.info("[3/6] Витрина временных трендов")
    save_ch(report_time_monthly(facts), "rpt_time_monthly", "(yr, mn)")
    save_ch(report_time_yearly(facts), "rpt_time_yearly", "yr")
    save_ch(report_time_avg_basket(facts), "rpt_time_avg_basket", "(yr, mn)")

    # --- Витрина 4: Магазины ---
    log.info("[4/6] Витрина магазинов")
    save_ch(report_stores_top(facts, stores), "rpt_store_top5", "store")
    save_ch(report_stores_geography(facts, stores), "rpt_store_geo", "(country, city)")
    save_ch(report_stores_avg_check(facts, stores), "rpt_store_avg_check", "store")

    # --- Витрина 5: Поставщики ---
    log.info("[5/6] Витрина поставщиков")
    save_ch(report_suppliers_top(facts, suppliers), "rpt_supplier_top5", "supplier")
    save_ch(report_suppliers_avg_price(facts, suppliers, products), "rpt_supplier_avg_price", "supplier")
    save_ch(report_suppliers_countries(facts, suppliers), "rpt_supplier_countries", "supplier_country")

    # --- Витрина 6: Качество ---
    log.info("[6/6] Витрина качества")
    save_ch(report_quality_extremes(facts, products), "rpt_quality_extremes", "product")
    save_ch(report_quality_correlation(facts, products), "rpt_quality_corr", "product")
    save_ch(report_quality_most_reviewed(facts, products), "rpt_quality_reviews", "product")

    log.info("=== Все 18 отчётов записаны в ClickHouse ===")
    spark.stop()


if __name__ == "__main__":
    run()

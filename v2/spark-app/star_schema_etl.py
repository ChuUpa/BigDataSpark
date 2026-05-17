"""
Читаем плоскую таблицу, разбиваем на измерения и факт.
"""
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

sys.path.insert(0, "/opt/app")
from config import PostgresConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("star_schema_etl")


# --- helpers ---

def get_spark():
    return (
        SparkSession.builder
        .appName("Lab2_StarSchema")
        .config("spark.jars", "/opt/drivers/pg-jdbc-42.7.1.jar")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )


def load_source(spark):
    """Читаем плоскую таблицу mock_data"""
    opts = PostgresConfig.jdbc_options()
    return (
        spark.read.format("jdbc")
        .option("url", opts["url"])
        .option("dbtable", "mock_data")
        .option("user", opts["user"])
        .option("password", opts["password"])
        .option("driver", opts["driver"])
        .load()
    )


def persist_to_pg(df, table):
    """Пишем в PostgreSQL с overwrite"""
    opts = PostgresConfig.jdbc_options()
    (
        df.write.format("jdbc")
        .option("url", opts["url"])
        .option("dbtable", table)
        .option("user", opts["user"])
        .option("password", opts["password"])
        .option("driver", opts["driver"])
        .mode("overwrite")
        .save()
    )
    logger.info(f"  -> {table} OK")


# --- dimensions ---

def create_dim_customers(raw):
    return (
        raw
        .select(
            F.col("sale_customer_id"),
            F.col("customer_first_name"),
            F.col("customer_last_name"),
            F.col("customer_age"),
            F.col("customer_email"),
            F.col("customer_country"),
            F.col("customer_postal_code"),
            F.col("customer_pet_type"),
            F.col("customer_pet_name"),
            F.col("customer_pet_breed"),
            F.col("pet_category"),
        )
        .dropDuplicates(["sale_customer_id"])
        .withColumn("dim_id", F.monotonically_increasing_id().cast(LongType()))
        .select(
            F.col("dim_id").alias("customer_key"),
            F.col("sale_customer_id").alias("customer_nk"),
            F.col("customer_first_name").alias("fname"),
            F.col("customer_last_name").alias("lname"),
            F.col("customer_age").alias("age"),
            F.col("customer_email").alias("email"),
            F.col("customer_country").alias("country"),
            F.col("customer_postal_code").alias("zip_code"),
            F.col("customer_pet_type").alias("pet_type"),
            F.col("customer_pet_name").alias("pet_name"),
            F.col("customer_pet_breed").alias("pet_breed"),
            F.col("pet_category").alias("pet_cat"),
        )
    )


def create_dim_sellers(raw):
    return (
        raw
        .select(
            "sale_seller_id", "seller_first_name", "seller_last_name",
            "seller_email", "seller_country", "seller_postal_code",
        )
        .dropDuplicates(["sale_seller_id"])
        .withColumn("seller_key", F.monotonically_increasing_id().cast(LongType()))
        .select(
            "seller_key",
            F.col("sale_seller_id").alias("seller_nk"),
            F.col("seller_first_name").alias("fname"),
            F.col("seller_last_name").alias("lname"),
            F.col("seller_email").alias("email"),
            F.col("seller_country").alias("country"),
            F.col("seller_postal_code").alias("zip_code"),
        )
    )


def create_dim_products(raw):
    return (
        raw
        .select(
            "sale_product_id", "product_name", "product_category",
            "product_price", "product_weight", "product_color",
            "product_size", "product_brand", "product_material",
            "product_description", "product_rating", "product_reviews",
            "product_release_date", "product_expiry_date",
        )
        .dropDuplicates(["sale_product_id"])
        .withColumn("product_key", F.monotonically_increasing_id().cast(LongType()))
        .select(
            "product_key",
            F.col("sale_product_id").alias("product_nk"),
            F.col("product_name").alias("title"),
            F.col("product_category").alias("category"),
            F.col("product_price").alias("unit_price"),
            F.col("product_weight").alias("weight_kg"),
            F.col("product_color").alias("color"),
            F.col("product_size").alias("size"),
            F.col("product_brand").alias("brand"),
            F.col("product_material").alias("material"),
            F.col("product_description").alias("descr"),
            F.col("product_rating").alias("rating"),
            F.col("product_reviews").alias("num_reviews"),
            F.col("product_release_date").alias("released"),
            F.col("product_expiry_date").alias("expires"),
        )
    )


def create_dim_stores(raw):
    return (
        raw
        .select(
            "store_name", "store_location", "store_city",
            "store_state", "store_country", "store_phone", "store_email",
        )
        .dropDuplicates(["store_name", "store_city", "store_country"])
        .withColumn("store_key", F.monotonically_increasing_id().cast(LongType()))
        .select(
            "store_key",
            F.col("store_name").alias("name"),
            F.col("store_location").alias("address"),
            F.col("store_city").alias("city"),
            F.col("store_state").alias("state"),
            F.col("store_country").alias("country"),
            F.col("store_phone").alias("phone"),
            F.col("store_email").alias("email"),
        )
    )


def create_dim_suppliers(raw):
    return (
        raw
        .select(
            "supplier_name", "supplier_contact", "supplier_email",
            "supplier_phone", "supplier_address", "supplier_city", "supplier_country",
        )
        .dropDuplicates(["supplier_name", "supplier_email"])
        .withColumn("supplier_key", F.monotonically_increasing_id().cast(LongType()))
        .select(
            "supplier_key",
            F.col("supplier_name").alias("name"),
            F.col("supplier_contact").alias("contact_person"),
            F.col("supplier_email").alias("email"),
            F.col("supplier_phone").alias("phone"),
            F.col("supplier_address").alias("address"),
            F.col("supplier_city").alias("city"),
            F.col("supplier_country").alias("country"),
        )
    )


# --- fact table ---

def create_fact_table(raw, dim_cust, dim_sell, dim_prod, dim_store, dim_supp):
    """Собираем факт-таблицу, джойним все измерения по натуральным ключам"""

    enriched = raw.withColumn("parsed_date", F.to_date(F.col("sale_date"), "M/d/yyyy"))

    result = (
        enriched
        .join(
            dim_cust.select("customer_key", "customer_nk"),
            enriched["sale_customer_id"] == dim_cust["customer_nk"],
            "left",
        )
        .join(
            dim_sell.select("seller_key", "seller_nk"),
            enriched["sale_seller_id"] == dim_sell["seller_nk"],
            "left",
        )
        .join(
            dim_prod.select("product_key", "product_nk"),
            enriched["sale_product_id"] == dim_prod["product_nk"],
            "left",
        )
        .join(
            dim_store.select(
                F.col("store_key"),
                F.col("name").alias("_st_name"),
                F.col("city").alias("_st_city"),
                F.col("country").alias("_st_country"),
            ),
            (enriched["store_name"] == F.col("_st_name"))
            & (enriched["store_city"] == F.col("_st_city"))
            & (enriched["store_country"] == F.col("_st_country")),
            "left",
        )
        .join(
            dim_supp.select(
                F.col("supplier_key"),
                F.col("name").alias("_sup_name"),
                F.col("email").alias("_sup_email"),
            ),
            (enriched["supplier_name"] == F.col("_sup_name"))
            & (enriched["supplier_email"] == F.col("_sup_email")),
            "left",
        )
    )

    return result.select(
        F.monotonically_increasing_id().cast(LongType()).alias("fact_id"),
        "customer_key",
        "seller_key",
        "product_key",
        "store_key",
        "supplier_key",
        F.col("parsed_date").alias("sale_date"),
        F.col("sale_quantity").alias("qty"),
        F.col("sale_total_price").alias("revenue"),
        F.col("product_price").alias("unit_price"),
    )


# --- entry point ---

def run():
    spark = get_spark()
    logger.info("=== Старт ETL: raw -> star schema ===")

    logger.info("Чтение mock_data из PostgreSQL...")
    raw = load_source(spark)
    logger.info(f"  Загружено {raw.count()} записей")

    logger.info("Формирование dim_customers...")
    dim_cust = create_dim_customers(raw)
    persist_to_pg(dim_cust, "dim_customers")

    logger.info("Формирование dim_sellers...")
    dim_sell = create_dim_sellers(raw)
    persist_to_pg(dim_sell, "dim_sellers")

    logger.info("Формирование dim_products...")
    dim_prod = create_dim_products(raw)
    persist_to_pg(dim_prod, "dim_products")

    logger.info("Формирование dim_stores...")
    dim_store = create_dim_stores(raw)
    persist_to_pg(dim_store, "dim_stores")

    logger.info("Формирование dim_suppliers...")
    dim_supp = create_dim_suppliers(raw)
    persist_to_pg(dim_supp, "dim_suppliers")

    logger.info("Формирование fact_sales...")
    fact = create_fact_table(raw, dim_cust, dim_sell, dim_prod, dim_store, dim_supp)
    persist_to_pg(fact, "fact_sales")

    logger.info("=== ETL star schema завершён успешно ===")
    spark.stop()


if __name__ == "__main__":
    run()

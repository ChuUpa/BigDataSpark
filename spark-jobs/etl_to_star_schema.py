from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, monotonically_increasing_id, to_date
)
from pyspark.sql.types import IntegerType


JDBC_URL = "jdbc:postgresql://postgres:5432/bigdata"
JDBC_PROPERTIES = {
    "user": "spark",
    "password": "spark123",
    "driver": "org.postgresql.Driver",
}


def create_spark_session():
    return (
        SparkSession.builder
        .appName("ETL to Star Schema")
        .config("spark.jars", "/opt/spark-jars/postgresql-42.7.1.jar")
        .config("spark.executor.memory", "512m")
        .config("spark.executor.cores", "1")
        .config("spark.driver.memory", "512m")
        .getOrCreate()
    )


def read_source(spark):
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "mock_data")
        .option("user", JDBC_PROPERTIES["user"])
        .option("password", JDBC_PROPERTIES["password"])
        .option("driver", JDBC_PROPERTIES["driver"])
        .load()
    )


def write_table(df, table_name):
    (
        df.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table_name)
        .option("user", JDBC_PROPERTIES["user"])
        .option("password", JDBC_PROPERTIES["password"])
        .option("driver", JDBC_PROPERTIES["driver"])
        .mode("overwrite")
        .save()
    )


def build_dim_customer(df):
    return (
        df.dropDuplicates(["sale_customer_id"])
        .withColumn("customer_sk", (monotonically_increasing_id() + 1).cast(IntegerType()))
        .select(
            col("customer_sk"),
            col("sale_customer_id").alias("customer_id"),
            col("customer_first_name").alias("first_name"),
            col("customer_last_name").alias("last_name"),
            col("customer_age").alias("age"),
            col("customer_email").alias("email"),
            col("customer_country").alias("country"),
            col("customer_postal_code").alias("postal_code"),
            col("customer_pet_type").alias("pet_type"),
            col("customer_pet_name").alias("pet_name"),
            col("customer_pet_breed").alias("pet_breed"),
            col("pet_category"),
        )
    )


def build_dim_seller(df):
    return (
        df.dropDuplicates(["sale_seller_id"])
        .withColumn("seller_sk", (monotonically_increasing_id() + 1).cast(IntegerType()))
        .select(
            col("seller_sk"),
            col("sale_seller_id").alias("seller_id"),
            col("seller_first_name").alias("first_name"),
            col("seller_last_name").alias("last_name"),
            col("seller_email").alias("email"),
            col("seller_country").alias("country"),
            col("seller_postal_code").alias("postal_code"),
        )
    )


def build_dim_product(df):
    return (
        df.dropDuplicates(["sale_product_id"])
        .withColumn("product_sk", (monotonically_increasing_id() + 1).cast(IntegerType()))
        .select(
            col("product_sk"),
            col("sale_product_id").alias("product_id"),
            col("product_name").alias("name"),
            col("product_category").alias("category"),
            col("product_price").alias("price"),
            col("product_weight").alias("weight"),
            col("product_color").alias("color"),
            col("product_size").alias("size"),
            col("product_brand").alias("brand"),
            col("product_material").alias("material"),
            col("product_description").alias("description"),
            col("product_rating").alias("rating"),
            col("product_reviews").alias("reviews"),
            col("product_release_date").alias("release_date"),
            col("product_expiry_date").alias("expiry_date"),
        )
    )


def build_dim_store(df):
    return (
        df.dropDuplicates(["store_name", "store_city", "store_country"])
        .withColumn("store_sk", (monotonically_increasing_id() + 1).cast(IntegerType()))
        .select(
            col("store_sk"),
            col("store_name"),
            col("store_location"),
            col("store_city"),
            col("store_state"),
            col("store_country"),
            col("store_phone"),
            col("store_email"),
        )
    )


def build_dim_supplier(df):
    return (
        df.dropDuplicates(["supplier_name", "supplier_email"])
        .withColumn("supplier_sk", (monotonically_increasing_id() + 1).cast(IntegerType()))
        .select(
            col("supplier_sk"),
            col("supplier_name"),
            col("supplier_contact"),
            col("supplier_email"),
            col("supplier_phone"),
            col("supplier_address"),
            col("supplier_city"),
            col("supplier_country"),
        )
    )


def build_fact_sales(df, dim_customer, dim_seller, dim_product, dim_store, dim_supplier):
    df_with_date = df.withColumn("sale_date_parsed", to_date(col("sale_date"), "M/d/yyyy"))

    dim_store_j = dim_store.select(
        col("store_sk"),
        col("store_name").alias("ds_store_name"),
        col("store_city").alias("ds_store_city"),
        col("store_country").alias("ds_store_country"),
    )
    dim_supplier_j = dim_supplier.select(
        col("supplier_sk"),
        col("supplier_name").alias("ds_supplier_name"),
        col("supplier_email").alias("ds_supplier_email"),
    )

    fact = (
        df_with_date
        .join(
            dim_customer.select("customer_sk", "customer_id"),
            df_with_date["sale_customer_id"] == col("customer_id"),
            "left",
        )
        .join(
            dim_seller.select("seller_sk", "seller_id"),
            df_with_date["sale_seller_id"] == col("seller_id"),
            "left",
        )
        .join(
            dim_product.select("product_sk", "product_id"),
            df_with_date["sale_product_id"] == col("product_id"),
            "left",
        )
        .join(
            dim_store_j,
            (df_with_date["store_name"] == col("ds_store_name"))
            & (df_with_date["store_city"] == col("ds_store_city"))
            & (df_with_date["store_country"] == col("ds_store_country")),
            "left",
        )
        .join(
            dim_supplier_j,
            (df_with_date["supplier_name"] == col("ds_supplier_name"))
            & (df_with_date["supplier_email"] == col("ds_supplier_email")),
            "left",
        )
    )

    return fact.select(
        (monotonically_increasing_id() + 1).cast(IntegerType()).alias("sale_sk"),
        col("customer_sk"),
        col("seller_sk"),
        col("product_sk"),
        col("store_sk"),
        col("supplier_sk"),
        col("sale_date_parsed").alias("sale_date"),
        col("sale_quantity"),
        col("sale_total_price"),
        col("product_price"),
    )


def main():
    spark = create_spark_session()

    print("Reading source table mock_data...")
    df = read_source(spark)

    print("Building dim_customer...")
    dim_customer = build_dim_customer(df)
    write_table(dim_customer, "dim_customer")

    print("Building dim_seller...")
    dim_seller = build_dim_seller(df)
    write_table(dim_seller, "dim_seller")

    print("Building dim_product...")
    dim_product = build_dim_product(df)
    write_table(dim_product, "dim_product")

    print("Building dim_store...")
    dim_store = build_dim_store(df)
    write_table(dim_store, "dim_store")

    print("Building dim_supplier...")
    dim_supplier = build_dim_supplier(df)
    write_table(dim_supplier, "dim_supplier")

    print("Building fact_sales...")
    fact_sales = build_fact_sales(df, dim_customer, dim_seller, dim_product, dim_store, dim_supplier)
    write_table(fact_sales, "fact_sales")

    print("ETL to star schema completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()

"""
Конфигурация подключений к БД
"""
import os


class PostgresConfig:
    URL = os.getenv("PG_JDBC_URL", "jdbc:postgresql://db:5432/salesdb")
    USER = os.getenv("PG_USER", "etl_user")
    PASSWORD = os.getenv("PG_PASSWORD", "etl_pass_2024")
    DRIVER = "org.postgresql.Driver"

    @classmethod
    def jdbc_options(cls):
        return {
            "url": cls.URL,
            "user": cls.USER,
            "password": cls.PASSWORD,
            "driver": cls.DRIVER,
        }


class ClickHouseConfig:
    URL = os.getenv("CH_JDBC_URL", "jdbc:ch://clickhouse-server:8123/analytics?compress=0")
    USER = os.getenv("CH_USER", "analyst")
    PASSWORD = os.getenv("CH_PASSWORD", "ch_secure_456")
    DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"

    @classmethod
    def jdbc_options(cls):
        return {
            "url": cls.URL,
            "user": cls.USER,
            "password": cls.PASSWORD,
            "driver": cls.DRIVER,
        }

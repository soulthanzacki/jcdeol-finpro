from pyspark.sql import functions as F
import logging

logging.basicConfig(level=logging.INFO)

def table_check(spark):
    logging.info(f"Table Check : dwh.dim_dentists")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dwh.dim_dentists (
            dentist_id INT,
            full_name STRING,
            licence STRING,
            workdays STRING,
            shift STRING,
            load_date DATE,
            end_date DATE,
            is_active INT
            )
            USING ICEBERG;
        """)

def read_data(spark):
    logging.info(f"Reading data from : raw.dentists")
    return spark.sql("SELECT * FROM raw.dentists;")

def insert_data(df):
    logging.info(f"Insert data to : dwh.dim_dentists")
    df.write \
    .format("iceberg") \
    .mode("overwrite") \
    .saveAsTable(f"dwh.dim_dentists")

def transform_dim_dentists(spark):
    table_check(spark)
    df = read_data(spark)
    insert_data(df)
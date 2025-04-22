from pyspark.sql import functions as F
import logging

logging.basicConfig(level=logging.INFO)

def table_check(spark):
    logging.info(f"Table Check : dwh.fact_visits")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dwh.fact_visits (
            visit_id INT,
            patient_id INT,
            dentist_id INT,
            cost INT,
            payment_method STRING,
            visit_day STRING,
            visit_date DATE,
            load_date DATE
            )
            USING ICEBERG;
        """)

def read_data(spark):
    logging.info(f"Reading data from : raw.visits")
    return spark.sql("SELECT * FROM raw.visits")

def insert_data(df):
    logging.info(f"Insert data to : dwh.fact_visits")
    df.write \
    .format("iceberg") \
    .mode("overwrite") \
    .saveAsTable(f"dwh.fact_visits")

def transform_fact_visits(spark):
    table_check(spark)
    df = read_data(spark)
    insert_data(df)
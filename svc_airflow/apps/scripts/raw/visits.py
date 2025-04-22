from pyspark.sql import functions as F
import logging

logging.basicConfig(level=logging.INFO)

def table_check(spark):
    logging.info(f"Table Check : raw.visits")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS raw.visits (
            visit_id INT,
            patient_id INT,
            dentist_id INT,
            cost INT,
            payment_method STRING,
            visit_day STRING,
            visit_date DATE,
            load_date DATE
            )
            USING ICEBERG
            PARTITIONED BY (load_date);
        """)

def read_data(spark, load_date):
    logging.info(f"Reading data from : staging.visits")
    return spark.sql(f"""
        SELECT
            s.visit_id,
            s.patient_id,
            s.dentist_id,
            s.cost,
            s.payment_method,
            s.visit_day,
            s.visit_date,
            DATE('{load_date}') AS load_date
        FROM staging.visits AS s;""")

def insert_data(df):
    logging.info(f"Insert data to : raw.visits")
    df.write.partitionBy("load_date") \
                    .format("iceberg") \
                    .mode("append") \
                    .saveAsTable(f"raw.visits")

def transform_visits(spark, load_date):
    table_check(spark)
    df = read_data(spark, load_date)
    insert_data(df)
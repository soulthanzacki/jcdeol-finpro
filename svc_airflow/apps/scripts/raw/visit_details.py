from pyspark.sql import functions as F
import logging

logging.basicConfig(level=logging.INFO)

def table_check(spark):
    logging.info(f"Table Check : raw.visit_details")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS raw.visit_details (
            visit_id INT,
            diagnose STRING,
            treatment STRING,
            follow_up_required INT,
            note STRING,
            load_date DATE
            )
            USING ICEBERG
            PARTITIONED BY (load_date);
        """)

def read_data(spark, load_date):
    logging.info(f"Reading data from : staging.visit_details")
    return spark.sql(f"""
        SELECT
            s.visit_id,
            s.diagnose,
            s.treatment,
            s.follow_up_required,
            s.note,
            DATE('{load_date}') AS load_date
        FROM staging.visit_details AS s;""")

def insert_data(df):
    logging.info(f"Insert data to : raw.visit_details")
    df.write.partitionBy("load_date") \
                    .format("iceberg") \
                    .mode("append") \
                    .saveAsTable(f"raw.visit_details")

def transform_visit_details(spark, load_date):
    table_check(spark)
    df = read_data(spark, load_date)
    insert_data(df)
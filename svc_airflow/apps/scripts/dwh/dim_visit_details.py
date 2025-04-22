from pyspark.sql import functions as F
import logging

logging.basicConfig(level=logging.INFO)

def table_check(spark):
    logging.info(f"Table Check : dwh.dim_visit_details")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dwh.dim_visit_details (
            visit_id INT,
            diagnose STRING,
            treatment STRING,
            follow_up_required INT,
            note STRING,
            load_date DATE
            )
            USING ICEBERG;
        WHERE NOT EXISTS (
            SELECT 1 
            FROM raw.visit_details AS t
            WHERE t.visit_id = s.visit_id
              AND t.diagnose = s.diagnose
              AND t.treatment = s.treatment
              AND t.follow_up_required = s.follow_up_required
              AND t.note = s.note
            ) AND EXISTS (
            SELECT 1 
            FROM raw.visits AS t
            WHERE t.visit_id = s.visit_id
            )
        """)

def read_data(spark):
    logging.info(f"Reading data from : raw.visit_details")
    return spark.sql("SELECT * FROM raw.visit_details")

def insert_data(df):
    logging.info(f"Insert data to : dim_visit_details")
    df.write \
    .format("iceberg") \
    .mode("overwrite") \
    .saveAsTable(f"dwh.dim_visit_details")

def transform_dim_visit_details(spark):
    table_check(spark)
    df = read_data(spark)
    insert_data(df)
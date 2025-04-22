from pyspark.sql import functions as F
import logging

logging.basicConfig(level=logging.INFO)

def table_check(spark):
    logging.info(f"Table Check : raw.dentists")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS raw.dentists (
            dentist_id INT,
            full_name STRING,
            licence STRING,
            workdays STRING,
            shift STRING,
            load_date DATE
            )
            USING ICEBERG
            PARTITIONED BY (load_date);
        """)

def read_data(spark, load_date):
    logging.info(f"Reading data from : staging.dentists")
    return spark.sql(f"""
        SELECT
            s.dentist_id,
            CONCAT(s.first_name, ' ', s.last_name) AS full_name,
            s.licence,
            s.workdays,
            s.shift,
            DATE('{load_date}') AS load_date
        FROM staging.dentists AS s;""")

def insert_data(df):
    logging.info(f"Insert data to : raw.dentists")
    df.write.partitionBy("load_date") \
                    .format("iceberg") \
                    .mode("append") \
                    .saveAsTable(f"raw.dentists")

def transform_dentists(spark, load_date):
    table_check(spark)
    df = read_data(spark, load_date)
    insert_data(df)
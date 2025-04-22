from pyspark.sql import functions as F
import logging

logging.basicConfig(level=logging.INFO)

def table_check(spark):
    logging.info(f"Table Check : raw.patients")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS raw.patients (
            patient_id INT,
            complaint STRING,
            fullname STRING,
            date_of_birth DATE,
            address STRING,
            phone STRING,
            load_date DATE
            )
            USING ICEBERG
            PARTITIONED BY (load_date);
        """)

def read_data(spark, load_date):
    logging.info(f"Reading data from : staging.patients")
    return spark.sql(f"""
        SELECT
            s.patient_id,
            s.complaint,
            CONCAT(s.first_name, ' ', s.last_name) AS fullname,
            s.date_of_birth,
            s.address,
            s.phone,
            DATE('{load_date}') AS load_date
        FROM staging.patients AS s;""")

def insert_data(df):
    logging.info(f"Insert data to : raw.patients")
    df.write.partitionBy("load_date") \
                    .format("iceberg") \
                    .mode("append") \
                    .saveAsTable(f"raw.patients")

def transform_patients(spark, load_date):
    table_check(spark)
    df = read_data(spark, load_date)
    insert_data(df)
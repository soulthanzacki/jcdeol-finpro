from pyspark.sql import functions as F
import logging

logging.basicConfig(level=logging.INFO)

def table_check(spark):
    logging.info(f"Table Check : dwh.dim_patients")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dwh.dim_patients (
            patient_id INT,
            complaint STRING,
            fullname STRING,
            date_of_birth DATE,
            address STRING,
            phone STRING,
            load_date DATE,
            end_date DATE,
            is_active
            )
            USING ICEBERG;
        FROM staging.patients AS s
        WHERE NOT EXISTS (
            SELECT 1 
            FROM raw.patients AS t
            WHERE t.patient_id = s.patient_id
              AND t.complaint = s.complaint
              AND t.fullname = CONCAT(s.first_name, ' ', s.last_name)
              AND t.date_of_birth = s.date_of_birth
              AND t.address = s.address
              AND t.phone = s.phone
            )
        """)

def read_data(spark):
    logging.info(f"Reading data from : raw.patients")
    return spark.sql("SELECT * FROM raw.patients")

def insert_data(df):
    logging.info(f"Insert data to : dwh.dim_patients")
    df.write \
    .format("iceberg") \
    .mode("overwrite") \
    .saveAsTable(f"dwh.dim_patients")

def transform_dim_patients(spark):
    table_check(spark)
    df = read_data(spark)
    insert_data(df)
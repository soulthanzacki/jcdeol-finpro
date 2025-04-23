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
            is_active INT
            )
            USING ICEBERG;
        """)
def update_data(spark, load_date):
    return spark.sql(f"""
        MERGE INTO dwh.dim_patients AS target
        USING (
        SELECT
            p.patient_id,
            p.complaint,
            p.fullname,
            p.date_of_birth,
            p.address,
            p.phone
        FROM raw.patients p
        WHERE p.load_date = '{load_date}'
        ) AS source
        ON target.patient_id = source.patient_id
        AND target.is_active = 1
        
        WHEN MATCHED AND (
            target.complaint != source.complaint OR
            target.fullname != source.fullname OR
            target.date_of_birth != source.date_of_birth OR
            target.address != source.address OR
            target.phone != source.phone
        ) THEN
        UPDATE SET
            target.end_date = DATE('{load_date}'),
            target.is_active = 0;
            """)

def read_data(spark, load_date):
    logging.info(f"Reading data from : raw.patients")
    return spark.sql(f""" 
        SELECT
            s.patient_id,
            s.complaint,
            s.fullname,
            s.date_of_birth,
            s.address,
            s.phone,
            s.load_date,
            NULL AS end_date,
            1 AS is_active
        FROM raw.patients s
        WHERE load_date = '{load_date}'
        AND NOT EXISTS (
            SELECT 1
            FROM dwh.dim_patients AS t
            WHERE t.patient_id = s.patient_id
                AND t.complaint = s.complaint
                AND t.fullname = s.fullname
                AND t.date_of_birth = s.date_of_birth
                AND t.address = s.address
                AND t.phone = s.phone
                AND t.is_active = 1
        );
        """)

def insert_data(df):
    logging.info(f"Insert data to : dwh.dim_patients")
    df.write \
    .format("iceberg") \
    .mode("append") \
    .saveAsTable(f"dwh.dim_patients")

def transform_dim_patients(spark, load_date):
    table_check(spark)
    update_data(spark, load_date)
    df = read_data(spark, load_date)
    insert_data(df)
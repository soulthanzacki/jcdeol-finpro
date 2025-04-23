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

def update_data(spark, load_date):
    logging.info(f"Update data from : raw.dentists")
    return spark.sql(f"""
        MERGE INTO dwh.dim_dentists AS target
        USING (
        SELECT
            d.dentist_id,
            d.full_name,
            d.licence,
            d.workdays,
            d.shift
        FROM raw.dentists d
        WHERE d.load_date = '{load_date}'
        ) AS source
        ON target.dentist_id = source.dentist_id
        AND target.is_active = 1
        
        WHEN MATCHED AND (
            target.full_name != source.full_name OR
            target.licence != source.licence OR
            target.workdays != source.workdays OR
            target.shift != source.shift
        ) THEN
        UPDATE SET
            target.end_date = DATE('{load_date}'),
            target.is_active = 0;
        """)

def read_data(spark, load_date):
    logging.info(f"Reading data from : raw.dentists")
    return spark.sql(f"""
        SELECT
            s.dentist_id,
            s.full_name,
            s.licence,
            s.workdays,
            s.shift,
            s.load_date,
            NULL AS end_date,
            1 AS is_active
        FROM raw.dentists s
        WHERE load_date = '{load_date}'
        AND NOT EXISTS (
            SELECT 1
            FROM dwh.dim_dentists AS t
            WHERE t.dentist_id = s.dentist_id
                AND t.full_name = s.full_name
                AND t.licence = s.licence
                AND t.workdays = s.workdays
                AND t.shift = s.shift
                AND t.is_active = 1
        );
        """)

def insert_data(df):
    logging.info(f"Insert data to : dwh.dim_dentists")
    df.write \
    .format("iceberg") \
    .mode("append") \
    .saveAsTable(f"dwh.dim_dentists")

def transform_dim_dentists(spark, load_date):
    table_check(spark)
    update_data(spark, load_date)
    df = read_data(spark, load_date)
    insert_data(df)
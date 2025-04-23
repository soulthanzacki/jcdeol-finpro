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

def read_data(spark, load_date, data_date):
    logging.info(f"Reading data from : raw.visits")
    return spark.sql(f"""
                SELECT 
                    s.visit_id,
                    s.patient_id,
                    s.dentist_id,
                    s.cost,
                    s.payment_method,
                    s.visit_day,
                    s.visit_date,
                    s.load_date
                FROM raw.visits s
                WHERE load_date = '{load_date}'
                AND visit_date = '{data_date}'
                AND NOT EXISTS (
                    SELECT 1
                    FROM dwh.fact_visits AS t
                    WHERE t.visit_id = s.visit_id
                        AND t.patient_id = s.patient_id
                        AND t.dentist_id = s.dentist_id
                        AND t.cost = s.cost
                        AND t.payment_method = s.payment_method
                        AND t.visit_day = s.visit_day
                        AND t.visit_date = s.visit_date
                );
                """)

def insert_data(df):
    logging.info(f"Insert data to : dwh.fact_visits")
    df.write \
    .format("iceberg") \
    .mode("append") \
    .saveAsTable(f"dwh.fact_visits")

def transform_fact_visits(spark, load_date, data_date):
    table_check(spark)
    df = read_data(spark, load_date, data_date)
    insert_data(df)
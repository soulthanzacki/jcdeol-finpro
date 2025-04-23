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
        """)

def read_data(spark, load_date):
    logging.info(f"Reading data from : raw.visit_details")
    return spark.sql(f"""
                SELECT 
                    s.visit_id,
                    s.diagnose,
                    s.treatment,
                    s.follow_up_required,
                    s.note,
                    s.load_date
                FROM raw.visit_details s
                WHERE s.load_date = '{load_date}'
                AND NOT EXISTS (
                    SELECT 1
                    FROM dwh.dim_visit_details AS t
                    WHERE t.visit_id = s.visit_id
                        AND t.diagnose = s.diagnose
                        AND t.treatment = s.treatment
                        AND t.follow_up_required = s.follow_up_required
                ) AND EXISTS (
                    SELECT 1
                    FROM dwh.fact_visits AS t
                    WHERE t.visit_id = s.visit_id
                );
                """)

def insert_data(df):
    logging.info(f"Insert data to : dim_visit_details")
    df.write \
    .format("iceberg") \
    .mode("append") \
    .saveAsTable(f"dwh.dim_visit_details")

def transform_dim_visit_details(spark, load_date):
    table_check(spark)
    df = read_data(spark, load_date)
    insert_data(df)
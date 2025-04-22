from pyspark.sql import SparkSession
import logging
from datetime import datetime

from scripts.raw.dentists import transform_dentists
from scripts.raw.patients import transform_patients
from scripts.raw.visits import transform_visits
from scripts.raw.visit_details import transform_visit_details

from scripts.dwh.dim_dentists import transform_dim_dentists
from scripts.dwh.dim_patients import transform_dim_patients
from scripts.dwh.fact_visits import transform_fact_visits
from scripts.dwh.dim_visit_details import transform_dim_visit_details

class Session():
    def __init__(self):
        self.spark = SparkSession.builder \
            .getOrCreate()
            # .appName("Transform") \
            # .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            # .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            # .config("spark.hadoop.fs.s3a.access.key", "admin") \
            # .config("spark.hadoop.fs.s3a.secret.key", "password") \
            # .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            # .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            # .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            # .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            # .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            # .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            # .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            # .config("spark.sql.catalog.spark_catalog.type", "hive") \
            # .config("spark.sql.catalog.spark_catalog.uri", "thrift://hive-metastore:9083") \
            # .config("spark.sql.warehouse.dir", "s3a://lakehouse/") \
            # .enableHiveSupport() \
            # .getOrCreate()

class Raw(Session):
    def __init__(self, load_date):
        super().__init__()
        self.load_date = load_date
        # self.data_date = data_date

        self.spark.sql("CREATE DATABASE IF NOT EXISTS raw")

    def execute(self):
        logging.info(f"Transforming dentists data")
        transform_dentists(self.spark, self.load_date)

        logging.info(f"Transforming patients data")
        transform_patients(self.spark, self.load_date)

        logging.info(f"Transforming visits data")
        transform_visits(self.spark, self.load_date)

        logging.info(f"Transforming visit_details data")
        transform_visit_details(self.spark, self.load_date)

class DWH(Session):
    def __init__(self):
        super().__init__()

        self.spark.sql("CREATE DATABASE IF NOT EXISTS dwh")

    def execute(self):
        logging.info(f"Transforming dentists data")
        transform_dim_dentists(self.spark)

    #     logging.info(f"Transforming patients data")
    #     transform_dim_patients(self.spark)

    #     logging.info(f"Transforming visits data")
    #     transform_fact_visits(self.spark)

    #     logging.info(f"Transforming visit_details data")
    #     transform_dim_visit_details(self.spark)

class Mart(Session):
    pass
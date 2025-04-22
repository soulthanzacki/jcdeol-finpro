# Extract and Load for staging

from pyspark.sql import SparkSession
import logging

class Session():
    def __init__(self):
        self.spark = SparkSession.builder \
            .getOrCreate()
            # .appName("Extract-Load") \
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
            # .config("spark.sql.catalog.type", "hive") \
            # .config("spark.sql.catalog.uri", "thrift://hive-metastore:9083") \
            # .config("spark.sql.warehouse.dir", "s3a://lakehouse/") \
            # .enableHiveSupport() \
            
        self.spark.sql("CREATE DATABASE IF NOT EXISTS staging;")

class Extract(Session):
    def __init__(self):
        super().__init__()
        logging.basicConfig(level=logging.INFO)

    def read_data(self, table_name):
        url = "jdbc:postgresql://db-main:5433/dental_clinic"
        dbtable = table_name
        db_properties = {
            "driver": "org.postgresql.Driver",
            "user": "postgres",
            "password": "postgres"
        }
        return self.spark.read.jdbc(url=url, table=dbtable, properties=db_properties)

    def write_data(self, df, table_name):
        df.write \
          .format("iceberg") \
          .mode("overwrite") \
          .saveAsTable(f"staging.{table_name}")

    def main(self):
        tables = ['dentists', 'patients', 'visits', 'visit_details']

        for table_name in tables:
            logging.info(f"Reading source data from table {table_name}")
            df = self.read_data(table_name)

            logging.info(f"Writing to spark staging.{table_name} as Iceberg table")
            self.write_data(df, table_name)

            logging.info(f"Finished processing table {table_name}")
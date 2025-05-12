from pyspark.sql import SparkSession
from config import AppConfig

class SparkManager:
    @staticmethod
    def create_session(config: AppConfig):
        return SparkSession.builder \
            .appName("S3StreamProcessor") \
            .config("spark.jars.packages", 
                    "org.apache.hadoop:hadoop-aws:3.3.6,"
                    "com.amazonaws:aws-java-sdk-bundle:1.12.367") \
            .config("spark.hadoop.fs.s3a.access.key", config.AWS_ACCESS_KEY_ID) \
            .config("spark.hadoop.fs.s3a.secret.key", config.AWS_SECRET_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{config.AWS_REGION}.amazonaws.com") \
            .config("spark.sql.warehouse.dir", "file:///C:/spark-warehouse") \
            .config("spark.sql.streaming.checkpointLocation", "C:/spark-checkpoints") \
            .getOrCreate()

    @staticmethod
    def shutdown_session(spark: SparkSession):
        try:
            spark.stop()
        except Exception:
            pass
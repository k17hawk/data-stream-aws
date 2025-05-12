from pyspark.sql import SparkSession
from config.settings import settings
from logger import logger

def create_spark_session():
    logger.info("Starting Spark session...")
    spark = SparkSession.builder \
        .appName("S3StreamProcessor") \
        .config("spark.jars.packages", 
                "org.apache.hadoop:hadoop-aws:3.3.6,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.367") \
        .config("spark.hadoop.fs.s3a.access.key", settings.AWS_ACCESS_KEY_ID) \
        .config("spark.hadoop.fs.s3a.secret.key", settings.AWS_SECRET_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{settings.AWS_REGION}.amazonaws.com") \
        .config("spark.sql.warehouse.dir", "file:///C:/spark-warehouse") \
        .config("spark.sql.streaming.checkpointLocation", "C:/spark-checkpoints") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def shutdown_spark_session(spark):
    try:
        logger.info("Shutting down Spark session...")
        spark.stop()
    except Exception as e:
        logger.error(f"Error shutting down Spark: {str(e)}")

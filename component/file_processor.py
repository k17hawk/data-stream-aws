from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime
from entity import S3Event
from config import AppConfig
import os
import time

class FileProcessor:
    @staticmethod
    def process(spark: SparkSession, event: S3Event, config: AppConfig) -> bool:
        try:
            s3_path = f"s3a://{event.bucket}/{event.key}"
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(s3_path)

            processed_df = df.withColumn("_processing_timestamp", lit(datetime.now())) \
                             .withColumn("_source_file", lit(event.filename))

            local_path = os.path.join(config.LOCAL_OUTPUT_DIR, f"{event.base_name}_{int(time.time())}")
            processed_df.write.mode("overwrite").parquet(local_path)
            return True
        except Exception as e:
            print(f"ERROR processing file: {str(e)}")
            return False
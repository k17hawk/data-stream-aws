import os
import time
from datetime import datetime
from pyspark.sql.functions import lit
from logger import logger
from config.settings import settings

def process_file(spark, bucket: str, key: str) -> bool:
    try:
        filename = key.split("/")[-1]
        base_name = os.path.splitext(filename)[0]
        timestamp = int(time.time())

        s3_path = f"s3a://{bucket}/{key}"
        logger.info(f"Reading from {s3_path}")

        df = spark.read.option("header", "true").option("inferSchema", "true").csv(s3_path)
        df = df.withColumn("_processing_timestamp", lit(datetime.now())) \
               .withColumn("_source_file", lit(filename))
        
        local_path = os.path.join(settings.LOCAL_OUTPUT_DIR, f"{base_name}_{timestamp}")
        
        df.write.mode("overwrite").parquet(local_path)
        spark.catalog.clearCache() 
        time.sleep(2) 
        print(f"File processed and saved at: {local_path}")
        logger.info(f"File processed and saved at: {local_path}")
        return True
    except Exception as e:
        logger.error(f"Error processing file {key}: {str(e)}")
        return False

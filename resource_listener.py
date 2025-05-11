import os
import sys
import json
import time
from datetime import datetime
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def validate_environment():
    required_vars = {
        'AWS_ACCESS_KEY_ID': os.getenv('AWS_ACCESS_KEY_ID'),
        'AWS_SECRET_ACCESS_KEY': os.getenv('AWS_SECRET_ACCESS_KEY'),
        'AWS_REGION': os.getenv('AWS_REGION', 'us-east-2'),
        'SQS_QUEUE_URL': "https://sqs.us-east-2.amazonaws.com/437878371411/s3-events-queue",
        'LOCAL_OUTPUT_DIR': os.getenv('LOCAL_OUTPUT_DIR', 'tmp')
    }
    
    os.makedirs(required_vars['LOCAL_OUTPUT_DIR'], exist_ok=True)
    
    missing = [k for k, v in required_vars.items() if not v and k != 'LOCAL_OUTPUT_DIR']
    if missing:
        print(f"ERROR: Missing required environment variables: {missing}")
        sys.exit(1)
    
    print("Environment variables validated successfully")
    return required_vars

def create_spark_session(config):
    print("\nInitializing Spark session...")
    
    spark = SparkSession.builder \
        .appName("S3StreamProcessor") \
        .config("spark.jars.packages", 
                "org.apache.hadoop:hadoop-aws:3.3.6,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.367") \
        .config("spark.hadoop.fs.s3a.access.key", config['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.secret.key", config['AWS_SECRET_ACCESS_KEY']) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{config['AWS_REGION']}.amazonaws.com") \
        .config("spark.sql.warehouse.dir", "file:///C:/spark-warehouse") \
        .config("spark.sql.streaming.checkpointLocation", "C:/spark-checkpoints") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark

def process_file(spark, bucket, key, local_output_dir):
    try:
        filename = key.split('/')[-1]
        base_name = os.path.splitext(filename)[0]
        timestamp = int(time.time())
        
        s3_path = f"s3a://{bucket}/{key}"
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(s3_path)

        processed_df = df.withColumn("_processing_timestamp", lit(datetime.now())) \
                         .withColumn("_source_file", lit(filename))

        local_path = os.path.join(local_output_dir, f"{base_name}_{timestamp}")
        processed_df.write.mode("overwrite").parquet(local_path)
        return True

    except Exception as e:
        print(f"ERROR processing file: {str(e)}")
        return False

def graceful_spark_shutdown(spark):
    try:
        spark.stop()
    except Exception:
        pass

def process_messages(config, sqs_client):
    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=config['SQS_QUEUE_URL'],
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20
            )
            
            if 'Messages' in response:
                spark = None
                try:
                    spark = create_spark_session(config)
                    
                    for message in response['Messages']:
                        body = json.loads(message['Body'])
                        
                        if 'bucket' in body and 'key' in body:
                            bucket = body['bucket']
                            key = body['key']
                            process_file(spark, bucket, key, config['LOCAL_OUTPUT_DIR'])
                        
                        sqs_client.delete_message(
                            QueueUrl=config['SQS_QUEUE_URL'],
                            ReceiptHandle=message['ReceiptHandle']
                        )
                
                finally:
                    if spark:
                        graceful_spark_shutdown(spark)
            
            else:
                time.sleep(5)
                
        except Exception as e:
            print(f"ERROR in polling loop: {str(e)}")
            time.sleep(10)

def main():
    config = validate_environment()
    
    sqs_client = boto3.client(
        'sqs',
        region_name=config['AWS_REGION'],
        aws_access_key_id=config['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=config['AWS_SECRET_ACCESS_KEY']
    )
    
    try:
        process_messages(config, sqs_client)
    except KeyboardInterrupt:
        print("\nShutdown requested")

if __name__ == "__main__":
    main()
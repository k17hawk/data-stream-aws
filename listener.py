import os
import sys
from pyspark.sql import SparkSession
import boto3
import json
import time
from pyspark.sql.functions import *
from datetime import datetime

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
    print(f"Local output will be saved to: {os.path.abspath(required_vars['LOCAL_OUTPUT_DIR'])}")
    return required_vars

def create_spark_session(config):
    print("Initializing Spark session...")

    os.environ['HADOOP_HOME'] = 'C:\\hadoop-3.3.6'
    os.environ['PATH'] = f"{os.environ['PATH']};C:\\hadoop-3.3.6\\bin"

    os.makedirs("C:/spark-warehouse", exist_ok=True)
    os.makedirs("C:/spark-temp", exist_ok=True)
    os.makedirs("C:/tmp/hive", exist_ok=True)

    spark = SparkSession.builder \
        .appName("S3StreamProcessor") \
        .config("spark.jars.packages", 
                "org.apache.hadoop:hadoop-aws:3.3.6,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.367") \
        .config("spark.hadoop.fs.s3a.access.key", config['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.secret.key", config['AWS_SECRET_ACCESS_KEY']) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{config['AWS_REGION']}.amazonaws.com") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.sql.warehouse.dir", "file:///C:/spark-warehouse") \
        .config("spark.local.dir", "C:/spark-temp") \
        .config("spark.sql.streaming.checkpointLocation", "C:/spark-checkpoints") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    try:
        os.system("C:\\hadoop-3.3.6\\bin\\winutils.exe chmod 777 C:\\spark-warehouse")
        os.system("C:\\hadoop-3.3.6\\bin\\winutils.exe chmod 777 C:\\spark-temp")
        os.system("C:\\hadoop-3.3.6\\bin\\winutils.exe chmod 777 C:\\tmp\\hive")
    except Exception as e:
        print(f"Warning: Could not set permissions via winutils: {str(e)}")
    
    print("\n=== Environment Verification ===")
    print(f"Spark Version: {spark.version}")
    print(f"Hadoop Version: {spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}")
    
    try:
        s3a_class = spark._jvm.org.apache.hadoop.fs.s3a.S3AFileSystem
        print("S3AFileSystem loaded successfully")
    except Exception as e:
        print(f"Could not access S3AFileSystem: {str(e)}")
    
    print("===============================\n")
    spark.sparkContext.setLogLevel("WARN")
    return spark


def process_file(spark, bucket, key, local_output_dir):
    """Process and save file with detailed logging"""
    try:
        print(f"\nProcessing file: s3://{bucket}/{key}")
        
        # Extract file info
        filename = key.split('/')[-1]
        file_format = filename.split('.')[-1].lower()
        base_name = filename.split('.')[0]
        timestamp = int(time.time())
        
        # Supported formats
        if file_format not in ['csv', 'parquet', 'json']:
            print(f"Unsupported file format: {file_format}")
            return False


        s3_path = f"s3a://{bucket}/{key}"
        print(f"Reading from S3: {s3_path}")
        df = spark.read.format(file_format).load(s3_path)
        
        print(f"Schema:")
        df.printSchema()
        print(f"Row count: {df.count()}")
        
        processed_df = df.withColumn("_processing_timestamp", lit(datetime.now())) \
                       .withColumn("_source_file", lit(filename))
        
        local_path = f"{local_output_dir}/{base_name}_{timestamp}"
        print(f"Saving locally to: {local_path}")
        processed_df.write.mode("overwrite").parquet(local_path)
        
        # Verify local save
        if os.path.exists(local_path):
            print(f"Successfully saved {len(os.listdir(local_path))} files to {local_path}")
            return True
        else:
            print("ERROR: Local output directory not created!")
            return False
            
    except Exception as e:
        print(f"ERROR processing file: {str(e)}")
        return False
def process_file(spark, bucket, key, local_output_dir):
    """Process and save CSV file from S3 with detailed logging"""

    try:
        print(f"\nProcessing file: s3://{bucket}/{key}")
        
  
        filename = key.split('/')[-1]
        file_format = filename.split('.')[-1].lower()
        base_name = os.path.splitext(filename)[0]
        timestamp = int(time.time())
    
        if file_format != 'csv':
            print(f"Unsupported file format: {file_format}")
            return False
        s3_path = f"s3a://{bucket}/{key}"
        print(f"Reading from S3: {s3_path}")
        print("\nUsing Hadoop Version:")
        print(spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion())
        print("Using Spark Version:")
        print(spark.version)

        df = spark.read.option("header", "true").option("inferSchema", "true").csv(s3_path)
        print("Schema:")
        df.printSchema()
        print(f"Row count: {df.count()}")

        # Add metadata
        processed_df = df.withColumn("_processing_timestamp", lit(datetime.now())) \
                         .withColumn("_source_file", lit(filename))

        # Save as Parquet
        local_path = os.path.join(local_output_dir, f"{base_name}_{timestamp}")
        print(f"Saving locally to: {local_path}")
        processed_df.write.mode("overwrite").parquet(local_path)

        # Verify output
        if os.path.exists(local_path) and any(os.scandir(local_path)):
            print(f"Successfully saved files to {local_path}")
            return True
        else:
            print("ERROR: Local output directory not created or is empty!")
            return False

    except Exception as e:
        print(f"ERROR processing file: {str(e)}")
        return False


def process_messages(spark, sqs_client, queue_url, local_output_dir):
    print(f"\nStarting SQS polling from: {queue_url}")
    
    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,
                VisibilityTimeout=300
            )
            
            if 'Messages' in response:
                for message in response['Messages']:
                    try:
                        print("\n" + "="*50)
                        print(f"Processing message ID: {message['MessageId']}")
                        
                        body = json.loads(message['Body'])
                        print(f"Message body: {json.dumps(body, indent=2)}")
                
                        if 'bucket' in body and 'key' in body:
                        
                            bucket = body['bucket']
                            key = body['key']
                            if process_file(spark, bucket, key, local_output_dir):
                                print("File processing successful")
                            else:
                                print("File processing failed")
                        elif 'Records' in body:
                
                            for record in body['Records']:
                                bucket = record['s3']['bucket']['name']
                                key = record['s3']['object']['key']
                                if process_file(spark, bucket, key, local_output_dir):
                                    print("File processing successful")
                                else:
                                    print("File processing failed")
                        else:
                            print("Unrecognized message format")
                            continue
                   
                        sqs_client.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        print("Message deleted from queue")
                        
                    except Exception as e:
                        print(f"ERROR processing message: {str(e)}")
            else:
                print("No messages available, waiting...")
                time.sleep(5)
                
        except Exception as e:
            print(f"ERROR in polling loop: {str(e)}")
            time.sleep(10)

def main():
    config = validate_environment()
    spark = create_spark_session(config)
    
    sqs_client = boto3.client(
        'sqs',
        region_name=config['AWS_REGION'],
        aws_access_key_id=config['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=config['AWS_SECRET_ACCESS_KEY']
    )
    
    try:
        process_messages(spark, sqs_client, config['SQS_QUEUE_URL'], config['LOCAL_OUTPUT_DIR'])
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
    finally:
        spark.stop()
        print("Spark session stopped")

if __name__ == "__main__":
    print("Starting S3 Stream Processor")
    main()
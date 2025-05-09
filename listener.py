import os
import sys
from pyspark.sql import SparkSession
import boto3
import json
import time
from pyspark.sql.functions import *

# 1. Environment Validation
def validate_environment():
    required_vars = {
        'AWS_ACCESS_KEY_ID': os.getenv('AWS_ACCESS_KEY_ID'),
        'AWS_SECRET_ACCESS_KEY': os.getenv('AWS_SECRET_ACCESS_KEY'),
        'AWS_REGION': os.getenv('AWS_REGION', 'us-east-2'),
        'SQS_QUEUE_URL': "https://sqs.us-east-2.amazonaws.com/437878371411/s3-events-queue"
    }
    
    missing = [k for k, v in required_vars.items() if not v]
    if missing:
        print(f"ERROR: Missing required environment variables: {missing}")
        sys.exit(1)
    
    print("Environment variables validated successfully")
    return required_vars

# 2. Initialize Spark with proper logging
def create_spark_session(config):
    print("Initializing Spark session...")
    
    spark = SparkSession.builder \
        .appName("S3StreamProcessor") \
        .config("spark.jars.packages", 
               "org.apache.hadoop:hadoop-aws:3.3.6,"
               "com.amazonaws:aws-java-sdk-bundle:1.12.720") \
        .config("spark.hadoop.fs.s3a.access.key", config['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.secret.key", config['AWS_SECRET_ACCESS_KEY']) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{config['AWS_REGION']}.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.ui.showConsoleProgress", "true") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("Spark session initialized successfully")
    return spark

# 3. Main processing loop
def process_messages(spark, sqs_client, queue_url):
    print(f"Starting to poll SQS queue: {queue_url}")
    
    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20,
                VisibilityTimeout=300
            )
            
            if 'Messages' in response:
                for message in response['Messages']:
                    try:
                        body = json.loads(message['Body'])
                        print(f"Processing message: {message['MessageId']}")
                        
                        
                        sqs_client.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        print("Message processed successfully")
                    except Exception as e:
                        print(f"Error processing message: {str(e)}")
            else:
                print("No messages available, waiting...")
                time.sleep(5)
                
        except Exception as e:
            print(f"Error in polling loop: {str(e)}")
            time.sleep(10)

def main():
    # Validate environment first
    config = validate_environment()
    
    # Initialize services
    spark = create_spark_session(config)
    sqs_client = boto3.client(
        'sqs',
        region_name=config['AWS_REGION'],
        aws_access_key_id=config['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=config['AWS_SECRET_ACCESS_KEY']
    )
    
    try:
        process_messages(spark, sqs_client, config['SQS_QUEUE_URL'])
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
    finally:
        spark.stop()
        print("Spark session stopped")

if __name__ == "__main__":
    print("Starting application...")
    main()
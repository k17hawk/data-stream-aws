import os
from dotenv import load_dotenv
import boto3
from pyspark.sql import SparkSession
import json

# Load AWS credentials from .env
load_dotenv()

# Set env vars for boto3 and Spark
os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('AWS_SECRET_ACCESS_KEY')
os.environ['AWS_DEFAULT_REGION'] = os.getenv('AWS_REGION')

# Create Spark session
spark = SparkSession.builder \
    .appName("SQS to Spark") \
    .getOrCreate()

# Spark config for S3 access
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", f"s3.{os.getenv('AWS_REGION')}.amazonaws.com")

# Initialize SQS
sqs = boto3.client('sqs')

QUEUE_URL = 'https://sqs.us-east-2.amazonaws.com/437878371411/s3-events-queue'

def poll_sqs_messages(max_messages=5):
    response = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=max_messages,
        WaitTimeSeconds=5
    )
    return response.get('Messages', [])

messages = poll_sqs_messages()

for message in messages:
    body = json.loads(message['Body'])
    bucket = body['bucket']
    key = body['key']
    s3_path = f"s3a://{bucket}/{key}"

    print(f"Reading file from: {s3_path}")
    df = spark.read.option("header", "true").csv(s3_path)
    df.show()

    # Delete the message
    sqs.delete_message(
        QueueUrl=QUEUE_URL,
        ReceiptHandle=message['ReceiptHandle']
    )

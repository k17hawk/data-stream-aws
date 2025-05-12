import json
import time
import boto3
from logger import logger
from config.settings import settings
from config.constants import SQS_QUEUE_URL
from components.spark_session import create_spark_session, shutdown_spark_session
from components.file_processor import process_file

def process_messages():
    sqs = boto3.client(
        'sqs',
        region_name=settings.AWS_REGION,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
    )
    print("SQS client created")
    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=5
            )
            print("Received response from SQS")

            if 'Messages' in response:
                print("Messages found in SQS response")
                spark = create_spark_session()
                try:
                    for message in response['Messages']:
                        print("Received new SQS message")
                        logger.info("Received new SQS message")
                        body = json.loads(message['Body'])
                        bucket = body.get("bucket")
                        key = body.get("key")
                        
                        if bucket and key:
                            print(f"Processing file: {key} from bucket: {bucket}")
                            logger.info(f"Processing file: {key} from bucket: {bucket}")
                            process_file(spark, bucket, key)

                        sqs.delete_message(
                            QueueUrl=SQS_QUEUE_URL,
                            ReceiptHandle=message["ReceiptHandle"]
                        )
                        print("Message processed and deleted from queue")
                        logger.info("Message processed and deleted from queue")

                finally:
                    shutdown_spark_session(spark)
            else:
                print("No messages received. Sleeping for 5 seconds...")
                logger.info("No messages received. Sleeping for 5 seconds...")
                time.sleep(2)

        except Exception as e:
            logger.error(f"Error in SQS polling: {str(e)}")
            time.sleep(4)

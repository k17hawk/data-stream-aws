import time
import json
import boto3
from config import AppConfig
from entity import S3Event
from typing import Optional

class SQSConsumer:
    def __init__(self, config: AppConfig):
        self.config = config
        self.client = boto3.client(
            'sqs',
            region_name=config.AWS_REGION,
            aws_access_key_id=config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY
        )

    def poll_messages(self) -> Optional[S3Event]:
        response = self.client.receive_message(
            QueueUrl=self.config.SQS_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20
        )
        
        if 'Messages' in response and len(response['Messages']) > 0:
            message = response['Messages'][0]
            body = json.loads(message['Body'])
            
            if 'bucket' in body and 'key' in body:
                return S3Event(
                    bucket=body['bucket'],
                    key=body['key'],
                    receipt_handle=message['ReceiptHandle'] 
                )
        return None

    def acknowledge_message(self, receipt_handle: str):
        self.client.delete_message(
            QueueUrl=self.config.SQS_QUEUE_URL,
            ReceiptHandle=receipt_handle
        )
import os
from dataclasses import dataclass

@dataclass
class AppConfig:
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_REGION: str
    SQS_QUEUE_URL: str
    LOCAL_OUTPUT_DIR: str

def get_config() -> AppConfig:
    return AppConfig(
        AWS_ACCESS_KEY_ID=os.getenv('AWS_ACCESS_KEY_ID'),
        AWS_SECRET_ACCESS_KEY=os.getenv('AWS_SECRET_ACCESS_KEY'),
        AWS_REGION=os.getenv('AWS_REGION', 'us-east-2'),
        SQS_QUEUE_URL="https://sqs.us-east-2.amazonaws.com/437878371411/s3-events-queue",
        LOCAL_OUTPUT_DIR=os.getenv('LOCAL_OUTPUT_DIR', 'tmp')
    )
import json
import boto3

QUEUE_URL = 'https://sqs.us-east-2.amazonaws.com/437878371411/s3-events-queue'

sqs = boto3.client('sqs')

def lambda_handler(event, context):
    try:
        for record in event['Records']:
            bucket_name = record['s3']['bucket']['name']
            object_key = record['s3']['object']['key']
            event_time = record['eventTime']

            message_body = {
                'bucket': bucket_name,
                'key': object_key,
                'eventTime': event_time
            }
            response = sqs.send_message(
                QueueUrl=QUEUE_URL,
                MessageBody=json.dumps(message_body)
            )

            print(f"Message sent to SQS: {response['MessageId']} for file {object_key}")

        return {
            'statusCode': 200,
            'body': json.dumps('Messages sent to SQS successfully')
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error processing S3 event')
        }

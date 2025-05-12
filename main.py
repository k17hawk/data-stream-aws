import time
from config import get_config
from utils.environment import validate_environment
from component.spark_manager import SparkManager
from component.file_processor import FileProcessor
from component.sqs_consumer import SQSConsumer

def main():
    config = get_config()
    validate_environment(config)
    
    sqs_consumer = SQSConsumer(config)
    
    try:
        while True:
            event = sqs_consumer.poll_messages()
            if event:
                spark = None
                try:
                    spark = SparkManager.create_session(config)
                    if FileProcessor.process(spark, event, config):
                        sqs_consumer.acknowledge_message(event.receipt_handle)
                finally:
                    if spark:
                        SparkManager.shutdown_session(spark)
            else:
                time.sleep(5)
                
    except KeyboardInterrupt:
        print("\nShutdown requested")

if __name__ == "__main__":
    main()
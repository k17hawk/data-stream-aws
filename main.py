from components.sqs_listener import process_messages
from logger import logger

if __name__ == "__main__":
    logger.info("Starting S3 Stream Processor Application")
    print("Starting S3 Stream Processor Application")
    try:
        process_messages()
    except KeyboardInterrupt:
        logger.info("Application shutdown requested. Exiting...")


import sys
from pyspark.sql import SparkSession

# Get S3 file path from command-line argument
if len(sys.argv) < 2:
    raise Exception("Missing S3 file path argument.")
s3_path = sys.argv[1]
print(f"Reading file from: {s3_path}")

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ReadS3CSV") \
    .getOrCreate()

# Read CSV from S3
df = spark.read.option("header", "true").csv(s3_path)

# Show a preview of the data
df.show()

from fastapi import FastAPI, Request
import asyncio

app = FastAPI()

@app.post("/trigger-spark")
async def trigger_spark(request: Request):
    data = await request.json()
    bucket = data.get("bucket")
    key = data.get("key")

    # Log or trigger your Spark job here
    print(f"Received trigger for S3 file: s3://{bucket}/{key}")

    # You could also call a Spark-submit, enqueue a job, etc.
    # subprocess.run(["spark-submit", "your_script.py", bucket, key])

    return {"message": "Spark job trigger received", "file": f"s3://{bucket}/{key}"}

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
from logger import logger
from components.spark_session import create_spark_session, shutdown_spark_session
from components.file_processor import process_file

# Create the FastAPI application instance
app = FastAPI(title="S3 ETL Processor API")

class ProcessRequest(BaseModel):
    bucket: str
    key: str
    process_immediately: Optional[bool] = True

@app.post("/process-file")
async def process_s3_file(request: ProcessRequest):
    """Endpoint to process a specific S3 file"""
    try:
        logger.info(f"API request received to process file: {request.key}")
        
        if not request.bucket or not request.key:
            raise HTTPException(status_code=400, detail="Both bucket and key are required")
        
        spark = create_spark_session()
        try:
            success = process_file(spark, request.bucket, request.key)
            if not success:
                raise HTTPException(status_code=500, detail="Failed to process file")
            return {"status": "success", "message": f"File {request.key} processed successfully"}
        finally:
            shutdown_spark_session(spark)
    except Exception as e:
        logger.error(f"API processing error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}

# This is only for direct execution, not for import
if __name__ == "__main__":
    import uvicorn
    logger.info("Starting ETL API Server")
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
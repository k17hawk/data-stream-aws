from pydantic import BaseModel

class S3Event(BaseModel):
    bucket: str
    key: str

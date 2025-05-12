from dataclasses import dataclass
from datetime import datetime
import os

@dataclass
class S3Event:
    bucket: str
    key: str
    receipt_handle: str = None  # Added this field
    timestamp: datetime = None
    processing_timestamp: datetime = None

    @property
    def filename(self):
        return self.key.split('/')[-1]

    @property
    def base_name(self):
        return os.path.splitext(self.filename)[0]
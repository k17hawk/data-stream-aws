import os
import sys
from config import AppConfig

def validate_environment(config: AppConfig):
    os.makedirs(config.LOCAL_OUTPUT_DIR, exist_ok=True)
    
    missing = []
    if not config.AWS_ACCESS_KEY_ID:
        missing.append('AWS_ACCESS_KEY_ID')
    if not config.AWS_SECRET_ACCESS_KEY:
        missing.append('AWS_SECRET_ACCESS_KEY')
    
    if missing:
        print(f"ERROR: Missing required environment variables: {missing}")
        sys.exit(1)
    
    print("Environment variables validated successfully")
import os

class Settings:
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
    LOCAL_OUTPUT_DIR = os.getenv("LOCAL_OUTPUT_DIR", "tmp")

    def validate(self):
        missing = [
            k for k, v in self.__dict__.items() if not v and k != 'LOCAL_OUTPUT_DIR'
        ]
        if missing:
            raise EnvironmentError(f"Missing environment variables: {missing}")

        os.makedirs(self.LOCAL_OUTPUT_DIR, exist_ok=True)

settings = Settings()
settings.validate()

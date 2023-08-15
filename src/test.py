import os
from dotenv import load_dotenv
load_dotenv()
# Load ENV secrets (AWS credentials and bucket name)
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_PROJECT_PATH = os.getenv("AWS_PROJECT_PATH")

print(AWS_SECRET_KEY)
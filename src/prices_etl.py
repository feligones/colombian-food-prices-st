import os
import pandas as pd
from conf import utils as uts
import boto3
from dotenv import load_dotenv

# Load ENV secrets (AWS credentials and bucket name)
load_dotenv()
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_PROJECT_PATH = os.getenv("AWS_PROJECT_PATH")

# Create an S3 client using AWS credentials
s3_client = boto3.client(
    's3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY
)

# Load the dataset for the last month using the load_last_month_dataset function
prices_dataframe = uts.load_prices_dataframe()
prices_dataframe.drop_duplicates(subset = ['date', 'product', 'market'], inplace=True)

print(prices_dataframe['market'].nunique(), prices_dataframe['product'].nunique())

print("Data load: Done!")


# Set monthly date range for all time series by reindexing the pivoted dataframe
date_range = pd.date_range(start=prices_dataframe['date'].min(), end=prices_dataframe['date'].max(), freq='MS')
prices_dataframe_piv = prices_dataframe.pivot(index='date', columns=['product', 'market'], values='mean_price')
prices_dataframe_piv = prices_dataframe_piv.reindex(date_range)

# Select Columns (Time series) with a minimum number of observations in the last 3 years
selected_series = prices_dataframe_piv.dropna(axis=1).columns
print(f"Selected time series: {len(selected_series)} out of {prices_dataframe_piv.shape[1]} in total")
prices_dataframe_piv = prices_dataframe_piv.loc[:, selected_series]

# Unstack MultiIndex and rename columns
prices_dataframe = prices_dataframe_piv.unstack().reset_index(['product', 'market']).reset_index()
prices_dataframe.columns = ['date', 'product', 'market', 'mean_price']

print("Batch reindexing and series selection: Done!")

# Save DataFrame to S3 Bucket
LOCAL_FILE_PATH = 'data/prices_dataframe.parquet'
S3_FILE_PATH = AWS_PROJECT_PATH + 'prices_dataframe.parquet'

# Save the DataFrame to a Parquet file
prices_dataframe.to_parquet(LOCAL_FILE_PATH, index=False)

# Upload the Parquet file to the S3 bucket
s3_client.upload_file(LOCAL_FILE_PATH, AWS_BUCKET_NAME, S3_FILE_PATH)

# Remove the local file after saving it to S3
os.remove(LOCAL_FILE_PATH)

print("Dataframe saved in S3: Done!")

from typing import List
import os
from io import StringIO

import boto3
import pandas as pd

BUCKET = "e88-spotify"


def save_data_to_s3(dfs: List[pd.DataFrame]) -> None:
    """
    save collected data to master storage
    Args:
        dfs: dataframes list to be stored in S3

    Returns: None

    """
    s3 = boto3.resource('s3',
                        aws_access_key_id=os.getenv("AWS_KEY"),
                        aws_secret_access_key=os.getenv("AWS_SECRET"))
    for df in dfs:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)

        date = df['date'][0]
        artist = df['artist'][0]
        file_name = f"{date}_{artist}"

        s3.Object(BUCKET, file_name).put(Body=csv_buffer.getvalue())

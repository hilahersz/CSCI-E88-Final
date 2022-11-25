import os
import logging
from datetime import datetime
from functools import partial

import boto3
import pandas as pd

logger = logging.getLogger(__name__)


def save_data_to_timestream(df: pd.DataFrame) -> None:
    """

    Args:
        df: Dataframe with information about Spotify's aggregated stats per album. Minimal columns:
            track_popularity
            artist
            album

   Returns: None

    """
    time_stream = boto3.client("timestream-write",
                               aws_access_key_id=os.getenv("AWS_KEY"),
                               aws_secret_access_key=os.getenv("AWS_SECRET"))
    logger.info("successfully connected to timestream-write client")

    date = str(int(datetime.timestamp(datetime.now()) * 1000))
    convert_series_now = partial(convert_series_to_dict, date=date)

    records = df.apply(convert_series_now, axis=1)

    time_stream.write_records(DatabaseName='spotify',
                              TableName='albums',
                              Records=records.to_list()
                              )
    logger.info("successfully written new records",
                extra={"database": "spotify", "table": "albums", "batch_size": len(records)})


def convert_series_to_dict(row: pd.Series, date: str) -> dict:
    """

    Args:
        row: A single dataframe row with at least the following columns:
            track_popularity
            artist
            album
        date: str representation of processing time

    Returns: a record to match AWS TimeStream format

    """
    record = {"Time": date,
              "MeasureValue": str(row['track_popularity']),
              "MeasureValueType": "DOUBLE",
              "MeasureName": "popularity",
              "Dimensions": [{"Name": "artist", "Value": row['artist']}, {"Name": "album", "Value": row['album']}],
              }
    return record

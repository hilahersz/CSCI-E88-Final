from datetime import datetime

import dask.dataframe as dd
import pandas as pd

GROUP_KEY = ["artist", "album"]


def transform_dask_to_time_stream(date: datetime.date) -> pd.DataFrame:
    """
    Batch processing function for AWS TimeStream to include aggregations by the album level.
    Args:
        date: The date to index that transformations should happen on

    Returns: a single dataframe for all artists with the following columns:
        artist: name of artist
        album: name of album
        track_popularity: the mean popularity
    """

    file_path = f"data/{date}_*.csv"
    ddf = dd.read_csv(file_path)

    album_info = ddf\
        .groupby(GROUP_KEY) \
        .track_popularity \
        .mean() \
        .compute()

    return album_info.reset_index()


def transform_dask_to_es(date):
    """
    Batch processing function for ES to include aggregations by the artist level.

    Args:
        date: The date to index that transformations should happen on

    Returns: a single dataframe for all artists with the following columns:
        artist: name of artist
        track_popularity: average popularity across different tracks
        artist_followers: count of followers of the artist
    """

    file_path = f"data/{date}_*.csv"
    ddf = dd.read_csv(file_path)

    mean_popularity = ddf \
        .groupby("artist") \
        .aggregate(arg={
            "track_popularity": "mean",
            "artist_followers": "max",
        }
        ).compute()
    return mean_popularity.reset_index()

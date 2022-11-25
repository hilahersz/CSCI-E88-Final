from typing import List
from datetime import datetime
from tempfile import TemporaryDirectory

import dask.dataframe as dd
import pandas as pd

GROUP_KEY = ["artist", "album"]


def aggregate_data(
        agg_func: callable,
        dfs: List[pd.DataFrame]
):
    with TemporaryDirectory() as d:
        for df in dfs:
            artist_key = df['artist'][0]
            file_path = f"{d}/{artist_key}.csv"
            df.to_csv(file_path, index=False)
        files_path = f"{d}/*.csv"
        agg_func(files_path)


def transform_dask_to_time_stream(files_path: str) -> pd.DataFrame:
    """
    Batch processing function for AWS TimeStream to include aggregations by the album level.
    Args:
        files_path: The location of csv files to be processed

    Returns: a single dataframe for all artists with the following columns:
        artist: name of artist
        album: name of album
        track_popularity: the mean popularity
    """

    ddf = dd.read_csv(files_path)

    album_info = ddf \
        .groupby(GROUP_KEY) \
        .track_popularity \
        .mean() \
        .compute()

    return album_info.reset_index()


def transform_dask_to_es(files_path: str) -> pd.DataFrame:
    """
    Batch processing function for ES to include aggregations by the artist level.

    Args:
        files_path: The location of csv files to be processed

    Returns: a single dataframe for all artists with the following columns:
        artist: name of artist
        track_popularity: average popularity across different tracks
        artist_followers: count of followers of the artist
    """
    ddf = dd.read_csv(files_path)

    mean_popularity = ddf \
        .groupby("artist") \
        .aggregate(arg={"track_popularity": "mean", "artist_followers": "max"}
                   ).compute()

    return mean_popularity.reset_index()

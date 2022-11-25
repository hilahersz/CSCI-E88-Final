import os
import uuid
from io import StringIO
from datetime import datetime, timedelta
from time import sleep

import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
import dask.dataframe as dd
from elasticsearch import Elasticsearch

BUCKET = "e88-spotify"

ARTISTS = [
    "Tylor Swift",
    "Ariana Grande",
    "Loote",
    "Bryce Vine",
    "Justin Bieber",
]


def get_artist_info(artist_name, sp):
    result = sp.search(q=artist_name, type="artist")
    artist = result.get('artists').get('items')[0]
    return artist


def get_artist_data(artist, sp):
    tracks = sp.artist_top_tracks(artist.get('id')).get('tracks')
    all_tracks = [get_track_info(track, artist) for track in tracks]
    data = pd.DataFrame.from_dict(all_tracks)
    return data


def get_track_info(track, artist):
    track_info = {
        "artist": artist.get('name'),
        "artist_popularity": artist.get('popularity'),
        "artist_followers": artist.get('followers').get('total'),
        "album": track.get('album').get('name'),
        "track": track.get('name'),
        "track_id": track.get('id'),
        "track_popularity": track.get('popularity')
    }
    return track_info


def collect_and_save_to_master(date) -> pd.DataFrame:
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials())
    artists_id = [get_artist_info(artist, sp) for artist in ARTISTS]

    for artist in artists_id:
        artist_data = get_artist_data(artist, sp)
        artist_data['date'] = date
        file_name = f'{date}_{artist.get("name").lower().replace(" ", "_")}.csv'
        save_data_to_s3(artist_data, file_name)
        artist_data.to_csv(f'data/{file_name}')
        print("saved file to s3: ", file_name)


def save_data_to_s3(data, file_name):
    s3 = boto3.resource('s3',
                        aws_access_key_id=os.getenv("AWS_KEY"),
                        aws_secret_access_key=os.getenv("AWS_SECRET"))

    csv_buffer = StringIO()
    data.to_csv(csv_buffer)

    try:
        s3.Object(BUCKET, file_name).put(Body=csv_buffer.getvalue())
        return file_name
    except Exception:
        raise ValueError("could not write data to S3")


def transform_dask_to_time_stream(date):
    ddf = dd.read_csv(f"data/{date}_*.csv")
    album_info = ddf.groupby(["artist", "album"]).track_popularity.mean().compute()
    return album_info.reset_index()


def transform_dask_to_es(date):
    ddf = dd.read_csv(f"data/{date}_*.csv")
    mean_popularity = ddf.groupby("artist").aggregate(
        arg={"track_popularity": "mean", "artist_followers": "max", "track": "count"}
    ).compute()
    return mean_popularity.reset_index()


def save_in_time_stream(data):
    records = data.apply(convert_series_to_dict, axis=1)

    time_stream = boto3.client("timestream-write",
                               aws_access_key_id=os.getenv("AWS_KEY"),
                               aws_secret_access_key=os.getenv("AWS_SECRET"))

    time_stream.write_records(DatabaseName='spotify',
                              TableName='albums',
                              Records=records.to_list()
                              )


def convert_series_to_dict(row: pd.Series) -> dict:
    time = str(int(datetime.timestamp(datetime.now()) * 1000))
    record = {"Time": time,
              "MeasureValue": str(row['track_popularity']),
              "MeasureValueType": "DOUBLE",
              "MeasureName": "popularity",
              "Dimensions": [{"Name": "artist", "Value": row['artist']}, {"Name": "album", "Value": row['album']}],
              }
    return record



def save_data_to_es(data, date):
    es = Elasticsearch(
        cloud_id=os.environ.get('cloudid'),
        basic_auth=('elastic', os.environ.get('pass'))
    )
    docs = data.apply(lambda x: get_document_from_row(x, date), axis=1)
    for doc in docs:
        result = es.index(index="artists", document=doc)
        print(result['result'] + " ID: " + result['_id'])



def get_document_from_row(row: pd.Series, date):
    document = {
        "artistId": str(uuid.uuid4()),
        "eventTime": date.strftime("%Y-%m-%d %H:%M:%S"),
        "artist": row['artist'],
        "popularity": row['track_popularity'],
        "followers": row['artist_followers']
    }
    return document


def main():
    date = datetime.now().date()
    data = collect_and_save_to_master(date)
    save_data_to_s3(data, date)


if __name__ == '__main__':
    main()
    sleep(60 * 60 * 24)

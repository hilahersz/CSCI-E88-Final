import os
import uuid
import logging

import pandas as pd
from elasticsearch import Elasticsearch

logger = logging.getLogger(__name__)


def save_data_to_es(df: pd.DataFrame) -> None:
    """
    Args:
        df: Dataframe with information about Spotify's aggregated stats per album. Minimal columns:
            date
            artist
            artist_popularity
            artist_followers

   Returns: None

    """
    es = Elasticsearch(cloud_id=os.environ.get('ES_CLOUD_ID'), basic_auth=('elastic', os.environ.get('ES_PASS')))

    docs = df.apply(get_document_from_row, axis=1)
    [es.index(index="artists", document=doc) for doc in docs]

    logger.info("successfully placed documents in elastic search",
                extra={"records": len(docs), "index": "artists"})


def get_document_from_row(row: pd.Series) -> dict:
    """
    Generate an ElasticSearch formatted document
    Args:
        row: pd.Series with relevant information for the doc

    Returns: Dictionary with keys to compliment artist table mapping

    """
    document = {
        "artistId": str(uuid.uuid4()),
        "eventTime": row['date'].strftime("%Y-%m-%d %H:%M:%S"),
        "artist": row['artist'],
        "popularity": row['artist_popularity'],
        "followers": row['artist_followers']
    }
    return document

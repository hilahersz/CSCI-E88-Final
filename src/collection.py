from typing import List
import logging

import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

logger = logging.getLogger(__name__)

ARTISTS = [
    "Tylor Swift",
    "Ariana Grande",
    "Loote",
    "Bryce Vine",
    "Justin Bieber",
]

FIRST = 0


def collect_data_from_spotify(artists: List[str] = None) -> List[pd.DataFrame]:
    """
    A function to generate dataframes collected from spotipy API
    Args:
        artists [optional]: list of artists required for the collection

    Returns: Generator of dataframes with collected data from Spotify.

    """
    if artists is None:
        artists = ARTISTS

    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials())
    logger.info("initiated spotify api connection")

    logger.info("started collecting data", extra={"artists": artists})

    artists = get_artist_by_name(sp, artists)
    top_tracks = get_artist_top_tracks(sp, artists)
    tracks_view = get_tracks_view(top_tracks, artists)

    logger.info("completed tracks lazy-load assignment")

    return tracks_view


def get_artist_top_tracks(sp: spotipy.Spotify,
                          artists: List[dict], ) -> pd.DataFrame:
    """
    Get the top tracks per artist
    Args:
        sp: spotipy client object
        artists: list of artist objects from Spotipy

    Returns:

    """

    def get_top_tracks(artist: dict):
        return sp.artist_top_tracks(artist.get('id')).get('tracks')

    top_tracks = map(get_top_tracks, artists)
    return top_tracks


def get_artist_by_name(sp: spotipy.Spotify,
                       artists_names: List[str]):
    """
    Get the spotipy artist object by name search
    Args:
        sp: spotipy client object
        artists_names: Stage name as presented in spotify (e.g. "Loote")

    Returns: artist object resulted from the search query

    """

    def get_artist(artist):
        result = sp.search(q=artist, type="artist")
        artist_obj = result.get('artists').get('items')[FIRST]
        logger.info("successfully fetched artist object", extra={"artist": artist})
        return artist_obj

    return map(get_artist, artists_names)


def get_tracks_view(tracks: List[dict],
                    artists: List[dict]
                    ) -> pd.DataFrame:
    def get_track_info(track, artist) -> dict:
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

    data = map(get_track_info, zip(tracks, artists))
    dfs = map(pd.DataFrame.from_dict, data)

    return list(dfs)

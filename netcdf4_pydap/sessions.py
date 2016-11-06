"""
This module provides functions (at the moment only one)
to manage requests and requests_cache sessions
"""

#External:
import os
import datetime
from sqlite3 import DatabaseError
import logging
import requests
import requests_cache

def create_single_session(cache=None, expire_after=datetime.timedelta(hours=1), **kwargs):
    # pylint: disable=unused-argument
    """
    Create a single session, possibly cached.

    Parameters
    ----------

    cache : str, optional
        A filename that should be used to store the session's cache
        Default: Do not store cache.
    expire_after : datetime.timedelta, optional
        How long cached data is kept, is a cache is used.
        Default: 1 hour.
    """
    # Credentials openid,username and password are accepted only for compatibility
    # purposes
    if cache is not None:
        try:
            session = requests_cache.core.CachedSession(cache, expire_after=expire_after)
        except DatabaseError as err:
            logging.info('Resseting possibly corrupted cache: ' + err.message)
            # Corrupted cache:
            try:
                os.remove(cache)
            except OSError:
                pass
            session = requests_cache.core.CachedSession(cache, expire_after=expire_after)
    else:
        #Create a phony in-memory cached session and disable it:
        session = requests.Session()
    return session

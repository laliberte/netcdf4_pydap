"""
This module provides functions (at the moment only one)
to manage requests and requests_cache sessions
"""

# External:
import os
import datetime
from sqlite3 import DatabaseError
import logging
import requests
import requests_cache


def create_single_session(cache=None, expire_after=datetime.timedelta(hours=1),
                          **kwargs):
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
    if ('backend' in kwargs and
        (kwargs['backend'] is not None and
         kwargs['backend'] not in ['sqlite',
                                   'memory'])):
        raise NotImplementedError('Only memory and sqlite caches are allowed')

    # Credentials openid,username and password are accepted only for
    # compatibility purposes
    if cache is not None:
        try:
            session = (requests_cache.core
                       .CachedSession(cache, expire_after=expire_after))
        except DatabaseError as err:
            try:
                info = err.message
            except AttributeError:
                info = str(err)
            logging.info('Resetting possibly corrupted cache: ' + info)
            # Corrupted cache:
            try:
                os.remove(cache + '.sqlite')
            except OSError:  # pragma: no cover
                pass
            session = (requests_cache.core
                       .CachedSession(cache, expire_after=expire_after))
    else:
        # Create a requests session
        session = requests.Session()
    return session

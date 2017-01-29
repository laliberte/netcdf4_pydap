import os
import requests
import requests_cache
import pytest

from netcdf4_pydap import sessions


def test_no_kwargs():
    session = sessions.create_single_session()
    assert isinstance(session, requests.Session)


def test_cache(tmpdir):
    cache_name = str(tmpdir.join('cache'))
    cached_session = sessions.create_single_session(cache=cache_name)

    assert isinstance(cached_session, requests_cache.core.CachedSession)

    assert os.path.exists(cache_name + '.sqlite')


def test_corrupted_cache(tmpdir):
    cache_name = str(tmpdir.join('cache'))
    cached_session = sessions.create_single_session(cache=cache_name)
    base_cache_size = os.stat(cache_name + '.sqlite').st_size
    cached_session.close()

    # Corrupt cache:
    with open(cache_name + '.sqlite', "r+b") as f:
        f.write(str.encode(chr(10) + chr(20) + chr(30) + chr(40)))

    cached_session = sessions.create_single_session(cache=cache_name)

    assert isinstance(cached_session, requests_cache.core.CachedSession)
    assert os.stat(cache_name + '.sqlite').st_size == base_cache_size


def test_unsupported_backend(tmpdir):
    cache_name = str(tmpdir.join('cache'))
    with pytest.raises(NotImplementedError):
        sessions.create_single_session(cache=cache_name, backend='mongodb')

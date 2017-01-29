"""
This code provides a Dataset class that allows the retrieval of
files using wget.
"""

# External:
import os
import requests
import requests_cache
import datetime

# Internal:
from .pydap_fork import esgf, get_cookies
from . import sessions


class Dataset:
    def __init__(self, url,
                 remote_data_node='',
                 timeout=120,
                 cache=None,
                 expire_after=datetime.timedelta(hours=1),
                 session=None,
                 authentication_uri=None,
                 openid=None,
                 username=None,
                 password=None,
                 use_certificates=False,
                 caching=False):
        self._url = url
        self.timeout = timeout
        self.cache = cache
        self.caching = caching
        self.expire_after = expire_after
        self.passed_session = session
        if authentication_uri == 'ESGF':
            self.authentication_uri = esgf._uri(openid)
        else:
            self.authentication_uri = authentication_uri
        self.username = username
        self.password = password
        self.use_certificates = use_certificates

        if (isinstance(self.passed_session, requests.Session) or
           isinstance(self.passed_session,
                      requests_cache.core.CachedSession)):
            self.session = self.passed_session
        else:
            self.session = (sessions
                            .create_single_session(
                                    cache=self.cache,
                                    expire_after=self.expire_after))

        self._is_initiated = False
        return

    def __enter__(self):
        # Disable cache for streaming get:
        if (isinstance(self.session, requests_cache.core.CachedSession) and
           not self.caching):
            with self.session.cache_disabled():
                return self._initiate_query()
        else:
            return self._initiate_query()

    def _initiate_query(self):
        headers = {'connection': 'keep-alive'}
        try:
            self.response = self.session.get(
                                self._url,
                                headers=headers,
                                allow_redirects=True,
                                timeout=self.timeout,
                                stream=True)
            retry = False
        except (requests.exceptions.HTTPError,
                requests.exceptions.SSLError,
                requests.exceptions.ConnectTimeout):
            retry = True
        else:
            if self.response.status_code == 401:
                self.response.close()
                retry = True

        if retry:
            # there could be something wrong with the cookies. Get them again:
            self.session = (get_cookies
                            .setup_session(self.authentication_uri,
                                           username=self.username,
                                           password=self.password,
                                           check_url=self._url,
                                           session=self.session))
            # Retry grabbing the file:
            self.response = self.session.get(self._url,
                                             headers=headers,
                                             allow_redirects=True,
                                             timeout=self.timeout,
                                             stream=True)

        if self.response.ok:
            try:
                # content size must be larger than 0.
                # when content-length key exists
                content_size = int(self.response.headers['Content-Length'])
                if content_size == 0:
                    raise RemoteEmptyError(('URL {0} is empty. It will not be '
                                            'considered').format(self._url))
            except KeyError:
                # Assume success:
                pass
        self._is_initiated = True
        return self

    def wget(self, dest_name, progress=False, block_sz=8192):
        if not self._is_initiated:
            if (isinstance(self.session, requests_cache.core.CachedSession) and
               not self.caching):
                with self.session.cache_disabled():
                    self._initiate_query()
                    size_string = self._initiated_wget(
                                         dest_name, progress=progress,
                                         block_sz=block_sz)
            else:
                self._initiate_query()
                size_string = self._initiated_wget(
                                        dest_name, progress=progress,
                                        block_sz=block_sz)
            self.response.close()
            self._is_initiated = False
            return size_string
        else:
            return self._initiated_wget(dest_name, progress=progress,
                                        block_sz=block_sz)

    def _initiated_wget(self, dest_name, progress=False, block_sz=8192):
        directory = os.path.dirname(dest_name)
        if directory != '' and not os.path.exists(directory):
            os.makedirs(directory)

        try:
            file_size = int(self.response.headers['Content-Length'])
        except KeyError:
            file_size = False

        if file_size:
            size_string = ("Downloading: %s MB: %s" %
                           (dest_name, file_size/2.0**20))
        else:
            size_string = "Downloading: %s MB: Unknown" % (dest_name)

        with open(dest_name, 'wb') as dest_file:
            file_size_dl = 0
            for buffer in self.response.iter_content(block_sz):
                dest_file.write(buffer)
                if progress and file_size:
                    file_size_dl += len(buffer)
                    status = (r"%10d  [%3.2f%%]" % (file_size_dl,
                                                    file_size_dl * 100. /
                                                    file_size))
                    status = status + chr(8)*(len(status) + 1)
        return size_string

    def __exit__(self, type, value, traceback):
        self.close()
        return

    def close(self):
        if self._is_initiated:
            self.response.close()
            self._is_initiated = False
        if not (isinstance(self.passed_session, requests.Session) or
                isinstance(self.passed_session,
                           requests_cache.core.CachedSession)):
            self.session.close()
        return


class RemoteEmptyError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

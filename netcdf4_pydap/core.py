"""
This code aims to provide:

A (partial) compatibility layer with netcdf4-python with
a special emphasis on ESGF authentication.
"""

# Internal:
from .pydap_fork import Dataset as pydap_Dataset
from .pydap_fork import esgf

DEFAULT_TIMEOUT = 120


class Dataset(pydap_Dataset):
    def __init__(self, url,
                 timeout=DEFAULT_TIMEOUT, session=None,
                 username=None, password=None, openid=None,
                 application=None, authentication_uri=None,
                 verify=True):

        if authentication_uri == 'ESGF':
            authentication_uri = esgf._uri(openid)

        pydap_Dataset.__init__(self, url, session=session, timeout=timeout,
                               verify=verify,
                               application=application,
                               authentication_uri=authentication_uri,
                               username=username, password=password)

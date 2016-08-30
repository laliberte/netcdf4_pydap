"""
Based on pydap.util.http

This code aims to provide:

An updated pydap that uses the requests package to handle Central Authentication Services.
In order to do so, code was directly borrowed from the
original pydap package.

Frederic Laliberte,2016

with special thanks to
Roberto De Almeida (pydap)
Jeff Whitaker and co-contributors (netcdf4-python)

License:
Copyright (c) 2003.2010 Roberto De Almeida
Modifications copyright (c) 2016 Frederic Laliberte

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import re
from urlparse import urlsplit, urlunsplit

import requests
import requests_cache
import warnings

import pydap.lib
import pydap.client
from pydap.model import BaseType, SequenceType
from pydap.exceptions import ServerError
from pydap.parsers.dds import DDSParser
from pydap.parsers.das import DASParser
from pydap.xdr import DapUnpacker
from pydap.lib import walk, combine_slices, fix_slice, parse_qs, fix_shn, encode_atom

import os
import datetime
import numpy as np

from collections import OrderedDict

import netCDF4.utils as utils

#Internal:
from . import requests_proxy, requests_sessions, get_cookies

python3=False
default_encoding = 'utf-8'

_private_atts =\
['_grpid','_grp','_varid','groups','dimensions','variables','dtype','data_model','disk_format',
 '_nunlimdim','path','parent','ndim','mask','scale','cmptypes','vltypes','enumtypes','_isprimitive',
 'file_format','_isvlen','_isenum','_iscompound','_cmptype','_vltype','_enumtype','name',
 '__orthogoral_indexing__','keepweakref','_has_lsd']


class Pydap_Dataset:
    def __init__(self,url,cache=None,expire_after=datetime.timedelta(hours=1),timeout=120,
                          session=None,username=None,password=None,
                          authentication_url=None, use_certificates=False):

        self._url = url
        self.timeout = timeout
        self.use_certificates = use_certificates
        self.passed_session = session

        if (isinstance(self.passed_session,requests.Session) or
            isinstance(self.passed_session,requests_cache.core.CachedSession)
            ):
            self.session=self.passed_session
        else:
            self.session=requests_sessions.create_single_session(cache=cache,expire_after=expire_after)

        if self.use_certificates:
            self._assign_dataset()
        else:
            try:
                #Assign dataset:
                self._assign_dataset()
                retry = False
            except (requests.exceptions.HTTPError, requests.exceptions.SSLError):
                #If error, try to get new cookies and then assign dataset:
                retry = True

            if retry:
                #print('Getting ESGF cookies '+esgf_get_cookies.get_node(self._url))
                self.session.cookies.update(get_cookies.cookieJar(self._url, 
                                                                       username, 
                                                                       password, 
                                                                       authentication_url=authentication_url))
                self._assign_dataset()

        # Remove any projections from the url, leaving selections.
        scheme, netloc, path, query, fragment = urlsplit(self._url)
        projection, selection = parse_qs(query)
        url = urlunsplit(
                (scheme, netloc, path, '&'.join(selection), fragment))

        # Set data to a Proxy object for BaseType and SequenceType. These
        # variables can then be sliced to retrieve the data on-the-fly.
        for var in walk(self._dataset, BaseType):
            var.data = requests_proxy.ArrayProxy(var.id, url, var.shape, self._request)
        for var in walk(self._dataset, SequenceType):
            var.data = requests_proxy.SequenceProxy(var.id, url, self._request)

        # Set server-side functions.
        self._dataset.functions = pydap.client.Functions(url)

        # Apply the corresponding slices.
        projection = fix_shn(projection, self._dataset)
        for var in projection:
            target = self._dataset
            while var:
                token, slice_ = var.pop(0)
                target = target[token]
                if slice_ and isinstance(target.data, VariableProxy):
                    shape = getattr(target, 'shape', (sys.maxint,))
                    target.data._slice = fix_slice(slice_, shape)
        return

    def _assign_dataset(self):
        for response in [self._ddx, self._ddsdas]:
            self._dataset = response()
            if self._dataset: return
        else:
            raise ServerError("Unable to open dataset.")

    def _request(self,mod_url):
        """
        Open a given URL and return headers and body.
        This function retrieves data from a given URL, returning the headers
        and the response body. Authentication can be set by adding the
        username and password to the URL; this will be sent as clear text
        only if the server only supports Basic authentication.
        """
        scheme, netloc, path, query, fragment = urlsplit(mod_url)
        mod_url = urlunsplit((
                scheme, netloc, path, query, fragment
                )).rstrip('?&')

        headers = {
            'user-agent': pydap.lib.USER_AGENT,
            'connection': 'close'}
            # Cannot keep-alive because the current pydap structure
            # leads to file descriptor leaks. Would require a careful closing
            # of requests resposes.
            #'connection': 'keep-alive'}

        if self.use_certificates:
            try:
                X509_PROXY=os.environ['X509_USER_PROXY']
            except KeyError:
                raise EnvironmentError('Environment variable X509_USER_PROXY must be set' 
                                       ' according to guidelines found at ' 
                                       ' https://pythonhosted.org/cdb_query/install.html#obtaining-esgf-certificates')
                
            with warnings.catch_warnings():
                 warnings.filterwarnings('ignore', message=('Unverified HTTPS request is being made.' 
                                                            ' Adding certificate verification is strongly advised.'
                                                            ' See: https://urllib3.readthedocs.org/en/latest/security.html'))
                 resp = self.session.get(mod_url, 
                                         cert=(X509_PROXY,X509_PROXY),
                                         verify=False,
                                         headers=headers,
                                         allow_redirects=True,
                                         timeout=self.timeout)
        else:
            #cookies are assumed to be passed to the session:
            resp = self.session.get(mod_url, 
                                    headers=headers,
                                    allow_redirects=True,
                                    timeout=self.timeout)

        # When an error is returned, we parse the error message from the
        # server and return it in a ``ClientError`` exception.
        try:
            if resp.headers["content-description"] in ["dods_error", "dods-error"]:
                m = re.search('code = (?P<code>[^;]+);\s*message = "(?P<msg>.*)"',
                        resp.content, re.DOTALL | re.MULTILINE)
                resp.close()
                msg = 'Server error %(code)s: "%(msg)s"' % m.groupdict()
                raise ServerError(msg)
        #except KeyError as e:
        #    #if content-description is missing, pass
        #    pass
        finally:
            resp.raise_for_status()

        return resp.headers, resp.content, resp

    def _ddx(self):
        """
        Stub function for DDX.

        Still waiting for the DDX spec to write this.

        """
        pass

    def _ddsdas(self):
        """
        Build the dataset from the DDS+DAS responses.

        This function builds the dataset object from the DDS and DAS
        responses, adding Proxy objects to the variables.

        """
        scheme, netloc, path, query, fragment = urlsplit(self._url)
        ddsurl = urlunsplit(
                (scheme, netloc, path + '.dds', query, fragment))
        dasurl = urlunsplit(
                (scheme, netloc, path + '.das', query, fragment))

        headerdds, dds, respdds = self._request(ddsurl)
        headerdas, das, respdas = self._request(dasurl)

        # Build the dataset structure and attributes.
        dataset = DDSParser(dds).parse()
        respdds.close()
        dataset = DASParser(das, dataset).parse()
        respdas.close()
        return dataset

    def close(self):
        if not (isinstance(self.passed_session,requests.Session) or
            isinstance(self.passed_session,requests_cache.core.CachedSession)
            ):
            #Close the session
            self.session.close()

    def __enter__(self):
        return self

    def __exit__(self,atype,value,traceback):
        self.close()

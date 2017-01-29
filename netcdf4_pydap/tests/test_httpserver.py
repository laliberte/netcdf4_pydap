import os
import hashlib
import requests_cache
import requests
import requests_mock
from six.moves.urllib.parse import quote_plus
import pytest

from netcdf4_pydap.pydap_fork import esgf
from netcdf4_pydap import httpserver
from netcdf4_pydap.httpserver import RemoteEmptyError

TEST_USERNAME = os.environ.get('USERNAME_ESGF')
TEST_OPENID = os.environ.get('OPENID_ESGF')
TEST_PASSWORD = os.environ.get('PASSWORD_ESGF')

_skip_openid = False if TEST_OPENID else True
_skip_password = False if TEST_PASSWORD else True
_skip_any = _skip_openid or _skip_password

TEST_OPENID = TEST_OPENID or ''


@pytest.fixture(scope='module')
def test_url():
    return ('http://aims3.llnl.gov/thredds/fileServer/'
            'cmip5_css02_data/cmip5/output1/CCCma/CanCM4/'
            'decadal1995/fx/atmos/fx/r0i0p0/orog/1/'
            'orog_fx_CanCM4_decadal1995_r0i0p0.nc')


@pytest.fixture(scope='module')
def test_auth_uri():
    return (('https://aims3.llnl.gov/esg-orp/'
             'j_spring_openid_security_check.htm?'
             'openid_identifier=') + quote_plus(TEST_OPENID))


@pytest.mark.skipif(_skip_openid, reason='OPENID_ESGF not set in environment')
def test_authentication_uri(test_url, test_auth_uri):
    authentication_uri = esgf._uri(TEST_OPENID)
    assert authentication_uri(test_url) == test_auth_uri


@pytest.mark.skipif(_skip_any, reason=('OPENID_ESGF or PASSWORD_ESGF'
                                       ' not set in environment'))
def test_httpserver_esgf(test_url, test_auth_uri, tmpdir):
    dest_name = str(tmpdir.join(os.path.basename(test_url)))
    with httpserver.Dataset(test_url, authentication_uri=test_auth_uri,
                            password=TEST_PASSWORD) as remote_data:
        size_string = remote_data.wget(dest_name, progress=True)

    assert size_string.startswith('Downloading:')

    expected_sha256_sum = ('624c5c40fe59d244c4b835e57936cf588d3dcf'
                           '9915935923d58dcb7ef6c9c029')
    sha256_sum = hashlib.sha256(open(dest_name, 'rb').read()).hexdigest()

    assert expected_sha256_sum == sha256_sum


def _create_tmp_file(tmpdir, content='content'):
    tmp_file = tmpdir.join("test.txt")
    tmp_file.write(content)
    return tmp_file, content


def test_httpserver_empty_mock(tmpdir):
    tmp_file, content = _create_tmp_file(tmpdir)
    dest_name = tmpdir.join("download.txt")
    with open(str(tmp_file), 'rb') as f:
        with requests_mock.Mocker() as m:
            m.get('mock://' + str(tmp_file), body=f,
                  headers={'Content-Length': '0'})
            with pytest.raises(RemoteEmptyError) as e:
                with httpserver.Dataset('mock://' +
                                        str(tmp_file)) as remote_data:
                    remote_data.wget(str(dest_name))
            assert str(e.value).endswith("test.txt is empty. It will "
                                         "not be considered'")


def test_httpserver_exception_mock(tmpdir):
    tmp_file, content = _create_tmp_file(tmpdir)
    dest_name = tmpdir.join("download.txt")

    def raise_exception(request, context):
        raise requests.exceptions.ConnectTimeout

    with requests_mock.Mocker() as m:
        m.get('mock://' + str(tmp_file),
              text=raise_exception)
        with pytest.raises(requests.exceptions.ConnectTimeout):
            with httpserver.Dataset('mock://' +
                                    str(tmp_file)) as remote_data:
                remote_data.wget(str(dest_name))


def test_httpserver_mock(tmpdir):
    tmp_file, content = _create_tmp_file(tmpdir)
    dest_name = tmpdir.join("download.txt")
    with open(str(tmp_file), 'rb') as f:
        with requests_mock.Mocker() as m:
            m.get('mock://' + str(tmp_file), body=f)
            with httpserver.Dataset('mock://' + str(tmp_file)) as remote_data:
                remote_data.wget(str(dest_name))

    assert dest_name.read() == content


def test_httpserver_not_inst_mock(tmpdir):
    tmp_file, content = _create_tmp_file(tmpdir)
    dest_name = tmpdir.join("download.txt")
    with open(str(tmp_file), 'rb') as f:
        with requests_mock.Mocker() as m:
            m.get('mock://' + str(tmp_file), body=f)
            remote_data = httpserver.Dataset('mock://' + str(tmp_file))
            remote_data.wget(str(dest_name))

    assert dest_name.read() == content


def test_httpserver_mock_session_not_inst(tmpdir):
    tmp_file, content = _create_tmp_file(tmpdir)
    session = requests.Session()
    adapter = requests_mock.Adapter()

    with open(str(tmp_file), 'rb') as f:
        adapter.register_uri('GET', str(tmp_file), body=f)
        session.mount('mock', adapter)

        dest_name = tmpdir.join("download.txt")
        remote_data = httpserver.Dataset('mock://' + str(tmp_file),
                                         session=session)
        remote_data.wget(str(dest_name))
        remote_data.close()

    assert dest_name.read() == content


def test_httpserver_mock_session_mkdir(tmpdir):
    tmp_file, content = _create_tmp_file(tmpdir)
    session = requests.Session()
    adapter = requests_mock.Adapter()

    with open(str(tmp_file), 'rb') as f:
        adapter.register_uri('GET', str(tmp_file), body=f)
        session.mount('mock', adapter)

        dest_name = str(tmpdir) + '/sub/' + "download.txt"
        with httpserver.Dataset('mock://' + str(tmp_file),
                                session=session) as remote_data:
            remote_data.wget(str(dest_name))
            remote_data.close()

    with open(dest_name, 'r') as f:
        assert f.read() == content


def test_cached_session_disabled_httpserver_mock(tmpdir):
    tmp_file, content = _create_tmp_file(tmpdir)
    adapter = requests_mock.Adapter()

    cache_name = str(tmpdir.join('test_cache'))
    with requests_cache.core.CachedSession(cache_name=cache_name,
                                           backend='sqlite') as session:
        base_cache_size = os.stat(cache_name + '.sqlite').st_size
        dest_name = tmpdir.join('download.txt')
        with open(str(tmp_file), 'rb') as f:
            adapter.register_uri('GET', str(tmp_file), body=f)
            session.mount('mock', adapter)
            with httpserver.Dataset('mock://' + str(tmp_file),
                                    session=session) as remote_data:
                remote_data.wget(str(dest_name))

    assert dest_name.read() == content
    assert os.stat(cache_name + '.sqlite').st_size == base_cache_size


def test_cached_session_disabled_not_inst_httpserver_mock(tmpdir):
    tmp_file, content = _create_tmp_file(tmpdir)
    adapter = requests_mock.Adapter()

    cache_name = str(tmpdir.join('test_cache'))
    with requests_cache.core.CachedSession(cache_name=cache_name,
                                           backend='sqlite') as session:
        base_cache_size = os.stat(cache_name + '.sqlite').st_size
        dest_name = tmpdir.join('download.txt')
        with open(str(tmp_file), 'rb') as f:
            adapter.register_uri('GET', str(tmp_file), body=f)
            session.mount('mock', adapter)
            remote_data = httpserver.Dataset('mock://' + str(tmp_file),
                                             session=session)
            remote_data.wget(str(dest_name))

    assert dest_name.read() == content
    assert os.stat(cache_name + '.sqlite').st_size == base_cache_size


def test_cached_session_httpserver_mock(tmpdir):
    tmp_file, content = _create_tmp_file(tmpdir)
    adapter = requests_mock.Adapter()

    cache_name = str(tmpdir.join('test_cache'))
    with requests_cache.core.CachedSession(cache_name=cache_name,
                                           backend='sqlite') as session:
        # base_cache_size = os.stat(cache_name + '.sqlite').st_size
        dest_name = tmpdir.join('download.txt')
        with open(str(tmp_file), 'rb') as f:
            adapter.register_uri('GET', str(tmp_file), body=f)
            session.mount('mock', adapter)
            with pytest.raises(Exception):
                with httpserver.Dataset('mock://' + str(tmp_file),
                                        session=session,
                                        caching=True) as remote_data:
                        remote_data.wget(str(dest_name))
            #    # In future versions, requests_cache might handle
            #    # this case. In that situation, expect:
            #    assert dest_name.read() == content
            #    assert (os.stat(cache_name + '.sqlite').st_size >
            #            base_cache_size)

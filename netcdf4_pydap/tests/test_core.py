"""
Test module for CAS

"""
import os
import requests
import netcdf4_pydap


def test_string_var():
    """
    Test with ESGF
    """
    import netcdf4_pydap.cas.esgf as esgf
    cred = {'username': None,
            'password': os.environ['PASSWORD_ESGF'],
            'use_certificates': False,
            'authentication_url': esgf._uri(os.environ['OPENID_ESGF'])}
    url = ('http://esgf-data1.ceda.ac.uk/thredds/dodsC/'
           'esg_dataroot/cmip5/output1/'
           'NCC/NorESM1-M/abrupt4xCO2/mon/ocean/Omon/r1i1p1/v20110901/msftmyz/'
           'msftmyz_Omon_NorESM1-M_abrupt4xCO2_r1i1p1_000101-015012.nc')

    session = requests.Session()
    with netcdf4_pydap.Dataset(url, session=session, **cred) as dataset:
        assert (dataset.variables['region'].dtype == '|S200')


def test_content_description():
    """
    Test with ESGF
    """
    import netcdf4_pydap.cas.esgf as esgf
    cred = {'username': None,
            'password': os.environ['PASSWORD_ESGF'],
            'use_certificates': False,
            'authentication_url': esgf._uri(os.environ['OPENID_ESGF'])}
    url = ('http://noresg.norstore.no/thredds/dodsC/'
           'esg_dataroot/cmor/CMIP5/output1/'
           'NCC/NorESM1-M/abrupt4xCO2/mon/ocean/Omon/r1i1p1/v20110901/msftmyz/'
           'msftmyz_Omon_NorESM1-M_abrupt4xCO2_r1i1p1_000101-015012.nc')

    session = requests.Session()

    from pydap.exceptions import ServerError
    try:
        with netcdf4_pydap.Dataset(url, session=session, **cred) as dataset:
            dataset.__repr__()
            pass
    except ServerError:
        pass
    except:
        assert(False)

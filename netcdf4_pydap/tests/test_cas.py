"""
Test module for CAS

"""
import os
import requests
import netcdf4_pydap
import numpy as np


def test_urs_earthdata_nasa_gov():
    """
    Test the urs.earthdata.nasa.gov portal.
    """
    cred = {'username': os.environ['USERNAME_URS'],
            'password': os.environ['PASSWORD_URS'],
            'authentication_uri': 'https://urs.earthdata.nasa.gov/'}
    url = ('http://goldsmr3.gesdisc.eosdis.nasa.gov:80/opendap/'
           'MERRA_MONTHLY/MAIMCPASM.5.2.0/1979/'
           'MERRA100.prod.assim.instM_3d_asm_Cp.197901.hdf')

    session = requests.Session()
    with netcdf4_pydap.Dataset(url, session=session, **cred) as dataset:
        data = dataset.variables['SLP'][0, :5, :5]
    expected_data = [[[99066.15625, 99066.15625, 99066.15625, 99066.15625,
                       99066.15625],
                      [98868.15625, 98870.15625, 98872.15625, 98874.15625,
                       98874.15625],
                      [98798.15625, 98810.15625, 98820.15625, 98832.15625,
                       98844.15625],
                      [98856.15625, 98828.15625, 98756.15625, 98710.15625,
                       98776.15625],
                      [99070.15625, 99098.15625, 99048.15625, 98984.15625,
                       99032.15625]]]
    assert (data == expected_data).all()
    # Make sure that credentials were propagated in session object:
    assert ('nasa_gesdisc_data_archive' in session.cookies.get_dict())


def test_esgf():
    """
    Test for ESGF
    """
    cred = {'username': None,
            'password': os.environ['PASSWORD_ESGF'],
            'openid': os.environ['OPENID_ESGF'],
            'authentication_uri': 'ESGF'}
    url = ('http://aims3.llnl.gov/thredds/dodsC/'
           'cmip5_css02_data/cmip5/output1/CCCma/CanCM4/'
           'decadal1995/fx/atmos/fx/r0i0p0/orog/1/'
           'orog_fx_CanCM4_decadal1995_r0i0p0.nc')

    session = requests.Session()
    with netcdf4_pydap.Dataset(url, session=session, **cred) as dataset:
        data = dataset['orog'][50:55, 50:55]
    expected_data = [[197.70425, 16.319595, 0.0, 0.0, 0.0],
                     [0.0, 0.0, 0.0, 0.0, 0.0],
                     [0.0, 0.0, 0.0, 0.0, 0.0],
                     [677.014, 628.29675, 551.06, 455.5758, 343.7354],
                     [1268.3304, 1287.9553, 1161.0402, 978.3153, 809.143]]
    assert np.isclose(data, expected_data).all()
    # Make sure that credentials were propagated in session object:
    assert ('esg.openid.saml.cookie' in session.cookies.get_dict())

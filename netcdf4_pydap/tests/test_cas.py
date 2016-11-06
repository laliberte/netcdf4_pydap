"""
Test module for CAS

"""
import os
import requests
import netcdf4_pydap
import pytest
import numpy as np
import time

def test_urs_earthdata_nasa_gov():
    """
    Test the urs.earthdata.nasa.gov portal.
    """
    cred = {'username' : os.environ['USERNAME_NASA'],
            'password' : os.environ['PASSWORD_NASA'],
            'use_certificates' : False,
            'authentication_url' : 'https://urs.earthdata.nasa.gov/'}
    url = ('http://goldsmr3.gesdisc.eosdis.nasa.gov:80/opendap/'
           'MERRA_MONTHLY/MAIMCPASM.5.2.0/1979/'
           'MERRA100.prod.assim.instM_3d_asm_Cp.197901.hdf')

    session = requests.Session()
    try:
        with netcdf4_pydap.Dataset(url, session=session, **cred) as dataset:
            data = dataset.variables['SLP'][0, :5, :5]
    except requests.exceptions.HTTPError:
        time.sleep(60)
        with netcdf4_pydap.Dataset(url, session=session, **cred) as dataset:
            data = dataset.variables['SLP'][0, :5, :5]
    expected_data = [[[99066.15625, 99066.15625, 99066.15625, 99066.15625, 99066.15625],
                      [98868.15625, 98870.15625, 98872.15625, 98874.15625, 98874.15625],
                      [98798.15625, 98810.15625, 98820.15625, 98832.15625, 98844.15625],
                      [98856.15625, 98828.15625, 98756.15625, 98710.15625, 98776.15625],
                      [99070.15625, 99098.15625, 99048.15625, 98984.15625, 99032.15625]]]
    assert (data == expected_data).all()


def test_esgf():
    """
    Test for ESGF
    """
    import netcdf4_pydap.cas.esgf as esgf
    cred = {'username': None,
            'password': os.environ['PASSWORD_ESGF'],
            'use_certificates': False,
            'authentication_url': esgf.authentication_url(os.environ['OPENID_ESGF'])}
    url = ('http://cordexesg.dmi.dk/thredds/dodsC/cordex_general/'
           'cordex/output/EUR-11/DMI/ICHEC-EC-EARTH/historical/r3i1p1/'
           'DMI-HIRHAM5/v1/day/pr/v20131119/'
           'pr_EUR-11_ICHEC-EC-EARTH_historical_r3i1p1_DMI-HIRHAM5_v1_day_19960101-20001231.nc')

    session = requests.Session()
    with netcdf4_pydap.Dataset(url, session=session, **cred) as dataset:
        data = dataset.variables['pr'][0, 200:205, 100:105]
    expected_data = [[[5.23546005e-05,  5.48864300e-05,  5.23546005e-05,  6.23914966e-05,
                       6.26627589e-05],
                      [5.45247385e-05,  5.67853021e-05,  5.90458621e-05,  6.51041701e-05,
                       6.23914966e-05],
                      [5.57906533e-05,  5.84129048e-05,  6.37478297e-05,  5.99500854e-05,
                       5.85033267e-05],
                      [5.44343166e-05,  5.45247385e-05,  5.60619228e-05,  5.58810752e-05,
                       4.91898136e-05],
                      [5.09982638e-05,  4.77430549e-05,  4.97323490e-05,  5.43438946e-05,
                       5.26258664e-05]]]
    assert np.isclose(data, expected_data).all()

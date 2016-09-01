netcdf4_pydap
=============

This code provides enhancements to the `pydap` client package:

#. An updated pydap client that uses the `requests` package to handle Central Authentication Services (such as ESGF or NASA Earthdata).
#. A (partial) compatibility layer with `netcdf4-python`.

Frederic Laliberte
University of Toronto, 2016

Documentation
-------------

ESGF
^^^^

#. Register at https://pcmdi.llnl.gov/user/add/?next=https://pcmdi.llnl.gov/projects/esgf-llnl/.
   When registering, you will create a `password` and receive an `openid`.

#. Go to https://pcmdi.llnl.gov/esg-orp/home.htm and login with your `openid` using your `password`.

#. Go to https://pcmdi.llnl.gov/esg-orp/registration-request.htm?resource=http%3A%2F%2Fpcmdi.llnl.gov%2Fthredds%2FfileServer%2Fesg_testroot%2Fregister%2Fcmip5_research.nc.
   Your browser will likely ask you to trust some certificates. CLICK `ALWAYS ALLOW`. If you don't do this, it won't work.

#. Register for `CMIP5 Research`. You do not need to download data.

#. Connect to OPeNDAP links as follows::

    import matplotlib.pyplot as plt
    import numpy as np

    import netcdf4_pydap 
    import netcdf4_pydap.esgf as esgf
    openid = YOUROPENID
    password = YOURPASSWORD

    credentials={
                  'password' : password,
                  'authentication_url' : esgf.authentication_url(openid)}

    url = ('http://cordexesg.dmi.dk/thredds/dodsC/cordex_general/cordex/' 
           'output/EUR-11/DMI/ICHEC-EC-EARTH/historical/r3i1p1/DMI-HIRHAM5/'
           'v1/day/pr/v20131119/'
           'pr_EUR-11_ICHEC-EC-EARTH_historical_r3i1p1_DMI-HIRHAM5_v1_day_19960101-20001231.nc')

    with netcdf4_pydap.Dataset(url,**credentials) as dataset:
        data = dataset.variables['pr'][0,:,:]
        plt.contourf(np.squeeze(data))
        plt.show()

#. If using https://ceda.ac.uk instead of https://pcmdi.llnl.gov, your `username` must be passed along with your credentials use::

    credentials={
                  'username' : YOURCEDAUSERNAME,
                  'password' : password,
                  'authentication_url' : esgf.authentication_url(openid)}


NASA Earthdata
^^^^^^^^^^^^^^
There are a few steps required to connect to NASA Earthdata:

#. Create an account at https://usr.earthdata.nasa.gov
#. Authorize the data to see your profile (NASA calls this adding an app, a la Facebook).
   Follow http://disc.sci.gsfc.nasa.gov/registration/authorizing-desidsc-data-access-in-earthdata_login
#. Load OPeNDAP data::

    import matplotlib.pyplot as plt
    import numpy as np
    import netcdf4_pydap

    credentials={'username': YOURUSERNAME,
                 'password': YOURPASSWORD,
                 'authentication_url':'https://urs.earthdata.nasa.gov/'}
    url = ('http://goldsmr3.gesdisc.eosdis.nasa.gov:80/opendap/'
           'MERRA_MONTHLY/MAIMCPASM.5.2.0/1979/MERRA100.prod.assim.instM_3d_asm_Cp.197901.hdf')

    with netcdf4_pydap.Dataset(url,**credentials) as dataset:
        data = dataset.variables['SLP'][0,:,:]
        plt.contourf(np.squeeze(data))
        plt.show()


Version History
---------------

0.1.1:  Added special Dataset class to mimic wget.

0.1 :   Initial commit.

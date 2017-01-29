"""
A simple wget-like utility to download data from the ESGF.
"""

import click

from netcdf4_pydap.cas import esgf
from netcdf4_pydap import httpserver


@click.command()
@click.argument('url')
@click.argument('dest_name')
@click.option('--password')
@click.option('--username')
@click.option('--openid')
def wget(url, dest_name, password=None, username=None, openid=None):
    authentication_url = esgf._uri(openid)
    with httpserver.Dataset(url, authentication_url=authentication_url,
                            password=password,
                            username=username) as remote_data:
        remote_data.wget(dest_name, progress=True)


if __name__ == '__main__':
    wget()

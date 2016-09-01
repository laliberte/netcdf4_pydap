import urllib
import warnings

def authentication_url(openid):
    '''
    Create ESGF authentication url.
    This function might be sensitive to a future evolution of the ESGF security.
    '''
    def generate_url(dest_url):
        dest_node=get_node(dest_url)

        url = dest_node+'/esg-orp/j_spring_openid_security_check.htm?openid_identifier='+urllib.quote_plus(openid)
        if get_node(openid)=='https://ceda.ac.uk':
            return [url, None]
        else:
            return url
    return generate_url

def get_node(url):
        return '/'.join(url.split('/')[:3]).replace('http:','https:')

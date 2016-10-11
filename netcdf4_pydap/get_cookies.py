import mechanize
import cookielib
import urllib
import ssl
import warnings
import requests
import copy

def cookieJar(dest_url, username, password, authentication_url=None):
    '''
    Retrieve CAS cookies using mechanize and by calling the right url.
    '''
    cj = cookielib.LWPCookieJar()
    if authentication_url is None:
        return cj

    br = mechanize.Browser()

    br.set_cookiejar(cj)

    # Browser options
    br.set_handle_equiv(True)
    br.set_handle_redirect(True)
    br.set_handle_referer(True)
    br.set_handle_robots(False)

    # Follows refresh 0 but not hangs on refresh > 0
    br.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1)

    # Want debugging messages?
    #br.set_debug_http(True)
    #br.set_debug_redirects(True)
    #br.set_debug_responses(True)

    br.addheaders = [('User-agent', 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1')]

    if isinstance(authentication_url,str):
        base_url = authentication_url 
    else:
        base_url = authentication_url(dest_url)

    #This is important to allow for several security layers:
    full_url = copy.copy(base_url)
    if isinstance(full_url,list):
        base_url = full_url[0]

    if password==None or password=='':
        warnings.warn('password was not set. this was likely unintentional but will result is much fewer datasets.')
        return cj

    #Do not verify certificate (we do not worry about MITM)
    ssl._https_verify_certificates(False)
    br = mechanize_login(br, base_url, username, password)

    if ( isinstance(full_url, list) and
         len(full_url) > 1):
        for url in full_url[1:]:
            br = mechanize_login(br, url, username, password)
        
    resp = requests.get(dest_url,cookies=cj)
    if resp.status_code==403:
        #The user has not registered with a usage category:
        raise Exception('Credentials were accepted but additional steps must be taken to access'
                        'data.'
                        'To do so, navigate to {0}, log in using your credentials '
                        'Then, follow instructions to properly register for data access.'
                        'This usually has to be done only once per data service provider.'.format(dest_url))
    resp.close()

    #Restore certificate verification
    ssl._https_verify_certificates(True)
    return cj

def mechanize_login(br, url, username, password):
    if url is not None:
        r = br.open(url)

    br.select_form(nr=0)

    try:
        br.form['username']=username
    except mechanize._form.ControlNotFoundError:
        pass

    try:
        br.form['password']=password
    except mechanize._form.ControlNotFoundError:
        if url is not None:
            br.close()
            raise Exception('Navigate to {0}. '
                            'If you are unable to login, you must either wait or use authentication from another service.'.format(url))
        else:
            pass

    try:
        br.find_control("remember").items[0].selected = True
    except mechanize._form.ControlNotFoundError:
        pass

    r = br.submit()
    return br

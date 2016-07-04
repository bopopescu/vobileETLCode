import urllib
import urllib2
import cookielib


cookie = cookielib.MozillaCookieJar()
#cookie = cookielib.LWPCookieJar()
#cookie = cookielib.FileCookieJar()
cookie.load('cookies', ignore_discard=True, ignore_expires=True)

url = "http://206.99.94.88:8080/opm_web/"

req = urllib.request.Request(url)
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie))
request = opener.open(req)
print request.getcode()

#!/opt/csw/bin/python3

"""
This script will collect report(s) and store the information in a file.
"""

import logging
import os
import requests
from lib import mf_env

cfg = mf_env.init_env("meldpuntfietspaden", __file__)
try:
    http_proxy = cfg['Main']['proxy']
    os.environ['http_proxy'] = http_proxy
except KeyError:
    pass
url1 = cfg['MeldpuntFietspaden']['url1']
url2 = cfg['MeldpuntFietspaden']['url2']
url3 = cfg['MeldpuntFietspaden']['url3']
reportdir = cfg['MeldpuntFietspaden']['reportdir']

monthrange = mf_env.month_year_iter(cfg['Main']['startmaand'], cfg['Main']['startjaar'])
for jaar, maand in monthrange:
    mnd = "{maand:0>2}".format(maand=maand)
    fn = "{jaar}_{mnd}.csv".format(jaar=jaar, mnd=mnd)
    report = reportdir + "/" + fn
    url = "{url1}{jaar}{url2}{mnd}{url3}".format(url1=url1, url2=url2, url3=url3, jaar=jaar, mnd=mnd)
    print("Report: {report}\nURL: {url}".format(report=report, url=url))
    res = requests.get(url).content
    fh = open(report, 'wb')
    fh.write(res)
    fh.close()
logging.info("End application")

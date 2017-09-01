"""
This script will collect report(s) and store the information in a file.
"""

import logging
import requests
from lib import mf_env

cfg = mf_env.init_env("meldpuntfietspaden", __file__)
logging.info("Start application")
url1 = cfg['MeldpuntFietspaden']['url1']
url2 = cfg['MeldpuntFietspaden']['url2']
url3 = cfg['MeldpuntFietspaden']['url3']
reportdir = cfg['MeldpuntFietspaden']['reportdir']

for jaar in range(2014, 2017):
    for maand in range(1, 13):
        mnd = "{maand:0>2}".format(maand=maand)
        fn = "{jaar}_{mnd}.csv".format(jaar=jaar, mnd=mnd)
        report = reportdir + "/" + fn
        url = "{url1}{jaar}{url2}{mnd}{url3}".format(url1=url1, url2=url2, url3=url3, jaar=jaar, mnd=mnd)
        print("Report: {report}\nURL: {url}".format(report=report, url=url))
        res = requests.get(url).content
        fh = open(report, 'wb')
        fh.write(res)
        fh.close()
for maand in range(1, 9):
    jaar = 2017
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

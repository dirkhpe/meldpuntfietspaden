#!/opt/csw/bin/python3

"""
This script will collect report(s) and store the information in a file.
"""

import logging
import os
import requests
from lib import mf_env

cfg = mf_env.init_env("meldpuntfietspaden", __file__)
logging.info("Start application")
http_proxy = cfg['Main']['proxy']
os.environ['http_proxy'] = http_proxy
url = cfg['MeldpuntFietspaden']['url']
reportdir = cfg['MeldpuntFietspaden']['reportdir']

jaar = '2014'
mnd = '01'
fn = "{jaar}_{mnd}.csv".format(jaar=jaar, mnd=mnd)
report = reportdir + "/" + fn
res = requests.get(url).content
fh = open(report, 'wb')
fh.write(res)
fh.close()
logging.info("End application")

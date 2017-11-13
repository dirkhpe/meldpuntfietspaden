#!/opt/csw/bin/python3

"""
This script will prepare the indicator_report file for load into the MOW Dataroom application. The file will be set
on the dropserver.
"""

import logging
import os
from lib.datastore import Datastore
from lib import mf_env
from lib import ftp_handler

cfg = mf_env.init_env("meldpuntfietspaden", __file__)
logging.info("Start application")
ds = Datastore(cfg)
hdr, res = ds.get_indicator_report(indicator_id=cfg['MeldpuntFietspaden']['indicator_id'])
fn = os.path.join(cfg['MeldpuntFietspaden']['indicdir'], cfg['MeldpuntFietspaden']['indicname'])
fh = open(fn, mode='w')
hdrline = ";".join(hdr)
fh.write(";".join(hdr)+"\n")
for rec in res:
    val_arr = [str(rec[k]) for k in rec.keys()]
    fh.write(";".join(val_arr)+"\n")
fh.close()

ftp = ftp_handler.FtpHandler(cfg)
ftp.load_file(fn)
ftp.close_connection()

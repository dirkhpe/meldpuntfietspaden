#!/opt/csw/bin/python3

import logging
import subprocess
from lib import mf_env

# Initialize Environment
projectname = "meldpuntfietspaden"
config = mf_env.get_inifile(projectname)
scriptdir = config['Main']['scriptdir']

# Rebuild sqlite database
for scriptname in ['100_refresh_apex.py', '110_refresh_meldingtx.py', '120_refresh_indicator.py']:
    cmdline = scriptdir + scriptname
    subprocess.call(cmdline)

# Collect data from the mobielvlaanderen.be
scriptname = '200_collect_data_from_web.py'
cmdline = scriptdir + scriptname
subprocess.call(cmdline)

# Consolidate data into sqlite database
scriptname = '300_process_month.py'
cmdline = scriptdir + scriptname
subprocess.call(cmdline)

# Extract data into file for load, move the file to dropserver
scriptname = '400_prepare_for_load.py'
cmdline = scriptdir + scriptname
subprocess.call(cmdline)

logging.info("End Application")

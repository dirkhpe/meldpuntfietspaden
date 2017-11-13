#!/opt/csw/bin/python3

"""
This script will refresh the information for the indicator. It will drop and recreate the table.
"""

import logging
from lib.datastore import Datastore
from lib import mf_env


def main():
    cfg = mf_env.init_env("meldpuntfietspaden", __file__)
    ds = Datastore(cfg)
    ds.create_indicator_table()
    return cfg, ds


if __name__ == "__main__":
    config, datastore = main()
    logging.info("End Application")

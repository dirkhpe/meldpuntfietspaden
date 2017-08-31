"""
This script will refresh the translation information from melding to element.
"""

import logging
from lib.datastore import Datastore
from lib import mf_env


def main():
    cfg = mf_env.init_env("meldpuntfietspaden", __file__)
    ds = Datastore(cfg)
    ds.create_meldingtx_table()
    return cfg, ds


if __name__ == "__main__":
    config, datastore = main()
    meldingtx = config['MeldpuntFietspaden']['meldingtx']
    datastore.populate_table(meldingtx, "meldingtx")
    logging.info("End Application")

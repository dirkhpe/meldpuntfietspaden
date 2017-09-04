#!/opt/csw/bin/python3

"""
This script will refresh the information from APEX. It will drop and re-create the tables dimensie and dim_element.
It will load the data from the most recent files.
"""

import logging
from lib.datastore import Datastore
from lib import mf_env


def main():
    cfg = mf_env.init_env("meldpuntfietspaden", __file__)
    ds = Datastore(cfg)
    ds.create_apex_tables()
    logging.info("End Application")
    return cfg, ds


if __name__ == "__main__":
    config, datastore = main()
    # Populate dimensie table
    dimensie_file = config['Main']['dimensie']
    datastore.populate_table(dimensie_file, "dimensie")
    dim_element_file = config['Main']['dim_element']
    datastore.populate_table(dim_element_file, "dim_element")


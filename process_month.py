"""
This script will process the information for one month.
"""

import logging
from lib.datastore import Datastore
from lib import mf_env


def main():
    cfg = mf_env.init_env("meldpuntfietspaden", __file__)
    ds = Datastore(cfg)
    return cfg, ds

def populate_table(ds, fn, tn):
    """
    This function will convert an output file of application Meldpunt Fietspaden and load the data in the indicator
    table meldpuntfietspaden.

    :param ds: Handle to the Datastore.

    :param fn: Path to the csv file to handle.

    :param tn: Table name to be loaded.

    :return:
    """
    rec_info = mf_env.LoopInfo(tn, 100)
    jaar = 2014
    maand = 1
    with open(fn) as fh:
        column_line = fh.readline().strip()
        columns = []
        prefix = ""
        # First set of columns is for gemeentewegen, second set is for gewestwegen.
        # Gewestwegen is triggered as soon as a duplicate column is found.
        for item in column_line.split(";"):
            if prefix == "":
                if item in columns:
                    prefix = "gewest_"
            columns.append(prefix + item)
        for line in fh:
            vals = [val.strip("\"") for val in line.strip().split(";")]
            row2insert = {}
            for cnt in range(len(columns)):
                row2insert[columns[cnt]] = vals[cnt]
            ds.insert_row(tn, row2insert)
            rec_info.info_loop()
    rec_info.end_loop()


if __name__ == "__main__":
    config, datastore = main()
    fn = config['MeldpuntFietspaden']['ffn']
    populate_table(datastore, fn, "meldpuntfietspaden")
    logging.info("End Application")

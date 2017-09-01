"""
This script will process the information for one month.
"""

import logging
from lib.datastore import Datastore
from lib import mf_env


def get_columns(line):
    """
    This method will collect the columns for the result file.
    First set of columns is for gemeentewegen, second set is for gewestwegen.
    Gewestwegen is triggered as soon as a duplicate column is found.

    :param line: First line of the result file containing the column attributes.

    :return: Array of column names.
    """
    column_array = []
    prefix = ""
    for item in line.split(";"):
        if prefix == "":
            if item in column_array:
                prefix = "gewest_"
        column_array.append(prefix + item)
    return column_array

def get_gemeente(naam):
    """
    This method will get the gemeente and the provincie. The gemeente naam must be found in the dim_element table.
    Then provincie is found using the URIs.

    :param naam: Gemeente naam to be found.

    :return: gemeente, provincie
    """
    uri = ds.get_uri("Gemeente", naam)
    if uri:
        provincie = ds.get_provincie(uri)
        return naam, provincie
    else:
        return False, False

def populate_table(fn, tn):
    """
    This function will convert an output file of application Meldpunt Fietspaden and load the data in the indicator
    table meldpuntfietspaden.

    :param fn: Path to the csv file to handle.

    :param tn: Table name to be loaded.

    :return:
    """
    rec_info = mf_env.LoopInfo(tn, 100)
    jaar = 2014
    maand = 1
    with open(fn) as fh:
        column_line = fh.readline().strip()
        columns = get_columns(column_line)
        # Then process each row.
        for line in fh:
            # convert the row to array of values
            rec_info.info_loop()
            vals = [val.strip("\"") for val in line.strip().split(";")]
            # Length of the row must be equal to length of columns
            if not (len(columns) == len(vals)):
                # TODO: add to mail
                logging.error("Invalid row length for jaar {jaar} and month {maand}".format(jaar=jaar, maand=maand))
            else:
                gemeente, provincie = get_gemeente(vals[0])
                if not gemeente:
                    logging.error("Gemeente {g} not found, Jaar {jaar} and month {maand}"
                                  .format(g=vals[0], jaar=jaar, maand=maand))
                else:
                    for cnt in range(1, len(columns)):
                        if vals[cnt] > 0:
                            netwerklink, type_probleem_aan_infra = get_netw_type_probl(columns[cnt])
                            aantal = vals[cnt]
                            row2insert = dict(
                                jaar=jaar,
                                maand=maand,
                                aantal=aantal,
                                gemeente=gemeente,
                                provincie=provincie,
                                netwerklink=netwerklink,
                                type_probleem_aan_infra=type_probleem_aan_infra
                            )
                            ds.insert_row(tn, row2insert)
    rec_info.end_loop()


cfg = mf_env.init_env("meldpuntfietspaden", __file__)
ds = Datastore(cfg)
fn = cfg['MeldpuntFietspaden']['ffn']
populate_table(fn, "meldpuntfietspaden")
logging.info("End Application")

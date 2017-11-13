#!/opt/csw/bin/python3

"""
This script will process the information for one month.
URL for consolidated report:
select jaar, maand, sum(aantal) as aantal, gemeente, provincie, netwerk, type_probleem_aan_infra from meldpuntfietspaden
group by jaar, maand, gemeente, provincie, netwerk, type_probleem_aan_infra
"""

import logging
import os
from lib.datastore import Datastore
from lib import mf_env

id4netwerk = "gewest_"

maandnaam = [
    'januari',
    'februari',
    'maart',
    'april',
    'mei',
    'juni',
    'juli',
    'augustus',
    'september',
    'oktober',
    'november',
    'december'
]


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
                prefix = id4netwerk
        column_array.append(prefix + item)
    return column_array


def get_gemeente(naam):
    """
    This method will get the gemeente and the provincie. The gemeente naam must be found in the dim_element table.
    Then provincie is found using the URIs.

    :param naam: Gemeente naam to be found.

    :return: gemeente, provincie
    """
    if "district " in naam:
        naam = "Antwerpen"
    uri = ds.get_uri("gemeente", naam)
    gemeente = ds.get_el(naam, "gemeente")
    if uri:
        provincie_naam = get_provincie(uri)
        # It may be a good idea to rework get_provincie(uri) instead.
        provincie = ds.get_el(provincie_naam, "provincie")
        if provincie:
            return gemeente, provincie
        else:
            logging.error("Found URI for gemeente {g}, but no provincie...".format(g=uri))
    # Error with collecting gemeente or provincie.
    return False, False


def get_netw_type_probl(cat):
    """
    This procedure will get network type and type probleem from the category label

    :param cat: Category label. If category label contains value from netwerk then this is 'gewestfietspaden'
    (dim_element_id: 3333), otherwise the netwerk dimensie is 'gemeentefietspaden' (dim_element_id: 3334)

    :return: element for netwerk and element for type probleem aan infra.
    """
    if id4netwerk in cat:
        netwerk_el = 3333
        # Strip netwerk_el from cat
        cat = cat[len(id4netwerk):]
    else:
        netwerk_el = 3334
    probl_aan_infra = ds.tx_cat2el_id(cat)
    return netwerk_el, probl_aan_infra


def get_provincie(uri):
    """
    This method will calculate the provincie for a gemeente. Get the NIS code, take first digit and add '0000' to it.
    This is the URI for the Provincie. Then find the provincie.

    :param uri: URI for the gemeente.

    :return: naam for the provincie
    """
    prefix = "http://id.fedstats.be/nis/"
    suffix = "#id"
    # Find number for the gemeente
    nr = uri[len(prefix)]
    if nr == '2':
        # Vlaams Brabant has a specific Provincie ID
        prov_id = '0001'
    else:
        prov_id = '0000'
    prov_uri = prefix + nr + prov_id + suffix
    return ds.get_waarde_from_uri(prov_uri)


def populate_table(fn, tn, jaar, maand):
    """
    This function will convert an output file of application Meldpunt Fietspaden and load the data in the indicator
    table meldpuntfietspaden.
    :type jaar: int
    :type  maand: int

    :param fn: Path to the csv file to handle.

    :param tn: Table name to be loaded.

    :param jaar: Jaar for which the report runs.

    :param maand: Maand for which the report runs.
    :return:
    """
    rec_info = mf_env.LoopInfo(tn, 20)
    # jaar = 2014
    # maand = 1
    # remove existing records for the period
    ds.remove_measurements_el(jaar=jaar, label=maandnaam[maand-1])
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
                        if int(vals[cnt]) > 0:
                            netwerk, type_probleem_aan_infra = get_netw_type_probl(columns[cnt])
                            aantal = vals[cnt]
                            label= maandnaam[maand-1]
                            periode = "{jaar} {label}".format(jaar=jaar, label=label)
                            # dagnr = ds.get_dagnr(jaar=jaar, maand=maand)
                            row2insert = dict(
                                periode=periode,
                                jaar=jaar,
                                label=label,
                                aantal=aantal,
                                gemeente=gemeente,
                                provincie=provincie,
                                netwerk=netwerk,
                                type_probleem_aan_infra=type_probleem_aan_infra
                            )
                            ds.insert_row(tn, row2insert)
    rec_info.end_loop()


cfg = mf_env.init_env("meldpuntfietspaden", __file__)
ds = Datastore(cfg)
reportdir = cfg['MeldpuntFietspaden']['reportdir']
filelist = [file for file in os.listdir(reportdir) if os.path.splitext(file)[1] == ".csv"]
for idx, file in enumerate(filelist):
    filename = os.path.join(reportdir, file)
    year_month = os.path.splitext(file)[0]
    year, month = year_month.split("_")
    print("Ready to populate table for {year} {month} from {fn}".format(year=year, month=month, fn=filename))
    populate_table(filename, "mf_el", int(year), int(month))

# filename = cfg['MeldpuntFietspaden']['ffn']
# populate_table(filename, "meldpuntfietspaden")
logging.info("End Application")

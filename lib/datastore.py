#!/opt/csw/bin/python3

"""
This module consolidates all actions for the SQL database connection.
SQL database is SQLite. SQLAlchemy is not available as a library so it is not used.
"""

import logging
from lib import mf_env
import sqlite3


class Datastore:

    def __init__(self, config, test=False):
        """
        Instantiate the class into an object for the datastore.

        :param config: Config object to connect to the database

        :param test: Boolean, indicating if this is a test run.

        :return: Object to handle datastore commands.
        """
        self.dbConn, self.cur = self._connect2db(config, test)

    @staticmethod
    def _connect2db(config, test):
        """
        Internal method to create a database connection and a cursor. This method is called during object
        initialization.
        Note that sqlite connection object does not test the Database connection. If database does not exist, this
        method will not fail. This is expected behaviour, since it will be called to create databases as well.

        :param config: Config object containing connection information

        :param test: Boolean, indicating if this is a test run.

        :return: Database handle and cursor for the database.
        """
        logging.debug("Creating Datastore object and cursor")
        if test:
            db = config['Main']['db_for_test']
        else:
            db = config['Main']['db']
        db_conn = sqlite3.connect(db)
        db_conn.row_factory = sqlite3.Row
        logging.debug("Datastore object and cursor are created")
        return db_conn, db_conn.cursor()

    def create_apex_tables(self):
        """
        This function will drop and recreate the tables that are provided from APEX.
        For now these are the tables dimensie and dim_element.
        :return:
        """
        self.create_table_dim_element()
        self.create_table_dimensie()
        self.create_table_frequenties()

    def create_indicator_table(self):
        self.create_table_meldpuntfietspaden()
        self.create_table_mf_el()

    def create_meldingtx_table(self):
        self.create_table_meldingtx()

    def create_table_dimensie(self):
        query = "DROP TABLE IF EXISTS dimensie"
        self.dbConn.execute(query)
        query = """
        CREATE TABLE IF NOT EXISTS dimensie
            (dimensie_id integer primary key,
             waarde text NOT NULL,
             type text,
             kolomnaam text)
        """
        self.dbConn.execute(query)
        logging.info("Table dimensie created")
        return

    def create_table_dim_element(self):
        query = "DROP TABLE IF EXISTS dim_element"
        self.dbConn.execute(query)
        query = """
        CREATE TABLE IF NOT EXISTS dim_element
            (dim_element_id integer primary key,
             dimensie_id integer NOT NULL,
             waarde text NOT NULL,
             uri text,
             FOREIGN KEY (dimensie_id) REFERENCES dimensie(dimensie_id))
        """
        self.dbConn.execute(query)
        logging.info("Table dim_element created.")
        return

    def create_table_frequenties(self):
        query = "DROP TABLE IF EXISTS frequenties"
        self.dbConn.execute(query)
        query = """
            CREATE TABLE IF NOT EXISTS frequenties
                (dagnr integer NOT NULL,
                 datum text NOT NULL,
                 dag_week text NOT NULL,
                 dag integer NOT NULL,
                 maand text NOT NULL,
                 jaar integer NOT NULL,
                 maandnr integer NOT NULL,
                 kwartaal text NOT NULL,
                 maand_label text NOT NULL,
                 kwartaal_label text NOT NULL,
                 schooljaar_label text NOT NULL)
            """
        self.dbConn.execute(query)
        logging.info("Table frequenties created.")
        return

    def create_table_meldpuntfietspaden(self):
        query = "DROP TABLE IF EXISTS meldpuntfietspaden"
        self.dbConn.execute(query)
        query = """
        CREATE TABLE IF NOT EXISTS meldpuntfietspaden
            (jaar integer NOT NULL,
             maand integer NOT NULL,
             aantal integer NOT NULL,
             gemeente text NOT NULL,
             provincie text NOT NULL,
             netwerk text NOT NULL,
             type_probleem_aan_infra text NOT NULL)
        """
        self.dbConn.execute(query)
        logging.info("Table meldpuntfietspaden created.")
        return

    def create_table_mf_el(self):
        query = "DROP TABLE IF EXISTS mf_el"
        self.dbConn.execute(query)
        query = """
        CREATE TABLE IF NOT EXISTS mf_el
            (periode text NOT NULL,
             jaar integer NOT NULL,
             label text NOT NULL,
             aantal integer NOT NULL,
             gemeente integer NOT NULL,
             provincie integer NOT NULL,
             netwerk integer NOT NULL,
             type_probleem_aan_infra integer NOT NULL)
        """
        self.dbConn.execute(query)
        logging.info("Table meldpuntfietspaden created.")
        return

    def create_table_meldingtx(self):
        query = "DROP TABLE IF EXISTS meldingtx"
        self.dbConn.execute(query)
        query = """
        CREATE TABLE IF NOT EXISTS meldingtx
            (melding text NOT NULL,
             dim_element_id integer NOT NULL,
             dimensie_id integer NOT NULL,
             waarde text NOT NULL,
             FOREIGN KEY (dim_element_id) REFERENCES dim_element(dim_element_id))
        """
        self.dbConn.execute(query)
        logging.info("Table meldingtx created.")
        return

    def insert_row(self, tablename, rowdict):
        columns = ", ".join(rowdict.keys())
        values_template = ", ".join(["?"] * len(rowdict.keys()))
        query = "insert into  {tn} ({cols}) values ({vt})".format(tn=tablename, cols=columns, vt=values_template)
        values = tuple(rowdict[key] for key in rowdict.keys())
        self.dbConn.execute(query, values)
        self.dbConn.commit()
        return

    def populate_table(self, fn, tn):
        """
        This function will get a csv file and populate the corresponding table from it.
        First line in the file has the column names, separated with ";".
        All subsequent lines are data. Each data line is converted into a dictionary line that is sent to the datastore
        handle for load in the table.

        :param fn: Path to the csv file to handle.

        :param tn: Table name to be loaded.

        :return:
        """
        rec_info = mf_env.LoopInfo(tn, 100)
        with open(fn) as fh:
            column_line = fh.readline().strip().split(";")
            columns = [col.strip("\"").lower() for col in column_line]
            for line in fh:
                vals = [val.strip("\"") for val in line.strip().split(";")]
                row2insert = {}
                for cnt in range(len(columns)):
                    row2insert[columns[cnt]] = vals[cnt]
                self.insert_row(tn, row2insert)
                rec_info.info_loop()
        rec_info.end_loop()

    def get_element(self, dim_element_id):
        """
        This method will return the waarde from an element for a specific ID.

        :param dim_element_id:

        :return: Waarde for the element, or False if not found.
        """
        query = "SELECT waarde FROM dim_element WHERE dim_element_id=?"
        self.cur.execute(query, (dim_element_id,))
        rows = self.cur.fetchall()
        if len(rows) == 0:
            return False
        else:
            return rows[0]['waarde']

    def get_indicator_report(self, indicator_id):
        """
        This script will extract the data for the csv file for loading into the MOW Dataroom application.

        :param indicator_id: ID for the indicator (Be careful: Production vs Development).

        :return: list of keys and list of dictionaries containing the data.
        """
        query = """
        SELECT {indic_id} as indicatorfiche_id, 'J' as actief, sum(aantal) as aantal, periode, jaar, label,
               gemeente, provincie, netwerk, type_probleem_aan_infra
        FROM mf_el
        GROUP BY periode, jaar, label, gemeente, provincie, netwerk, type_probleem_aan_infra
        """.format(indic_id=indicator_id)
        self.cur.execute(query)
        col_name_tuples = self.cur.description
        hdr = [col_name[0] for col_name in col_name_tuples]
        return hdr, self.cur.fetchall()

    def tx_cat2el(self, cat):
        """
        This method will return the translation for a specific category label. It will use function get_element instead
        of having one single but slightly more complex query.

        :param cat:

        :return:
        """
        query = "SELECT dim_element_id FROM meldingtx WHERE melding=?"
        self.cur.execute(query, (cat,))
        rows = self.cur.fetchall()
        if len(rows) == 0:
            return False
        else:
            dim_element_id = rows[0]['dim_element_id']
            return self.get_element(dim_element_id)

    def tx_cat2el_id(self, cat):
        """
        This method will return the translation for a specific category label. It will use function get_element instead
        of having one single but slightly more complex query.

        :param cat:

        :return:
        """
        query = "SELECT dim_element_id FROM meldingtx WHERE melding=?"
        self.cur.execute(query, (cat,))
        rows = self.cur.fetchall()
        if len(rows) == 0:
            return False
        else:
            dim_element_id = rows[0]['dim_element_id']
            return dim_element_id

    def get_uri(self, dimensie, waarde):
        """
        This method will return the URI for element waarde for dimensie.

        :param dimensie: waarde for the dimensie

        :param waarde: waarde for the element.

        :return: URI for the element, empty if found but no URI available, False if not found.
        """
        query = """
        SELECT uri
        FROM dim_element el, dimensie dim
        WHERE dim.waarde = ?
          AND el.waarde = ?
          AND dim.dimensie_id = el.dimensie_id
        """
        self.cur.execute(query, (dimensie, waarde))
        rows = self.cur.fetchall()
        if len(rows) == 0:
            # element or dimensie not found
            return False
        else:
            return rows[0]['uri']

    def get_dagnr(self, jaar, maand):
        """
        This function will return the dagnr for a specific jaar and maand number.

        :param jaar: Year

        :param maand: Number of the month

        :return: dagnr
        """
        query = "SELECT dagnr FROM frequenties WHERE jaar=? AND maandnr=?"
        self.cur.execute(query, (jaar, maand))
        rows = self.cur.fetchall()
        if len(rows) == 0:
            return False
        else:
            return rows[0]['dagnr']

    def get_el(self, waarde, dimensie):
        """
        This function will return the dim_element_id for a specific waarde and dimensie (kolomnaam!)

        :param waarde: Waarde from the element

        :param dimensie: Dimensie (kolomnaam) from the element

        :return: element ID.
        """
        query = """
        SELECT dim_element_id
        FROM dim_element el, dimensie dim
        WHERE el.waarde = ?
          AND dim.kolomnaam = ?
        """
        self.cur.execute(query, (waarde, dimensie))
        rows = self.cur.fetchall()
        if len(rows) == 0:
            return False
        else:
            return rows[0]['dim_element_id']

    def get_waarde_from_uri(self, uri):
        """
        This method will return the element waarde for a specific URI.

        :param uri: URI for which the waarde is required.

        :return: Waarde for the URI. Empty if URI found but no waarde available (should never happen). False if URI not
        found.
        """
        query = """
        SELECT waarde
        FROM dim_element el
        WHERE uri = ?
        """
        self.cur.execute(query, (uri,))
        rows = self.cur.fetchall()
        if len(rows) == 0:
            # element or dimensie not found
            return False
        else:
            return rows[0]['waarde']

    def remove_measurements(self, jaar, maand):
        """
        This method will remove all records for a specific month.

        :param jaar:

        :param maand:

        :return:
        """
        query = "DELETE FROM meldpuntfietspaden WHERE jaar=? AND maand=?"
        self.cur.execute(query, (jaar, maand))
        return

    def remove_measurements_el(self, jaar, label):
        """
        This method will remove all records for a specific month.

        :param jaar:

        :param maand:

        :return:
        """
        query = "DELETE FROM mf_el WHERE jaar=? AND label=?"
        self.cur.execute(query, (jaar, label))
        return
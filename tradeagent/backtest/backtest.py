from pandas import read_sql_query
from pandas.io.sql import DatabaseError
from tradeagent.utils import db_connect
from pathlib import Path


class History(object):
    """Class to retrieve and store historical price data
    """

    def __init__(self, db_file):
        """
        :param db_file: Full path to the location of the sqlite file
        """

        self.db_file = db_file
        if not Path(self.db_file).is_file():
            raise FileNotFoundError

        self.query = None
        self.df = None

    def retrieve_data(self):
        """
        Queries the sqlite database using the instance query property
        Sets the instance DataFrame to the results

        :return: True if successful, false otherwise
        """

        try:
            self.df = read_sql_query(self.query, db_connect(self.db_file))
        except DatabaseError:
            raise

        return True

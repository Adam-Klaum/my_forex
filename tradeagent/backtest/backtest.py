from pandas import read_sql_query
from tradeagent.util import db_connect
from pathlib import Path


class History:

    def __init__(self, db_file):

        self.db_file = db_file
        if not Path(self.db_file).is_file():
            raise FileNotFoundError

        self.query = None
        self.df = None

    def retrieve_data(self):

        self.df = read_sql_query(self.query, db_connect(self.db_file))
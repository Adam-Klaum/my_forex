from pandas import read_sql_query
from pandas.io.sql import DatabaseError
from tradeagent.utils import db_connect
from pathlib import Path
from tradeagent.config import config
from decimal import Decimal


def price_convert(instrument, price):
    return int(Decimal(price) * config.fx_info[instrument].multiplier)


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

        # Converting all price columns to integers based on their instrument multiplier

        price_columns = ['bid_open', 'bid_high', 'bid_low', 'bid_close', 'ask_open', 'ask_high', 'ask_low', 'ask_close']

        for column in price_columns:
            self.df[column] = self.df.apply(lambda row: price_convert(row['instrument'], row[column]), axis=1)

        # Adding the spread column for each row

        self.df['spread'] = abs(self.df.ask_close - self.df.bid_close)

        return True

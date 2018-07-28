from pandas import read_sql_query
from pandas.io.sql import DatabaseError
from tradeagent.utils import db_connect
from pathlib import Path
from tradeagent.config import config
from decimal import Decimal


def convert_price(instrument, price):

    return int(Decimal(price) * config.fx_info[instrument].multiplier)


def get_data(db_file, query):

    if not Path(db_file).is_file():
            raise FileNotFoundError

    df = read_sql_query(query, db_connect(db_file))

    # Converting all price columns to integers based on their instrument multiplier

    price_columns = ['bid_open', 'bid_high', 'bid_low', 'bid_close', 'ask_open', 'ask_high', 'ask_low', 'ask_close']

    for column in price_columns:
        df[column] = df.apply(lambda row: convert_price(row['instrument'], row[column]), axis=1)

    # Adding the spread column for each row

    df['spread'] = abs(df.ask_close - df.bid_close)

    return df

from pandas import read_sql_query
from tradeagent.utils import db_connect
from pathlib import Path
from tradeagent.config import config
import numpy as np


def get_data(instrument, db_file, query):

    if not Path(db_file).is_file():
            raise FileNotFoundError

    multiplier = config.fx_info[instrument].multiplier

    df = read_sql_query(query, db_connect(db_file))

    # Converting all price columns to integers based on their instrument multiplier

    price_columns = ['bid_open', 'bid_high', 'bid_low', 'bid_close', 'ask_open', 'ask_high', 'ask_low', 'ask_close']

    for column in price_columns:
        df[column] = np.round(np.multiply(df[column].astype(float).values, multiplier))

    # Adding the spread column for each row

    df['spread'] = abs(df.ask_close - df.bid_close)

    return df

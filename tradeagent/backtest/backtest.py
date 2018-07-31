from pandas import read_csv
from pathlib import Path
from tradeagent.config import config


def get_data(instrument, csv_file):

    column_types = {'instrument': 'category',
                    'bid_open': 'float32',
                    'bid_high': 'float32',
                    'bid_low': 'float32',
                    'bid_close': 'float32',
                    'ask_open': 'float32',
                    'ask_high': 'float32',
                    'ask_low': 'float32',
                    'ask_close': 'float32'
                    }

    if not Path(csv_file).is_file():
            raise FileNotFoundError

    multiplier = config.fx_info[instrument].multiplier

    df = read_csv(csv_file, dtype=column_types, parse_dates=['candle_time'], infer_datetime_format=True)

    # Converting all price columns to integers based on their instrument multiplier

    price_columns = ['bid_open', 'bid_high', 'bid_low', 'bid_close', 'ask_open', 'ask_high', 'ask_low', 'ask_close']

    for column in price_columns:
        df[column] = (df[column] * multiplier).astype('uint32')

    # Adding the spread column for each row

    df['spread'] = abs(df.ask_close - df.bid_close).astype('uint8')

    return df

from tradeagent.backtest import get_data
from tradeagent.config import root
from tradeagent.indicators import apply_adx
from tradeagent.candle import candle_converter
import numpy as np
import pandas as pd


def full_data():

    csv_file = root / 'raw_candle.csv'
    hist = get_data('EUR_USD', csv_file)

    new_df = candle_converter(hist, '30T')

    apply_adx(hist, 'bid')
    apply_adx(hist, 'ask')
    print(hist.info())
    print(hist.head())


def test_calc():

    # Test set of 20 random integers
    df = pd.DataFrame({'base': [15, 16, 2, 16, 14,
                                1, 18, 18, 4, 7,
                                4, 18, 19, 13, 16,
                                11, 1, 8, 1, 9]})

    # Empty array to hold calculated values
    calc_data = np.empty((20, 1))

    period = 14

    for idx, value in enumerate(df.base):

        # Seeding the first element of the calculated array
        if idx == 0:
            calc_data[idx] = 5

        else:
            calc_data[idx] = (calc_data[idx - 1] * (period - 1) + df.base.iloc[idx]) / period

    df['calculated'] = calc_data

    print(df)


full_data()

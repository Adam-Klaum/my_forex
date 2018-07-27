from tradeagent.config import config
from decimal import Decimal
from numpy import subtract, roll, clip, ma


class DM(object):
    """Takes a DataFrame in a specific format and generates +DM and -DM"""

    def __init__(self, df):
        """
        :param df: DataFrame of historical candle data for a SINGLE forex instrument

        Requires the following columns to be present in the DataFrame:

        instrument     object
        bid_high       object
        bid_low        object
        ask_high       object
        ask_low        object

        """

        self.df = df

    def row_calc(self, current, prev):
        """Calculates the spread given a bid and ask close

        Consults the project-wide config to determine the correct
        multiplier for a given instrument

        :param instrument: Forex instrument (i.e. EUR_USD)
        :param bid_close: Closing price of the bid candle
        :param ask_close: Closing price of the ask candle
        :return: Calculated spread as a string
        """

        # if both are negative then zero for both
        # if one is negative then it is zero
        # if both are positive then the lesser one is zero


        # spread = abs(Decimal(ask_close) - Decimal(bid_close))
        # spread *= Decimal(config.fx_info[instrument].multiplier)
        #
        # return str(spread.quantize(Decimal('.1')))

    def roll_column(self, column):

        column = self.df[column].values
        column_prev = roll(column, 1)
        column_prev[0] = 0
        column[0] = 0
        return column, column_prev

    def dm_calc(self, high_diff, low_diff):

        dm_plus = []
        dm_minus = []

        for high, low in zip(high_diff, low_diff):

            if high <= 0:
                if low <= 0:
                    dm_plus.append(0)
                    dm_minus.append(0)
                else:
                    dm_plus.append(0)
                    dm_minus.append(low)

            elif low <= 0:
                if high <= 0:
                    dm_plus.append(0)
                    dm_minus.append(0)
                else:
                    dm_plus.append(high)
                    dm_minus.append(0)

        return dm_plus, dm_minus

    def apply(self):
        """Applies the indicator to each row of the Data Frame"""

        bid_high, bid_high_prev = self.roll_column('bid_high')
        bid_low, bid_low_prev = self.roll_column('bid_low')
        ask_high, ask_high_prev = self.roll_column('ask_high')
        ask_low, ask_low_prev = self.roll_column('ask_low')

        bid_high_diff = subtract(bid_high, bid_high_prev).clip(0)
        bid_low_diff = subtract(bid_low, bid_low_prev).clip(0)
        ask_high_diff = subtract(ask_low, ask_low_prev).clip(0)
        ask_low_diff = subtract(ask_high, ask_high_prev).clip(0)

        bid_high_diff = ma.array(bid_high_diff, mask=bid_high_diff > bid_low_diff)
        bid_low_diff = ma.array(bid_low_diff, mask=bid_low_diff > bid_high_diff)
        ask_high_diff = ma.array(ask_high_diff, mask=ask_high_diff > ask_low_diff)
        ask_low_diff = ma.array(ask_low_diff, mask=ask_low_diff > ask_high_diff)

        self.df['bid +DM'] = bid_high_diff.filled(bid_high_diff)
        self.df['bid -DM'] = bid_low_diff.filled(bid_low_diff)
        self.df['ask +DM'] = ask_high_diff.filled(ask_high_diff)
        self.df['ask -DM'] = ask_low_diff.filled(ask_low_diff)


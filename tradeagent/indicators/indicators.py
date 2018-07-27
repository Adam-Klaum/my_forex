from tradeagent.config import config
from decimal import Decimal
from numpy import subtract, roll, vectorize, array


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


        spread = abs(Decimal(ask_close) - Decimal(bid_close))
        spread *= Decimal(config.fx_info[instrument].multiplier)

        return str(spread.quantize(Decimal('.1')))

    def roll_column(self, column):

        column = self.df[column].values.astype(float)
        column_prev = roll(column, 1)
        column_prev[0] = 0
        return column, column_prev

    def dm_calc(self, dm_plus, dm_minus):

        if dm_plus <= 0:
            if dm_minus <= 0:
                return array(0, 0)
            else:
                return array(0, dm_minus)

        elif dm_minus <= 0:
            if dm_plus <= 0:
                return array(0, 0)
            else:
                return array(dm_plus, 0)

    def apply(self):
        """Applies the indicator to each row of the Data Frame"""

        dm_calc_vec = vectorize(self.dm_calc)


        # bid_high, bid_high_prev = self.roll_column('bid_high')
        # bid_low, bid_low_prev = self.roll_column('bid_low')
        # ask_high, ask_high_prev = self.roll_column('ask_high')
        # ask_low, ask_low_prev = self.roll_column('ask_low')
        #
        # bid_high_diff = subtract(bid_high, bid_high_prev)
        # bid_low_diff = subtract(bid_low, bid_low_prev)
        # ask_high_diff = subtract(ask_low, ask_low_prev)
        # ask_low_diff = subtract(ask_high, ask_high_prev)
        #
        # dm_result = dm_calc_vec(bid_high_diff, bid_low_diff)
        #
        # print(dm_result.shape)
        #
        # print(dm_result)
        #
        # # self.df['bid_high_diff'] = self.df.bid_high - self.df.bid_high_prev
        # self.df['bid_low_diff'] = self.df.bid_low - self.df.bid_low_prev
        # self.df['ask_high_diff'] = self.df.ask_high - self.df.ask_high_prev
        # self.df['ask_low_diff'] = self.df.ask_low - self.df.ask_low_prev

        # self.df['bid +DI'] = self.df.apply(lambda row: self.row_calc(row['bid_high'],
        #                                                          row['bid_high_prev']), axis=1)
        #
        # self.df['bid -DI'] = self.df.apply(lambda row: self.row_calc(row['bid_low'],
        #                                                          row['bid_low_prev']), axis=1)
        #
        # self.df['ask +DI'] = self.df.apply(lambda row: self.row_calc(row['ask_high'],
        #                                                          row['ask_high_prev']), axis=1)
        #
        # self.df['ask -DI'] = self.df.apply(lambda row: self.row_calc(row['ask_low'],
        #                                                          row['ask_low_prev']), axis=1)


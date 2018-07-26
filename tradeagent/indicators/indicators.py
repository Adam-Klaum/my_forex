from tradeagent.config import config
from decimal import Decimal


class Spread(object):
    """Takes a DataFrame in a specific format and generates the Spread
    """

    def __init__(self, df):
        """
        :param df: DataFrame of historical candle data for a forex instrument

        Requires the following columns to be present in the DataFrame:

        instrument     object
        bid_close      object
        ask_close      object
        """

        self.df = df

    def row_calc(self, instrument, bid_close, ask_close):
        """Calculates the spread given a bid and ask close

        Consults the project-wide config to determine the correct
        multiplier for a given instrument

        :param instrument: Forex instrument (i.e. EUR_USD)
        :param bid_close: Closing price of the bid candle
        :param ask_close: Closing price of the ask candle
        :return: Calculated spread as a string
        """
        spread = abs(Decimal(ask_close) - Decimal(bid_close))
        spread *= Decimal(config.fx_info[instrument].multiplier)

        return str(spread.quantize(Decimal('.1')))

    def apply(self):
        """Applies an indicator to each row of the Data Frame"""

        self.df['spread'] = self.df.apply(lambda row: self.row_calc(row['instrument'],
                                                                    row['bid_close'],
                                                                    row['ask_close']), axis=1)


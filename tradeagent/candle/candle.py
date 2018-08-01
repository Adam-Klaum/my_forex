from datetime import datetime
from typing import Tuple
from decimal import Decimal, DecimalException
from multiprocessing import Process
from tradeagent.config import config
from pandas import Grouper
from collections import OrderedDict


def candle_converter(df, frequency):
    """ Convert passed in dataframe of forex prices from one time frame to another

    Create a new dataframe of OHLC candle data aggregated by the specified offset alias

    http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases

    :param df: Dataframe containing the source OHLC data
    :param frequency: Offset alias
    :return: New converted dataframe
    """

    grouped = df.groupby(Grouper(key='candle_time', freq=frequency))

    new_df = grouped.agg(OrderedDict([('instrument', 'first'),
                                      ('bid_open', 'first'),
                                      ('bid_high', 'max'),
                                      ('bid_low', 'min'),
                                      ('bid_close', 'last'),
                                      ('ask_open', 'first'),
                                      ('ask_high', 'max'),
                                      ('ask_low', 'min'),
                                      ('ask_close', 'last')
                                      ]))

    return new_df


class Candle(object):

    def __init__(self,
                 dt: datetime,
                 inst: str,
                 time_frame: str,
                 bid: Tuple[str, str, str, str],
                 ask: Tuple[str, str, str, str]
                 ):

        self.dt = dt
        self.inst = inst
        self.time_frame = time_frame

        try:
            self.bid_o, self.bid_h, self.bid_l, self.bid_c = [int(Decimal(i) * config.fx_info[inst].multiplier) for i in bid]
            self.ask_o, self.ask_h, self.ask_l, self.ask_c = [int(Decimal(i) * config.fx_info[inst].multiplier) for i in ask]

        except (ValueError, DecimalException):
            raise

        self.spread = abs(self.ask_c - self.bid_c)


class CandleMaker(Process):

    def __init__(self, tick_queue):
        super(CandleMaker, self).__init__()
        self.tick_queue = tick_queue

    def run(self):

        while True:

            while not self.tick_queue.empty():
                msg = self.tick_queue.get()

                if msg == 'KILL':
                    return

                date_part, minute, _ = msg.time.split(':')
                minute = int(minute)

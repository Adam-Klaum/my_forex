from datetime import datetime
from typing import Tuple
from decimal import Decimal, DecimalException
from multiprocessing import Process


class ProcessKilled(Exception):
    pass


class Candle(object):

    #TODO parameterize the spread multiplier

    def __init__(self,
                 dt: datetime,
                 inst: str,
                 time_frame: str,
                 bid: Tuple[float, float, float, float],
                 ask: Tuple[float, float, float, float]
                 ):

        self.dt = dt
        self.inst = inst
        self.time_frame = time_frame

        try:
            self.bid_o, self.bid_h, self.bid_l, self.bid_c = [Decimal(str(i)) for i in bid]
            self.ask_o, self.ask_h, self.ask_l, self.ask_c = [Decimal(str(i)) for i in ask]

        except (ValueError, DecimalException):
            raise

        self.spread = Decimal(abs(self.ask_c - self.bid_c) * 10000)


class CandleMaker(Process):

    def __init__(self, tick_queue):
        super(CandleMaker, self).__init__()
        self.tick_queue = tick_queue

    def run(self):

        while True:

            while not self.tick_queue.empty():
                msg = self.tick_queue.get()

                if msg == 'KILL':
                    print('got a kill')
                    raise ProcessKilled

                date_part, minute, _ = msg.time.split(':')
                minute = int(minute)

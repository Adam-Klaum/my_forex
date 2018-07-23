from pytest import *
from tradeagent.candle.candle import Candle, CandleMaker, ProcessKilled
from datetime import datetime
from decimal import Decimal, DecimalException
from multiprocessing import Queue, Pipe, Process


def test_candle_creation():

    candle = Candle(datetime.now(),
                    'EUR_USD',
                    'M1',
                    (1.16769, 1.16780, 1.16767, 1.16780),
                    (1.16782, 1.16794, 1.16782, 1.16794)
                    )

    assert candle.bid_o == Decimal('1.16769')
    assert candle.bid_h == Decimal('1.16780')
    assert candle.bid_l == Decimal('1.16767')
    assert candle.bid_c == Decimal('1.16780')

    assert candle.ask_o == Decimal('1.16782')
    assert candle.ask_h == Decimal('1.16794')
    assert candle.ask_l == Decimal('1.16782')
    assert candle.ask_c == Decimal('1.16794')

    assert candle.spread == Decimal('1.4')


def test_candle_init_raises_exceptions():

    with raises(ValueError):
        candle1 = Candle(datetime.now(),
                         'EUR_USD',
                         'M1',
                         (1.16769, 1.16780, 1.16767, 1.16780, 1),
                         (1.16782, 1.16794, 1.16782, 1.16794)
                         )

    with raises(DecimalException):
        candle2 = Candle(datetime.now(),
                         'EUR_USD',
                         'M1',
                         (1.16769, 1.16780, 1.16767, 1.16780),
                         (1.16782, 'A', 1.16782, 1.16794)
                         )


def test_candle_maker_kill():

    tick_queue = Queue()
    candle_proc = CandleMaker(tick_queue)

    candle_proc.start()
    tick_queue.put('KILL')
    candle_proc.join()

    assert candle_proc.exitcode


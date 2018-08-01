from pytest import *
from tradeagent.candle import Candle, CandleMaker, candle_converter
from tradeagent.backtest import get_data
from datetime import datetime
from decimal import Decimal, DecimalException
from multiprocessing import Queue
from tradeagent.config import root

csv_file = root / 'tests' / 'backtest' / 'raw_candle.csv'


@fixture
def hist_data():

    hist = get_data('EUR_USD', csv_file)
    return hist


def test_candle_converter(hist_data):

    new_df = candle_converter(hist_data, '30T')

    assert 600441 == new_df.bid_open.sum()
    assert 600668 == new_df.bid_high.sum()
    assert 600201 == new_df.bid_low.sum()
    assert 600531 == new_df.bid_close.sum()

    new_df.to_csv('candle_converter.csv')

    return


def test_candle_creation():

    candle = Candle(datetime.now(),
                    'EUR_USD',
                    'M1',
                    ('1.16769', '1.16780', '1.16767', '1.16780'),
                    ('1.16782', '1.16794', '1.16782', '1.16794')
                    )

    assert candle.bid_o == 116769
    assert candle.bid_h == 116780
    assert candle.bid_l == 116767
    assert candle.bid_c == 116780

    assert candle.ask_o == 116782
    assert candle.ask_h == 116794
    assert candle.ask_l == 116782
    assert candle.ask_c == 116794

    assert candle.spread == 14


def test_candle_init_raises_exceptions():

    with raises(ValueError):
        candle1 = Candle(datetime.now(),
                         'EUR_USD',
                         'M1',
                         ('1.16769', '1.16780', '1.16767', '1.16780', '1'),
                         ('1.16782', '1.16794', '1.16782', '1.16794'))

    with raises(DecimalException):
        candle2 = Candle(datetime.now(),
                         'EUR_USD',
                         'M1',
                         ('1.16769', '1.16780', '1.16767', '1.16780'),
                         ('1.16782', 'A', '1.16782', '1.16794'))


def test_candle_maker_kill():

    tick_queue = Queue()
    candle_proc = CandleMaker(tick_queue)

    candle_proc.start()
    tick_queue.put('KILL')
    candle_proc.join()

    assert not candle_proc.exitcode


from pytest import *
from tradeagent.backtest import History
from tradeagent.config import root
from pandas.io.sql import DatabaseError
from tradeagent.indicators import Spread
from decimal import Decimal, getcontext

def test_history_load():

    db_file = root / 'tests' / 'backtest' / 'test.sqlite3'

    hist = History(db_file)
    hist.query = 'SELECT * FROM raw_candle;'
    hist.retrieve_data()

    assert hist.df.shape[0] == 100

    with raises(FileNotFoundError):
        hist2 = History('not a file')
        hist2.query = 'SELECT * FROM raw_candle;'
        hist2.retrieve_data()

    with raises(DatabaseError):
        hist3 = History(db_file)
        hist3.retrieve_data()


def test_spread_indicator():

    db_file = root / 'tests' / 'backtest' / 'test.sqlite3'
    hist = History(db_file)
    hist.query = 'SELECT * FROM raw_candle;'
    hist.retrieve_data()

    spread = Spread(hist.df)
    spread.apply()

    print(hist.df.info())

    assert sum([Decimal(spread) for spread in hist.df['spread']]) == Decimal('288.6')




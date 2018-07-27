from pytest import raises
from tradeagent.backtest import History
from tradeagent.config import root
from pandas.io.sql import DatabaseError
from tradeagent.indicators import DM


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


def test_spread():

    db_file = root / 'tests' / 'backtest' / 'test.sqlite3'

    hist = History(db_file)
    hist.query = 'SELECT * FROM raw_candle;'
    hist.retrieve_data()

    assert hist.df.spread.sum() == 2886


def test_dm_indicator():

    db_file = root / 'tests' / 'backtest' / 'test.sqlite3'

    hist = History(db_file)
    hist.query = 'SELECT * FROM raw_candle;'
    hist.retrieve_data()
    dm = DM(hist.df)
    dm.apply()






from pytest import *
from tradeagent.backtest import History
from tradeagent.config import root


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


from pytest import raises
from tradeagent.backtest import get_data
from tradeagent.config import root
from pandas.io.sql import DatabaseError
from tradeagent.indicators import apply_dm


def test_history_load():

    db_file = root / 'tests' / 'backtest' / 'test.sqlite3'

    hist = get_data(db_file, 'SELECT * FROM raw_candle;')

    assert hist.shape[0] == 100

    with raises(FileNotFoundError):
        hist2 = get_data('not a file', 'SELECT * FROM raw_candle;')

    with raises(DatabaseError):
        hist3 = get_data(db_file, 'Rumplestiltskin')
        hist3.retrieve_data()


def test_spread():

    db_file = root / 'tests' / 'backtest' / 'test.sqlite3'

    hist = get_data(db_file, 'SELECT * FROM raw_candle;')

    assert hist.spread.sum() == 2886


def test_dm_indicator():

    db_file = root / 'tests' / 'backtest' / 'test.sqlite3'

    hist = get_data(db_file, 'SELECT * FROM raw_candle;')
    apply_dm(hist)


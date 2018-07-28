from pytest import raises, fixture
from tradeagent.backtest import get_data
from tradeagent.config import root
from pandas.io.sql import DatabaseError
from tradeagent.indicators import apply_dm, apply_tr, apply_atr

db_file = root / 'tests' / 'backtest' / 'test.sqlite3'


@fixture
def hist_data():

    hist = get_data(db_file, 'SELECT * FROM raw_candle;')
    return hist


def test_history_load(hist_data):

    assert hist_data.shape[0] == 100

    with raises(FileNotFoundError):
        hist2 = get_data('not a file', 'SELECT * FROM raw_candle;')

    with raises(DatabaseError):
        hist3 = get_data(db_file, 'Rumplestiltskin')
        hist3.retrieve_data()


def test_spread(hist_data):

    assert hist_data.spread.sum() == 2886


def test_dm_indicator(hist_data):

    apply_dm(hist_data)

    assert hist_data['bid +DM'].sum() == 535
    assert hist_data['bid -DM'].sum() == 591
    assert hist_data['ask +DM'].sum() == 439
    assert hist_data['ask -DM'].sum() == 617


def test_tr_indicator(hist_data):

    apply_tr(hist_data)

    assert hist_data['bid TR'].sum() == 1873


def test_atr_indicator(hist_data):

    apply_atr(hist_data, 14)

    hist_data.to_csv('atr_test.csv')

    assert hist_data['bid ATR'].sum() == 1757








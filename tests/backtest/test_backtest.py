from pytest import raises, fixture
from tradeagent.backtest import get_data
from tradeagent.config import root
from pandas.io.sql import DatabaseError
from tradeagent.indicators import apply_adx

db_file = root / 'tests' / 'backtest' / 'test.sqlite3'


@fixture
def hist_data():

    hist = get_data('EUR_USD', db_file, 'SELECT * FROM raw_candle;')
    return hist


def test_history_load(hist_data):

    assert hist_data.shape[0] == 100

    with raises(FileNotFoundError):
        hist2 = get_data('EUR_USD', 'not a file', 'SELECT * FROM raw_candle;')

    with raises(DatabaseError):
        hist3 = get_data('EUR_USD', db_file, 'Rumplestiltskin')
        hist3.retrieve_data()


def test_spread(hist_data):

    assert hist_data.spread.sum() == 2886


def test_adx_indicator(hist_data):

    apply_adx(hist_data)

    assert hist_data['bid TR'].sum() == 1873
    assert hist_data['bid TR Smooth'].sum() == 24780
    assert hist_data['bid ATR'].sum() == 1757
    assert hist_data['bid +DM'].sum() == 535
    assert hist_data['bid -DM'].sum() == 591
    assert hist_data['bid +DM Smooth'].sum() == 7051
    assert hist_data['bid -DM Smooth'].sum() == 7801
    assert hist_data['bid +DI'].sum() == 2446
    assert hist_data['bid -DI'].sum() == 2694
    assert hist_data['bid DX'].sum() == 1140
    assert hist_data['bid ADX'].sum() == 906

    #hist_data.iloc[0:500].to_csv('adx_test.csv')

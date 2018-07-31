from pytest import raises, fixture
from tradeagent.backtest import get_data
from tradeagent.config import root
from pandas.io.sql import DatabaseError
from tradeagent.indicators import apply_adx

csv_file = root / 'tests' / 'backtest' / 'raw_candle.csv'


@fixture
def hist_data():

    hist = get_data('EUR_USD', csv_file)
    return hist


def test_history_load(hist_data):

    assert hist_data.shape[0] == 100

    with raises(FileNotFoundError):
        hist2 = get_data('EUR_USD', 'not a file')


def test_spread(hist_data):

    assert hist_data.spread.sum() == 2884


def test_adx_indicator(hist_data):

    apply_adx(hist_data, 'bid')
    hist_data.to_csv('raw_candle_test.csv')

    assert 534 == hist_data['bid +DM'].sum()
    assert 588 == hist_data['bid -DM'].sum()
    assert 1886 == hist_data['bid TR'].sum()
    assert 1787 == hist_data['bid ATR'].sum()
    assert 24914 == hist_data['bid smooth TR'].sum()
    assert 7023 == hist_data['bid smooth +DM'].sum()
    assert 7743 == hist_data['bid smooth -DM'].sum()
    assert 2423 == hist_data['bid +DI'].sum()
    assert 2631 == hist_data['bid -DI'].sum()
    assert 1126 == hist_data['bid DX'].sum()
    assert 1103 == hist_data['bid ADX'].sum()

    #hist_data.iloc[0:500].to_csv('adx_test.csv')

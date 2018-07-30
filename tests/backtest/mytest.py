from tradeagent.backtest import get_data
from tradeagent.config import root
from tradeagent.indicators import apply_adx

db_file = root / 'tradeagent.sqlite3'
hist = get_data('EUR_USD', db_file, 'SELECT * FROM raw_candle;')
apply_adx(hist)



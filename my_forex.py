import json
import numpy as np
import pandas as pd
import bokeh.plotting as bpl
from bokeh.models import HoverTool, CrosshairTool
import v20

# TODO parameterize the Context values

def fetch_candle(instrument, **kwargs):
    api = v20.Context(
        'api-fxproactice.oanda.com',
        '443',
        token='38a9bcc1dc54e3e9cf7b50fb7a79c386-ae7eb6b66ed6c7e47b5cbb757e6bc9bc')

    response = api.instrument.candles(instrument, granularity='D', count=200)

    j = json.loads(response.raw_body)
    ohlc = pd.io.json.json_normalize(j['candles'])
    ohlc['date'] = pd.to_datetime(ohlc['time'])
    ohlc = ohlc[['date', 'mid.o', 'mid.h', 'mid.l', 'mid.c', 'volume']]
    ohlc.columns = ['date', 'o', 'h', 'l', 'c', 'volume']
    ohlc.o = pd.to_numeric(ohlc.o)
    ohlc.h = pd.to_numeric(ohlc.h)
    ohlc.l = pd.to_numeric(ohlc.l)
    ohlc.c = pd.to_numeric(ohlc.c)

    seqs = np.arange(ohlc.shape[0])
    ohlc["seq"] = pd.Series(seqs)

    ohlc['date'] = ohlc['date'].apply(lambda x: x.strftime('%m/%d'))
    ohlc['mid'] = ohlc.apply(lambda x: (x['o']+x['c'])/2, axis=1)
    ohlc['height'] = ohlc.apply(lambda x: abs(x['c']-x['o'] if x['c'] != x['o'] else 0.001), axis=1)
    return ohlc

fetch_candle('USD_CAD')

import json
import v20
import numpy as np
import pandas as pd
import bokeh.plotting as bpl
from bokeh.models import HoverTool, CrosshairTool
from math import pi

# TODO parameterize the Context values

def fetch_candle(instrument, **kwargs):
    api = v20.Context(
        'api-fxpractice.oanda.com',
        '443',
        token='38a9bcc1dc54e3e9cf7b50fb7a79c386-ae7eb6b66ed6c7e47b5cbb757e6bc9bc')

    response = api.instrument.candles(instrument, **kwargs)

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


def plot_candle(df):

    inc = df.c > df.o
    dec = df.o > df.c
    w = 0.3

    #use ColumnDataSource to pass in data for tooltips
    sourceInc = bpl.ColumnDataSource(bpl.ColumnDataSource.from_df(df.loc[inc]))
    sourceDec = bpl.ColumnDataSource(bpl.ColumnDataSource.from_df(df.loc[dec]))


    #the values for the tooltip come from ColumnDataSource
    hover = HoverTool(
        tooltips=[
            ("date", "@date"),
            ("open", "@o"),
            ("close", "@c")
        ]
    )

    TOOLS = [CrosshairTool(), hover, 'wheel_zoom', 'reset', 'xpan']
    p = bpl.figure(plot_width=900, plot_height=400, tools=TOOLS)
    p.xaxis.major_label_orientation = pi/4
    p.grid.grid_line_alpha=0.3

    #this is the up tail
    p.segment(df.seq[inc], df.h[inc], df.seq[inc], df.l[inc], color="red")
    #this is the bottom tail
    p.segment(df.seq[dec], df.h[dec], df.seq[dec], df.l[dec], color="green")
    #this is the candle body for the red dates
    p.rect(x='seq', y='mid', width=w, height='height', fill_color="red", line_color="red", source=sourceInc)
    #this is the candle body for the green dates
    p.rect(x='seq', y='mid', width=w, height='height', fill_color="green", line_color="green", source=sourceDec)

    return p

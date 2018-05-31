import json
import v20
import numpy as np
import pandas as pd
import bokeh.plotting as bpl
from bokeh.io import curdoc
from bokeh.themes import Theme
from bokeh.models import HoverTool, CrosshairTool, Span, Label, Arrow, NormalHead
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
    ohlc['mid'] = ohlc.apply(lambda x: (x['o'] + x['c']) / 2, axis=1)
    ohlc['height'] = ohlc.apply(lambda x: abs(x['c'] - x['o'] if x['c'] != x['o'] else 0.001), axis=1)
    return ohlc


def plot_candle(df, bid_ask):

    curdoc().theme = Theme(json={'attrs': {

        # apply defaults to Figure properties
        'Figure': {
            'background_fill_color': 'black',
            'border_fill_color': '#2F2F2F',
            'outline_line_color': '#444444',
        },

        # apply defaults to Axis properties
        'Axis': {
            'axis_line_color': '#1a1a1a',
            'axis_label_text_color': 'white',
            'major_label_text_color': 'white',
            'major_tick_line_color': 'white',
            'minor_tick_line_color': 'white',
        },

        'Grid': {
            'grid_line_dash': [6, 4],
            'grid_line_alpha': .3
        },

        'Title': {
            'text_color': 'white'
        }
    }})

    if bid_ask == 'ask':
        open = 'ask_open'
        high = 'ask_high'
        low = 'ask_low'
        close = 'ask_close'

    else:
        open = 'bid_open'
        high = 'bid_high'
        low = 'bid_low'
        close = 'bid_close'

    inc = df[close] > df[open]
    dec = df[open] > df[close]
    w = 0.70

    df['seq'] = np.arange(df.shape[0])
#    df['seq'] = df.index
    df['mid'] = df.apply(lambda x: (x[open] + x[close]) / 2, axis=1)
    df['height'] = df.apply(lambda x: abs(x[close] - x[open] if x[close] != x[open] else 0.00001), axis=1)


    # use ColumnDataSource to pass in data for tooltips
    source_inc = bpl.ColumnDataSource(bpl.ColumnDataSource.from_df(df.loc[inc]))
    source_dec = bpl.ColumnDataSource(bpl.ColumnDataSource.from_df(df.loc[dec]))

    # the values for the tooltip come from ColumnDataSource
    hover = HoverTool(
        tooltips=[
            ("date", "@date"),
            ("open", "@o"),
            ("close", "@c")
        ]
    )

    TOOLS = [CrosshairTool(), hover, 'wheel_zoom', 'reset', 'xpan']
    p = bpl.figure(plot_width=1200, plot_height=600, tools=TOOLS, x_range=(0,200))
    p.xaxis.major_label_orientation = pi / 4
    p.grid.grid_line_alpha = 0.3

    # this is the up tail
    p.segment(df.seq[inc], df[high][inc], df.seq[inc], df[low][inc], color="#909090")
    # this is the bottom tail
    p.segment(df.seq[dec], df[high][dec], df.seq[dec], df[low][dec], color="#909090")
    # this is the candle body for the red dates
    p.rect(x='seq', y='mid', width=w, height='height', fill_color="#b20000", line_color="#909090", source=source_inc)
    # this is the candle body for the green dates
    p.rect(x='seq', y='mid', width=w, height='height', fill_color="#00b200", line_color="#909090", source=source_dec)
    p.line(df.seq, df['bid_55_sma'])
    p.line(df.seq, df['bid_20_ema'], line_color='yellow')

    day_starts = df[(df.index.hour == 7) & (df.index.minute == 0)]['seq'].tolist()
    buy_signals = df[df['signal'] == 'BUY'][['seq', 'bid_20_ema']]
    sell_signals = df[df['signal'] == 'SELL'][['seq', 'bid_20_ema']]

    for seq in day_starts:

        month = df[df['seq'] == seq].index.month.data[0]
        day = df[df['seq'] == seq].index.day.data[0]

        month_day = str(month) + '/' + str(day)

        day_start = Span(location=seq,
                         dimension='height', line_color='orange',
                         line_dash='dashed', line_width=2)

        day_label = Label(x=seq + 1, y=540, y_units='screen', text=month_day,
                          border_line_color='black', background_fill_color='black', text_color='orange')

        p.add_layout(day_start)
        p.add_layout(day_label)

    for index, row in buy_signals.iterrows():

        start = row['bid_20_ema'] - .0010
        end = row['bid_20_ema'] - .0003
        seq = row['seq']

        arrow = Arrow(end=NormalHead(fill_color="green", size=15),
                      x_start=seq, y_start=start, x_end=seq, y_end=end)

#        signal_label = Label(x=seq, y=540, y_units='screen', text='BUY',
#                             border_line_color='black', background_fill_color='black', text_color='green')

        p.add_layout(arrow)

    for index, row in sell_signals.iterrows():

        start = row['bid_20_ema'] + .0010
        end = row['bid_20_ema'] + .0003
        seq = row['seq']

        arrow = Arrow(end=NormalHead(fill_color="red", size=15),
                      x_start=seq, y_start=start, x_end=seq, y_end=end)

#        signal_label = Label(x=seq, y=540, y_units='screen', text='BUY',
#                             border_line_color='black', background_fill_color='black', text_color='green')

        p.add_layout(arrow)


    #day_df = df[df.index.hour == 7]

    # for index, row in day_df.iterrows():
    #
    #     day_start = Span(location=index,
    #                      dimension='height', line_color='green',
    #                      line_dash='dashed', line_width=3)
    #     p.add_layout(day_start)

    return p

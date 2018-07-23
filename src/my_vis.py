def plot_candle(df, bid_ask):

    # TODO 1. Document Code
    # TODO 2. Put Theme in a file
    # TODO 3. Parameterize the indicators (i.e. ema)
    # TODO 4. General cleanup

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
        op = 'ask_open'
        hi = 'ask_high'
        lo = 'ask_low'
        cl = 'ask_close'

    else:
        op = 'bid_open'
        hi = 'bid_high'
        lo = 'bid_low'
        cl = 'bid_close'

    inc = df[cl] > df[op]
    dec = df[op] > df[cl]
    w = 0.70

    df['seq'] = np.arange(df.shape[0])
    df['mid'] = df.apply(lambda x: (x[op] + x[cl]) / 2, axis=1)
    df['height'] = df.apply(lambda x: abs(x[cl] - x[op] if x[cl] != x[op] else 0.00001), axis=1)


    # use ColumnDataSource to pass in data for tooltips
    # source_inc = bpl.ColumnDataSource(bpl.ColumnDataSource.from_df(df.loc[inc]))
    # source_dec = bpl.ColumnDataSource(bpl.ColumnDataSource.from_df(df.loc[dec]))

    # # the values for the tooltip come from ColumnDataSource
    # hover = HoverTool(
    #     tooltips=[
    #         ("date", "@date"),
    #         ("open", "@o"),
    #         ("close", "@c")
    #     ]
    # )

    # TOOLS = [CrosshairTool(), hover, 'wheel_zoom', 'reset', 'xpan']
    TOOLS = [CrosshairTool(), 'wheel_zoom', 'reset', 'xpan']
    p = bpl.figure(plot_width=1200, plot_height=600, tools=TOOLS, x_range=(0,200))
    p.xaxis.major_label_orientation = pi / 4
    p.grid.grid_line_alpha = 0.3

    # this is the up tail
    p.segment(df.seq[inc], df[hi][inc], df.seq[inc], df[lo][inc], color="#909090")
    # this is the bottom tail
    p.segment(df.seq[dec], df[hi][dec], df.seq[dec], df[lo][dec], color="#909090")

    # red candles
    p.rect(x=df['seq'][dec], y=df['mid'][dec], width=w, height=df['height'][dec],
           fill_color="#b20000", line_color="#909090")

    # green candles
    p.rect(x=df['seq'][inc], y=df['mid'][inc], width=w, height=df['height'][inc],
           fill_color="#00b200", line_color="#909090")

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

        arrow = Arrow(end=VeeHead(fill_color="green", size=15),
                      x_start=seq, y_start=start, x_end=seq, y_end=end)

        p.add_layout(arrow)

    for index, row in sell_signals.iterrows():

        start = row['bid_20_ema'] + .0010
        end = row['bid_20_ema'] + .0003
        seq = row['seq']

        arrow = Arrow(end=VeeHead(fill_color="red", size=15),
                      x_start=seq, y_start=start, x_end=seq, y_end=end)

        p.add_layout(arrow)

    return p

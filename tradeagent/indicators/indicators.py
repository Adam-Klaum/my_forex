from numpy import subtract, roll, ma, maximum, array, mean, repeat, NaN


def roll_column(column, df, return_orig=True):

    column = df[column].values
    column_prev = roll(column, 1)
    column_prev[0] = 0
    column[0] = 0

    if return_orig:
        return column, column_prev
    else:
        return column_prev


def apply_dm(df):
    """Applies Directional Movement indicators (+DM, -DM) to each row of the Data Frame"""

    bid_high, bid_high_prev = roll_column('bid_high', df)
    bid_low, bid_low_prev = roll_column('bid_low', df)
    ask_high, ask_high_prev = roll_column('ask_high', df)
    ask_low, ask_low_prev = roll_column('ask_low', df)

    bid_high_diff = subtract(bid_high, bid_high_prev).clip(0)
    bid_low_diff = subtract(bid_low, bid_low_prev).clip(0)
    ask_high_diff = subtract(ask_low, ask_low_prev).clip(0)
    ask_low_diff = subtract(ask_high, ask_high_prev).clip(0)

    bid_high_diff = ma.array(bid_high_diff, mask=bid_high_diff > bid_low_diff)
    bid_low_diff = ma.array(bid_low_diff, mask=bid_low_diff > bid_high_diff)
    ask_high_diff = ma.array(ask_high_diff, mask=ask_high_diff > ask_low_diff)
    ask_low_diff = ma.array(ask_low_diff, mask=ask_low_diff > ask_high_diff)

    df['bid +DM'] = bid_high_diff.filled(bid_high_diff)
    df['bid -DM'] = bid_low_diff.filled(bid_low_diff)
    df['ask +DM'] = ask_high_diff.filled(ask_high_diff)
    df['ask -DM'] = ask_low_diff.filled(ask_low_diff)


def apply_tr(df):

    bid_prev_close = roll_column('bid_close', df, False)
    bid_current_high = df.bid_high.values
    bid_current_high[0] = 0
    bid_current_low = df.bid_low.values
    bid_current_low[0] = 0

    ask_prev_close = roll_column('ask_close', df, False)
    ask_current_high = df.ask_high.values
    ask_current_high[0] = 0
    ask_current_low = df.ask_low.values
    ask_current_low[0] = 0

    df['bid TR'] = maximum(maximum(subtract(bid_current_high, bid_current_low),
                                   abs(subtract(bid_current_high, bid_prev_close))),
                           abs(subtract(bid_current_low, bid_prev_close)))

    df['ask TR'] = maximum(maximum(subtract(ask_current_high, ask_current_low),
                                   abs(subtract(ask_current_high, ask_prev_close))),
                           abs(subtract(ask_current_low, ask_prev_close)))


def apply_atr(df, period):

    apply_tr(df)

    bid_tr = df['bid TR'].values
    bid_atr = repeat(0, bid_tr.shape[0])

    ask_tr = df['ask TR'].values
    ask_atr = repeat(0, ask_tr.shape[0])

    bid_atr[period - 1] = round(mean(bid_tr[0:period]))
    ask_atr[period - 1] = round(mean(ask_tr[0:period]))

    for i in range(period, bid_tr.shape[0]):
        bid_atr[i] = round((bid_atr[i - 1] * (period - 1) + bid_tr[i]) / 14)
        ask_atr[i] = round((ask_atr[i - 1] * (period - 1) + ask_tr[i]) / 14)

    df['bid ATR'] = bid_atr
    df['ask ATR'] = ask_atr





import numpy as np
np.seterr(divide='ignore', invalid='ignore')


def roll_column(column, df, return_orig=True):

    column = df[column].values
    column_prev = np.roll(column, 1)
    column_prev[0] = 0
    column[0] = 0

    if return_orig:
        return column, column_prev
    else:
        return column_prev


def apply_adx(df, period=14):

    # Apply Directional Movement indicators (+DM, -DM) to each row of the Data Frame"""

    df_length = df.shape[0]

    bid_high, bid_high_prev = roll_column('bid_high', df)
    bid_low, bid_low_prev = roll_column('bid_low', df)
    ask_high, ask_high_prev = roll_column('ask_high', df)
    ask_low, ask_low_prev = roll_column('ask_low', df)

    bid_high_diff = np.subtract(bid_high, bid_high_prev).clip(0)
    bid_low_diff = np.subtract(bid_low, bid_low_prev).clip(0)
    ask_high_diff = np.subtract(ask_low, ask_low_prev).clip(0)
    ask_low_diff = np.subtract(ask_high, ask_high_prev).clip(0)

    bid_high_diff = np.ma.array(bid_high_diff, mask=bid_high_diff > bid_low_diff)
    bid_low_diff = np.ma.array(bid_low_diff, mask=bid_low_diff > bid_high_diff)
    ask_high_diff = np.ma.array(ask_high_diff, mask=ask_high_diff > ask_low_diff)
    ask_low_diff = np.ma.array(ask_low_diff, mask=ask_low_diff > ask_high_diff)

    bid_plus_dm = bid_high_diff.filled(bid_high_diff)
    bid_minus_dm = bid_low_diff.filled(bid_low_diff)
    ask_plus_dm = ask_high_diff.filled(ask_high_diff)
    ask_minus_dm = ask_low_diff.filled(ask_low_diff)

    # Apply True Range (TR) to each row of the Data Frame

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

    bid_tr = np.maximum(np.maximum(np.subtract(bid_current_high, bid_current_low),
                                   np.abs(np.subtract(bid_current_high, bid_prev_close))),
                        np.abs(np.subtract(bid_current_low, bid_prev_close)))

    ask_tr = np.maximum(np.maximum(np.subtract(ask_current_high, ask_current_low),
                                   np.abs(np.subtract(ask_current_high, ask_prev_close))),
                        np.abs(np.subtract(ask_current_low, ask_prev_close)))

    # Apply Average True Range (ATR) to each row of the Data Frame
    # Apply smoothing to the True Range (TR) indicator

    bid_smooth_tr = np.zeros(df_length)
    bid_atr = np.zeros(df_length)
    bid_smooth_plus_dm = np.zeros(df_length)
    bid_smooth_minus_dm = np.zeros(df_length)
    bid_adx = np.zeros(df_length)

    ask_smooth_tr = np.zeros(df_length)
    ask_atr = np.zeros(df_length)
    ask_smooth_plus_dm = np.zeros(df_length)
    ask_smooth_minus_dm = np.zeros(df_length)
    ask_adx = np.zeros(df_length)

    bid_atr[period - 1] = round(np.mean(bid_tr[0:period]))
    bid_smooth_tr[period - 1] = sum(bid_tr[0:period])
    bid_smooth_plus_dm[period - 1] = sum(bid_plus_dm[0:period])
    bid_smooth_minus_dm[period - 1] = sum(bid_minus_dm[0:period])

    ask_atr[period - 1] = round(np.mean(ask_tr[0:period]))
    ask_smooth_tr[period - 1] = sum(ask_tr[0:period])
    ask_smooth_plus_dm[period - 1] = sum(ask_plus_dm[0:period])
    ask_smooth_minus_dm[period - 1] = sum(ask_minus_dm[0:period])

    for i in range(period, df_length):
        bid_atr[i] = round((bid_atr[i - 1] * (period - 1) + bid_tr[i]) / period)
        bid_smooth_tr[i] = round(bid_smooth_tr[i - 1] - bid_smooth_tr[i - 1]/period + bid_tr[i])
        bid_smooth_plus_dm[i] = round(bid_smooth_plus_dm[i - 1] - bid_smooth_plus_dm[i - 1]/period + bid_plus_dm[i])
        bid_smooth_minus_dm[i] = round(bid_smooth_minus_dm[i - 1] - bid_smooth_minus_dm[i - 1]/period + bid_minus_dm[i])

        ask_atr[i] = round((ask_atr[i - 1] * (period - 1) + ask_tr[i]) / period)
        ask_smooth_tr[i] = round(ask_smooth_tr[i - 1] - ask_smooth_tr[i - 1]/period + ask_tr[i])
        ask_smooth_plus_dm[i] = round(ask_smooth_plus_dm[i - 1] - ask_smooth_plus_dm[i - 1]/period + ask_plus_dm[i])
        ask_smooth_minus_dm[i] = round(ask_smooth_minus_dm[i - 1] - ask_smooth_minus_dm[i - 1]/period + ask_minus_dm[i])

    bid_plus_di = np.round(np.divide(bid_smooth_plus_dm, bid_smooth_tr) * 100)
    bid_minus_di = np.round(np.divide(bid_smooth_minus_dm, bid_smooth_tr) * 100)
    ask_plus_di = np.round(np.divide(ask_smooth_plus_dm, ask_smooth_tr) * 100)
    ask_minus_di = np.round(np.divide(ask_smooth_minus_dm, ask_smooth_tr) * 100)

    bid_dx_a = np.subtract(bid_plus_di, bid_minus_di)
    bid_dx_b = np.add(bid_plus_di, bid_minus_di)
    bid_dx = np.round(np.abs(np.divide(bid_dx_a, bid_dx_b, out=np.zeros_like(bid_dx_a), where=bid_dx_b!=0)) * 100)

    ask_dx_a = np.subtract(ask_plus_di, ask_minus_di)
    ask_dx_b = np.add(ask_plus_di, ask_minus_di)
    ask_dx = np.round(np.abs(np.divide(ask_dx_a, ask_dx_b, out=np.zeros_like(ask_dx_a), where=ask_dx_b!=0)) * 100)

    bid_adx[(period - 1) * 2] = round(np.mean(bid_dx[period - 1:period * 2]))
    ask_adx[(period - 1) * 2] = round(np.mean(ask_dx[period - 1:period * 2]))

    for i in range(period * 2, df_length):

        bid_adx[i] = round((bid_adx[i - 1] * (period - 1) + bid_dx[i])/period)
        ask_adx[i] = round((ask_adx[i - 1] * (period - 1) + ask_dx[i])/period)

    df['bid TR'] = bid_tr
    df['bid TR Smooth'] = bid_smooth_tr
    df['bid ATR'] = bid_atr
    df['bid +DM'] = bid_plus_dm
    df['bid -DM'] = bid_minus_dm
    df['bid +DM Smooth'] = bid_smooth_plus_dm
    df['bid -DM Smooth'] = bid_smooth_minus_dm
    df['bid +DI'] = bid_plus_di
    df['bid -DI'] = bid_minus_di
    df['bid DX'] = bid_dx
    df['bid ADX'] = bid_adx

    df['ask TR'] = ask_tr
    df['ask TR Smooth'] = ask_smooth_tr
    df['ask ATR'] = ask_atr
    df['ask +DM'] = ask_plus_dm
    df['ask -DM'] = ask_minus_dm
    df['ask +DM Smooth'] = ask_smooth_plus_dm
    df['ask -DM Smooth'] = ask_smooth_minus_dm
    df['ask +DI'] = ask_plus_di
    df['ask -DI'] = ask_minus_di
    df['ask DX'] = ask_dx
    df['ask ADX'] = ask_adx

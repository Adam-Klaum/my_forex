from numpy import subtract, roll, ma


def roll_column(column, df):

    column = df[column].values
    column_prev = roll(column, 1)
    column_prev[0] = 0
    column[0] = 0
    return column, column_prev


def apply_dm(df):
    """Applies the indicator to each row of the Data Frame"""

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

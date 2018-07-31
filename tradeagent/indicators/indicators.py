import numpy as np
import numba


def apply_sma(df, name, period):
    """ Calculate Simple Moving Average (SMA) against input data

    Take a pandas dataframe as input and add a new column for SMA
    based on the specified input column

    Note: The function operates directly on the passed in dataframe, not a copy of it.

    :param df: Dataframe upon which to operate
    :param name: Column to use for SMA calculation
    :param period: Period to use for calculating SMA
    :return:
    """

    new_col = '{} {} SMA'.format(name, period)

    df[new_col] = df[name].rolling(window=period).mean()
    df[new_col].fillna(0, inplace=True)
    df[new_col] = df[new_col].round()
    df[new_col] = df[new_col].astype('uint32')


def apply_ema(df, name, period):
    """ Calculate Exponential Moving Average (EMA) against input data

    Take a pandas dataframe as input and add a new column for EMA
    based on the specified input column

    Note: The function operates directly on the passed in dataframe, not a copy of it.

    :param df: Dataframe upon which to operate
    :param name: Column to use for SMA calculation
    :param period: Period to use for calculating SMA
    :return:
    """

    new_col = '{} {} EMA'.format(name, period)

    df[new_col] = df[name].ewm(com=period).mean()
    df[new_col].fillna(0, inplace=True)
    df[new_col] = df[new_col].round()
    df[new_col] = df[new_col].astype('uint32')


@numba.jit(nopython=True)
def avg_tr_vec(atr, tr, period=14):
    """Vectorized function to calculate Average True Range (ATR)

    :param atr: Pre-created numpy array to hold ATR values
    :param tr:  Numpy array containing True Range (TR) values
    :param period: Time period used for averaging, 14 recommended
    :return: None
    """

    for i in range(period, len(atr)):
        atr[i] = round((atr[i - 1] * (period - 1) + tr[i]) / period)

    return atr


@numba.jit(nopython=True)
def avg_dx_vec(adx, dx, period=14):
    """ Vectorized function to calculate Average Directional Index (ADX)

    :param adx: Pre-created numpy array to hold ADX values
    :param dx:  Numpy array containing Directional Movement Index (DX) values
    :param period: Time period used for averaging, 14 recommended
    :return: None
    """

    for i in range(period * 2, len(adx)):
        adx[i] = round((adx[i - 1] * (period - 1) + dx[i]) / period)

    return adx


@numba.jit(nopython=True)
def smooth_vec(smooth, input_data, period=14):
    """ Vectorized function to smooth time series data

    Technique specifically for smoothing True Range (TR) and Directional Movement (-DM, +DM)
    data for Wilders ADX indicator

    :param smooth: Pre-created numpy array to hold target values
    :param input_data: Pre-created numpy array containing the values to smooth
    :param period: Time period used for smoothing, 14 recommended
    :return:
    """

    for i in range(period, len(smooth)):
        smooth[i] = round(smooth[i - 1] - smooth[i - 1]/period + input_data[i])

    return smooth


def apply_adx(df, name, period=14):

    """Apply Wilder's ADX indicator to a dataframe of OHLC prices

    Do all of the required intermediate calculations to calculate Wilder's ADX/DMI
    indicator given bid and ask ohlc data

    This function expects the following columns to be present in the passed-in dataframe
    where <name> is a prefix like bid, ask, etc.

    | <name>_open    uint32
    | <name>_high    uint32
    | <name>_low     uint32
    | <name>_close   uint32

    :param df: Dataframe containing price information
    :param name: Prefix of the ohlc column names (i.e. bid, ask etc.)
    :param period: Period used for all ADX calculations.  Wilder recommends 14
    :return: None
    """

    # Apply Directional Movement indicators (+DM, -DM) to each row of the Data Frame

    new_cols = ['+DM', '-DM', 'TR', 'ATR', 'smooth TR', 'smooth +DM', 'smooth -DM', '+DI', '-DI', 'DX', 'ADX']

    df_length = df.shape[0]

    high_diff = np.subtract(df[name + '_high'].values, df[name + '_high'].shift(1).values).clip(0)
    high_diff[0] = 0

    low_diff = np.subtract(df[name + '_low'].values, df[name + '_low'].shift(1).values).clip(0)
    low_diff[0] = 0

    high_diff = np.ma.array(high_diff, mask=high_diff > low_diff)
    low_diff = np.ma.array(low_diff, mask=low_diff > high_diff)

    df[name + ' +DM'] = high_diff.filled(high_diff)
    df[name + ' -DM'] = low_diff.filled(low_diff)

    # Adding True Range (TR) indicator

    df[name + ' TR'] = np.maximum(np.maximum(
        np.subtract(df[name + '_high'].values, df[name + '_low'].values),
        np.abs(np.subtract(df[name + '_high'].values, df[name + '_close'].shift(1).values))),
        np.abs(np.subtract(df[name + '_low'].values, df[name + '_close'].shift(1).values)))

    # Adding Average True Range (ATR) and smoothed True Range (TR) indicators

    smooth_tr = np.zeros(df_length)
    atr = np.zeros(df_length)
    smooth_plus_dm = np.zeros(df_length)
    smooth_minus_dm = np.zeros(df_length)
    adx = np.zeros(df_length)

    atr[period - 1] = round(df[name + ' TR'][0:period].mean())
    smooth_tr[period - 1] = df[name + ' TR'][0:period].sum()
    smooth_plus_dm[period - 1] = df[name + ' +DM'][0:period].sum()
    smooth_minus_dm[period - 1] = df[name + ' -DM'][0:period].sum()

    df[name + ' ATR'] = avg_tr_vec(atr, df[name + ' TR'].values, period)

    df[name + ' smooth TR'] = smooth_vec(smooth_tr, df[name + ' TR'].values, period)
    df[name + ' smooth +DM'] = smooth_vec(smooth_plus_dm, df[name + ' +DM'].values, period)
    df[name + ' smooth -DM'] = smooth_vec(smooth_minus_dm, df[name + ' -DM'].values, period)

    # Adding +DI and -DI indicators

    df[name + ' +DI'] = np.round(np.divide(smooth_plus_dm,
                                           smooth_tr,
                                           out=np.zeros_like(smooth_tr),
                                           where=smooth_tr!=0) * 100)

    df[name + ' -DI'] = np.round(np.divide(smooth_minus_dm,
                                           smooth_tr,
                                           out=np.zeros_like(smooth_tr),
                                           where=smooth_tr!=0) * 100)

    # Adding DX indicator

    dx_a = np.subtract(df[name + ' +DI'], df[name + ' -DI'])
    dx_b = np.add(df[name + ' +DI'], df[name + ' -DI'])
    df[name + ' DX'] = np.round(np.abs(np.divide(dx_a,
                                                 dx_b,
                                                 out=np.zeros_like(dx_a),
                                                 where=dx_b!=0)) * 100)

    adx[period * 2 - 1] = round(df[name + ' DX'][period - 1:period * 2].mean())

    df[name + ' ADX'] = avg_dx_vec(adx, df[name + ' DX'].values, period)

    # Saving memory with more appropriate data types

    for col in new_cols:
        df[name + ' ' + col].fillna(0, inplace=True)
        df[name + ' ' + col] = df[name + ' ' + col].astype('uint16')





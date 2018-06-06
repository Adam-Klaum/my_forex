import v20
import pandas as pd
from datetime import datetime, timedelta


class Account:

    def __init__(self, balance):
        self.balance = balance


class Candle:

    def __init__(self, dt, bid_open, bid_high, bid_low, bid_close,
                 ask_open, ask_high, ask_low, ask_close, spread):

        self.dt = dt
        self.bid_open = bid_open
        self.bid_high = bid_high
        self.bid_low = bid_low
        self.bid_close = bid_close
        self.ask_open = ask_open
        self.ask_high = ask_high
        self.ask_low = ask_low
        self.ask_close = ask_close
        self.spread = spread


class Trade:

    # TODO add per pip price calculation, for now assuming all lots are 10000

    def __init__(self, trade_type, lot_size, stop_type, profit_type, candle,
                 risk_pips, reward_pips):

        self.start_date = candle.dt
        self.end_date = candle.dt
        self.trade_type = trade_type
        self.lot_size = lot_size
        self.spread = candle.spread
        self.stop_type = stop_type
        self.profit_type = profit_type

        if trade_type == 'BUY':
            self.order_price = candle.ask_close
        if trade_type == 'SELL':
            self.order_price = candle.bid_close

        self.close_price = 0

        self.risk_pips = risk_pips
        self.reward_pips = reward_pips

        if trade_type == 'BUY':
            self.risk_price = candle.ask_close - self.risk_pips / 10000
            self.reward_price = candle.ask_close + self.reward_pips / 10000

        if trade_type == 'SELL':
            self.risk_price = candle.bid_close + self.risk_pips / 10000
            self.reward_price = candle.bid_close - self.reward_pips / 10000

        self.status = 'open'
        self.profit_loss = 0
        self.commission = candle.spread
        self.close_type = ''
        self.prev_risk_price = 0

    def update(self, candle):

        # TODO add in functionality for trailing stop
        # Every time the price moves in a profitable direction adjust the trailing stop
        # Do this by adjusting the risk_price variable
        # Is the change from the last candle in our favor?  Then update the stop

        if self.stop_type == 'trailing':

            if self.trade_type == 'BUY':
                self.risk_price = candle.ask_high - self.risk_pips / 10000
                if self.prev_risk_price and self.risk_price < self.prev_risk_price:
                    self.risk_price = self.prev_risk_price

            if self.trade_type == 'SELL':
                self.risk_price = candle.bid_low + self.risk_pips / 10000
                if self.prev_risk_price and self.risk_price > self.prev_risk_price:
                    self.risk_price = self.prev_risk_price

        if self.trade_type == 'BUY':

            # taking profit
            if candle.ask_low <= self.reward_price <= candle.ask_high:

                if self.profit_type == 'flat':
                    self.close(candle, 'PROFIT')
                    return

                if self.profit_type == 'trailing':
                    self.risk_price = candle.ask_close
                    self.prev_risk_price = self.risk_price
                    return

            # getting stopped out
            if candle.ask_low <= self.risk_price <= candle.ask_high:
                self.close(candle, 'STOP')
                return

        if self.trade_type == 'SELL':

            # taking profit
            if candle.bid_low <= self.reward_price <= candle.bid_high:

                if self.profit_type == 'flat':
                    self.close(candle, 'PROFIT')
                    return

                if self.profit_type == 'trailing':
                    self.risk_price = candle.bid_close
                    self.prev_risk_price = self.risk_price
                    return

            # getting stopped out
            if candle.bid_low <= self.risk_price <= candle.bid_high:
                self.close(candle, 'STOP')
                return

        self.prev_risk_price = self.risk_price

    def close(self, candle, close_type):

        self.commission += candle.spread
        self.status = 'closed'
        self.end_date = candle.dt
        self.close_type = close_type

        if self.trade_type == 'BUY':

            self.close_price = candle.ask_close
            self.profit_loss = 10000 * (self.close_price - self.order_price) - self.commission

        if self.trade_type == 'SELL':

            self.close_price = candle.bid_close
            self.profit_loss = 10000 * (self.order_price - self.close_price) - self.commission
        return


class Strategy:

    def __init__(self, name, data_file, indicators, risk_pct, account, signal_func,
                 start_date, end_date, trade_start_hour, trade_end_hour,
                 start_buffer, end_buffer, lot_size, risk_pips, reward_pips):

        # TODO risk_pips and reward pips should be a function of account size and risk_pct

        self.name = name
        self.data_file = data_file
        self.indicators = indicators
        self.risk_pct = risk_pct
        self.account = account
        self.signal_func = signal_func
        self.start_date = start_date
        self.end_date = end_date
        self.trade_start_hour = trade_start_hour
        self.trade_end_hour = trade_end_hour
        self.start_buffer = start_buffer
        self.end_buffer = end_buffer
        self.lot_size = lot_size
        self.risk_pips = risk_pips
        self.reward_pips = reward_pips
        self.df = pd.DataFrame()

    def load_history(self):

        self.df = pd.read_csv(self.data_file, index_col=0)
        self.df['signal'] = 'HOLD'
        self.df['datetime'] = pd.to_datetime(self.df['datetime'])
        self.df.set_index('datetime', drop=True, inplace=True)
        self.df = self.df.between_time(self.trade_start_hour, self.trade_end_hour)

    def add_indicators(self):

        for key, value in self.indicators.items():

            self.df = key(self.df, **value)

        # TODO for testing only, remove later
        self.df = self.df.iloc[-4000:]

    def run_strategy(self):

        # TODO add functionality to close all trades if trading window is over

        prev_state = -1

        active_trades = []
        closed_trades = []

        # main loop through all candles
        for index, row in self.df.iterrows():

            candle = Candle(index,
                            row['bid_open'],
                            row['bid_high'],
                            row['bid_low'],
                            row['bid_close'],
                            row['ask_open'],
                            row['ask_high'],
                            row['ask_low'],
                            row['ask_close'],
                            row['spread'])

            for trade_idx, trade in enumerate(active_trades):

                if trade.status == 'closed':

                    closed_trades.append(trade)
                    # TODO is this actually working?
                    del(active_trades[trade_idx])

            signal_data = {'20_ema': row['bid_20_ema'], '55_sma': row['bid_55_sma']}
            signal, prev_state = self.signal_func(signal_data, prev_state)
            self.df.at[index, 'signal'] = signal

            # TODO parameterize stop type

            if signal != 'HOLD':

                if len(active_trades) < 4:

                    trade = Trade(signal,
                                  10000,
                                  'trailing',
                                  'trailing',
                                  candle,
                                  self.risk_pips,
                                  self.reward_pips)

                    active_trades.append(trade)

            for trade in active_trades:

                # TODO parameterize the EOD

                if index.hour == 19 and index.minute == 59:

                    trade.close(candle, 'EOD')

                trade.update(candle)

        # active_df = pd.DataFrame(columns=closed_trades[0].__dict__.keys())
        #
        # for trade_idx, trade in enumerate(active_trades):
        #     active_df.loc[trade_idx] = list(trade.__dict__.values())
        #
        # print(active_df)
        #
        trade_df = pd.DataFrame(columns=closed_trades[0].__dict__.keys())

        for trade_idx, trade in enumerate(closed_trades):

            trade_df.loc[trade_idx] = list(trade.__dict__.values())

        return trade_df


def add_sma(df, period):

    df['bid_' + str(period) + '_sma'] = df['bid_close'].rolling(window=period).mean()
    df['ask_' + str(period) + '_sma'] = df['ask_close'].rolling(window=period).mean()

    return df


def add_ema(df, period):

    df['bid_' + str(period) + '_ema'] = df['bid_close'].ewm(span=period, adjust=False).mean()
    df['ask_' + str(period) + '_ema'] = df['ask_close'].ewm(span=period, adjust=False).mean()

    return df


def add_atr(df, period):


    # Method 1: Current High less the current Low
    # Method 2: Current High less the previous Close (absolute value)
    # Method 3: Current Low less the previous Close (absolute value)
    #

    def calc_bid_tr(row):

        bid_tr = max([row['bid_high'] - row['bid_low'],
                      abs(row['bid_high'] - row['bid_prev_close']),
                      abs(row['bid_low'] - row['bid_prev_close'])])

        return bid_tr

    def calc_ask_tr(row):

        ask_tr = max([row['ask_high'] - row['ask_low'],
                      abs(row['ask_high'] - row['ask_prev_close']),
                      abs(row['ask_low'] - row['ask_prev_close'])])

        return ask_tr

    df['bid_prev_close'] = df['bid_close'].shift(1)
    df['ask_prev_close'] = df['ask_close'].shift(1)

    df['bid_tr'] = df.apply(calc_bid_tr, axis=1)
    df['ask_tr'] = df.apply(calc_ask_tr, axis=1)

    df['bid_atr'] = df['bid_tr'].rolling(window=period).mean()
    df['ask_atr'] = df['ask_tr'].rolling(window=period).mean()

    return df


def calc_spread(instrument, spread):

    instruments = {'EUR_USD': {'dec_digits': 5},
                   'USD_CAD': {'dec_digits': 5}}

    spread = spread * (10 ** (instruments[instrument]['dec_digits'] - 1))

    return round(spread, 1)


def sub_minute(minute):
    '''
    A simple function to properly calculate the previous minute
    :param minute:
    :return previous minute:
    '''
    if minute == 0:
        return 59
    else:
        return minute - 1


def oa_date_cv(oa_dt):

    date_part, tick_minute, _ = oa_dt.split(':')
    tick_date, tick_hour = date_part.split('T')
    return datetime.strptime(tick_date + ' ' + tick_hour + ':' + tick_minute, '%Y-%m-%d %H:%M')


def live_feed(instrument):
    oa_cf = oa_config.OAConf()

    oa_api = v20.Context(oa_cf.streaming_hostname,
                         oa_cf.port,
                         token=oa_cf.token)

    response = oa_api.pricing.stream(oa_cf.active_account,
                                     snapshot=False,
                                     instruments=instrument)

    if response.status != 200:
        print(response)
        print(response.body)
        return

    candle_df = pd.DataFrame(columns=['instrument',
                                      'datetime',
                                      'bid_open',
                                      'bid_high',
                                      'bid_low',
                                      'bid_close',
                                      'ask_open',
                                      'ask_high',
                                      'ask_low',
                                      'ask_close',
                                      'spread'
                                      ])

    df_index = 0
    minutes = 5
    minute_count = 0
    min_bid_list = []
    min_ask_list = []
    last_minute = -1

    for msg_type, msg in response.parts():

        if msg_type == "pricing.Price":

            date_part, minute, _ = msg.time.split(':')

            minute = int(minute)
            if last_minute == -1:
                last_minute = minute

            tick_date, tick_hour = date_part.split('T')
            tick_date = datetime.strptime(tick_date + ' ' + tick_hour + ':' + str(minute), '%Y-%m-%d %H:%M')

            bid = float(msg.bids[0].price)
            ask = float(msg.asks[0].price)

            spread = ask - bid
            spread = calc_spread(msg.instrument, spread)

            # If the minute rolled over, write a record to the DataFrame
            if minute != last_minute:

                bid_open = min_bid_list[0]
                bid_high = max(min_bid_list)
                bid_low = min(min_bid_list)
                bid_close = min_bid_list[-1]

                ask_open = min_ask_list[0]
                ask_high = max(min_ask_list)
                ask_low = max(min_ask_list)
                ask_close = min_ask_list[-1]

                candle_df.loc[df_index] = [msg.instrument,
                                           tick_date,
                                           bid_open,
                                           bid_high,
                                           bid_low,
                                           bid_close,
                                           ask_open,
                                           ask_high,
                                           ask_low,
                                           ask_close,
                                           spread]

                df_index += 1

                min_bid_list = [bid_close, bid]
                min_ask_list = [ask_close, ask]

                if minute_count == minutes:
                    break
                else:
                    minute_count += 1

            else:

                min_bid_list.append(float(bid))
                min_ask_list.append(float(ask))

            last_minute = minute
            print(msg.instrument + ' ' +
                  str(msg.time) + ' ' +
                  str(minute) + ' ' +
                  str(msg.bids[0].price) + ' ' +
                  str(spread))

    print(candle_df)


def get_history(inst, **kwargs):

    oa_cf = oa_config.OAConf()

    oa_api = v20.Context(oa_cf.hostname,
                         oa_cf.port,
                         token=oa_cf.token)

    candle_df = pd.DataFrame(columns=['instrument',
                                      'datetime',
                                      'bid_open',
                                      'bid_high',
                                      'bid_low',
                                      'bid_close',
                                      'ask_open',
                                      'ask_high',
                                      'ask_low',
                                      'ask_close',
                                      'spread'
                                      ])

    df_index = 0

    response = oa_api.instrument.candles(instrument=inst, **kwargs)

    if response.status != 200:
        print(response)
        print(response.body)
        return

    instrument = response.get("instrument", 200)

    for candle in response.get("candles", 200):

        candle_df.loc[df_index] = [instrument,
                                   oa_date_cv(candle.time),
                                   float(candle.bid.o),
                                   float(candle.bid.h),
                                   float(candle.bid.l),
                                   float(candle.bid.c),
                                   float(candle.ask.o),
                                   float(candle.ask.h),
                                   float(candle.ask.l),
                                   float(candle.ask.c),
                                   calc_spread(instrument, float(candle.ask.c) - float(candle.bid.c))
                                   ]

        df_index += 1

    return candle_df


def save_history_range(inst, output, start_date, end_date, **kwargs):

    date_range = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

    temp_frames = []

    for day in date_range:

        from_time = str(day) + 'T00:00:00Z'
        to_time = str(day) + 'T23:59:59Z'

        temp_frames.append(get_history(inst,
                                       fromTime=from_time,
                                       toTime=to_time,
                                       **kwargs))

        print(day)

    candle_df = pd.concat(temp_frames, ignore_index=True)

    candle_df.to_csv(output)


def ma_55_20_cross(ind, prev_state):
    """
    prev_state state signal
    -----------------------
    *   L     |  H  | BUY
        L     |  L  | HOLD
        L     |  X  | HOLD
    *   H     |  L  | SELL
        H     |  H  | HOLD
        H     |  X  | HOLD
    *   X     |  L  | SELL
    *   X     |  H  | BUY
        X     |  X  | HOLD

    :param ind:
    :param prev_state:
    :return:
    """

    if ind['20_ema'] > ind['55_sma']:
        state = 'H'

    elif ind['20_ema'] < ind['55_sma']:
        state = 'L'

    else:
        state = 'X'

    if (prev_state == 'L' and state == 'H') or (prev_state == 'X' and state == 'H'):
        return 'BUY', state

    elif (prev_state == 'H' and state == 'L') or (prev_state == 'X' and state == 'L'):
        return 'SELL', state

    else:
        return 'HOLD', state


def main():

    account = Account(2500)

    strategy_001 = Strategy('1 min 55 SMA 20 EMA Cross',
                            'EUR_USD_2017.csv',
                            {add_sma: {'period': 55},
                             add_ema: {'period': 20}},
                            .01,
                            account,
                            ma_55_20_cross,
                            '2017-01-01',
                            '2017-12-31',
                            7,
                            20,
                            1,
                            0,
                            10000)

    strategy_001.load_history()
    strategy_001.add_indicators()
    strategy_001.run_strategy()

    # save_history_range('EUR_USD',
    #                    'EUR_USD_2012.csv',
    #                    date(2012, 1, 1),
    #                    date(2012, 12, 31),
    #                    granularity='M1',
    #                    price='BA')


    #live_feed('EUR_USD')


#    candle_df = add_sma(candle_df, 55, 'bid_close', 'bid_sma_55')
#    candle_df = add_sma(candle_df, 55, 'ask_close', 'ask_sma_55')
#    candle_df = add_ema(candle_df, 20, 'bid_close', 'bid_ema_20')
#    candle_df = add_ema(candle_df, 20, 'ask_close', 'ask_ema_20')
#    print(candle_df)


if __name__ == "__main__":
    main()

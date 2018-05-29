import v20
import pandas as pd
import oa_config
from datetime import datetime


def add_sma(df, period, column, name):

    df[name] = df[column].rolling(window=period).mean()

    return df


def add_ema(df, period, column, name):

    df[name] = df[column].ewm(span=period, adjust=False).mean()

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


def history(instrument):

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

    response = oa_api.instrument.candles(instrument=instrument,
                                         granularity='M1',
                                         price='BA',
                                         count=2000)

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


def main():

    live_feed('EUR_USD')

    # candle_df = history('EUR_USD')
    # candle_df = add_sma(candle_df, 55, 'bid_close', 'bid_sma_55')
    # candle_df = add_sma(candle_df, 55, 'ask_close', 'ask_sma_55')
    # candle_df = add_ema(candle_df, 20, 'bid_close', 'bid_ema_20')
    # candle_df = add_ema(candle_df, 20, 'ask_close', 'ask_ema_20')
    # print(candle_df)


if __name__ == "__main__":
    main()

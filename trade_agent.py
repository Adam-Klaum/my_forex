import logging
import logging.handlers
import trade_config
import trade_queue
import multiprocessing
from datetime import datetime
import pandas as pd
import trade_candle


def live_feed(inst, oa_api, oa_cfg, tick_queue):

    # TODO add logging

    response = oa_api.pricing.stream(oa_cfg.active_account,
                                     snapshot=False,
                                     instruments=inst)

    for msg_type, msg in response.parts():
        if msg_type == "pricing.Price":
            tick_queue.put(msg)


def candlestick_maker(inst, tick_queue, event_queue):

    # TODO add logging, log tick if DEBUG is set

    tick_df = pd.DataFrame(columns=['tick_date',
                                    'bid',
                                    'ask'
                                    ])

    tick_df_idx = 0
    last_m1 = -1

    while True:
        while not tick_queue.empty():

            msg = tick_queue.get()
            date_part, minute, _ = msg.time.split(':')
            minute = int(minute)

            if last_m1 == -1:
                last_m1 = minute

            tick_date, tick_hour = date_part.split('T')
            tick_date = datetime.strptime(tick_date + ' ' + tick_hour + ':' + str(minute), '%Y-%m-%d %H:%M')

            bid = float(msg.bids[0].price)
            ask = float(msg.asks[0].price)

            # If the minute rolled over make a candle
            if minute != last_m1:

                bid_op = tick_df.iloc[0]['bid']
                bid_hi = max(tick_df['bid'])
                bid_lo = min(tick_df['bid'])
                bid_cl = tick_df.iloc[-1]['bid']

                ask_op = tick_df.iloc[0]['ask']
                ask_hi = max(tick_df['ask'])
                ask_lo = min(tick_df['ask'])
                ask_cl = tick_df.iloc[-1]['ask']

                m1_candle = trade_candle.Candle(tick_df.iloc[-1]['tick_date'],
                                                inst,
                                                'M1',
                                                bid_op,
                                                bid_hi,
                                                bid_lo,
                                                bid_cl,
                                                ask_op,
                                                ask_hi,
                                                ask_lo,
                                                ask_cl
                                                )

                event_queue.put(m1_candle)

                print(tick_df)

                print(m1_candle.__dict__)

                tick_df = pd.DataFrame(columns=['tick_date',
                                                'bid',
                                                'ask'
                                                ])

            tick_df.loc[tick_df_idx] = [tick_date, bid, ask]
            tick_df_idx += 1
            last_m1 = minute


def main():

    # TODO move logging setup to a file

    # Setting up global logger
    logfile = 'trade_agent.log'

    logger = logging.getLogger('trade_agent')
    logger.setLevel(logging.DEBUG)

    file_handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=5000000, backupCount=5)
    file_handler.setLevel(logging.DEBUG)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.ERROR)

    formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    logger.info('Log Initialization')

    # Retrieving data for each forex instrument
    fx_info = trade_config.init_fx_info('fx_inst.json')

    oa_cfg = trade_config.OAConf('/home/aklaum/v20.conf')

    # Setting up the Oanda API
    oa_api = trade_config.init_oa_api(oa_cfg)

    fifo_queue = trade_queue.FIFOQueue()

    # feed = live_feed('EUR_USD', oa_api, oa_cfg)

    tick_queue = multiprocessing.Queue()
    event_queue = multiprocessing.Queue()

    p1 = multiprocessing.Process(target=candlestick_maker, args=('EUR_USD', tick_queue, event_queue,))
    p2 = multiprocessing.Process(target=live_feed, args=('EUR_USD', oa_api, oa_cfg, tick_queue,))

    p1.start()
    p2.start()


if __name__ == "__main__":
    main()


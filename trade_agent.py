import logging
import logging.handlers
import trade_config
import multiprocessing
from datetime import datetime
import pandas as pd
import trade_candle


def live_feed(inst, oa_api, oa_cfg, tick_queue, log_queue, feed_pipe_c):

    log_queue.put(['INFO', 'live_feed', 'Starting live feed...'])

    response = oa_api.pricing.stream(oa_cfg.active_account,
                                     snapshot=False,
                                     instruments=inst)

    if response.status != 200:
        log_queue.put(['ERROR', 'live_feed', 'Oanda API call returned - ' + str(response.status)])
        feed_pipe_c.send('FATAL')
        return

    for msg_type, msg in response.parts():
        if feed_pipe_c.poll():
            msg = feed_pipe_c.recv()
            if msg == 'KILL':
                return

        if msg_type == "pricing.Price":
            tick_queue.put(msg)


def candlestick_maker(inst, tick_queue, event_queue, log_queue, candle_pipe_c):

    # TODO add logging, log tick if DEBUG is set

    log_queue.put(['INFO', 'candlestick_maker', 'Starting candle maker ...'])

    tick_df = pd.DataFrame(columns=['tick_date',
                                    'bid',
                                    'ask'
                                    ])

    tick_df_idx = 0
    last_m1 = -1

    while True:
        if candle_pipe_c.poll():
            msg = candle_pipe_c.recv()
            if msg == 'KILL':
                return

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

                # print(tick_df)
                # print(m1_candle.__dict__)

                tick_df = pd.DataFrame(columns=['tick_date',
                                                'bid',
                                                'ask'
                                                ])

                log_queue.put(['INFO', 'candlestick_maker', 'M1 candle generated for: ' + str(tick_date)])

            tick_df.loc[tick_df_idx] = [tick_date, bid, ask]
            tick_df_idx += 1
            last_m1 = minute


def log_handler(log_queue, log_pipe_c):

    log_levels = {'CRITICAL': 50,
                  'ERROR': 40,
                  'WARNING': 30,
                  'INFO': 20,
                  'DEBUG': 10,
                  'NOTSET': 0
                  }

    # Setting up global logger
    logfile = 'trade_agent.log'

    logger = logging.getLogger('trade_agent')
    logger.setLevel(logging.DEBUG)

    file_handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=5000000, backupCount=5)
    file_handler.setLevel(logging.DEBUG)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.ERROR)

    formatter = logging.Formatter('%(asctime)s:%(message)s')
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    logger.info('INFO: log_handler: Log Initialization')

    while True:

        while not log_queue.empty():
            log_msg = log_queue.get()
            log_level = log_msg[0]
            log_final_msg = ': '.join(log_msg)
            logger.log(log_levels[log_level], log_final_msg)

        if log_pipe_c.poll():
            msg = log_pipe_c.recv()
            if msg == 'KILL':
                return


def main():

    # TODO how to implement this in queues
    # Retrieving data for each forex instrument
    fx_info = trade_config.init_fx_info('fx_inst.json')

    oa_cfg = trade_config.OAConf('/home/aklaum/v20.conf')

    # Setting up the Oanda API
    oa_api = trade_config.init_oa_api(oa_cfg)

    log_queue = multiprocessing.Queue()

    log_queue.put(['INFO', 'main', 'Initializing...'])

    tick_queue = multiprocessing.Queue()
    event_queue = multiprocessing.Queue()

    log_pipe_p, log_pipe_c = multiprocessing.Pipe(duplex=True)
    candle_pipe_p, candle_pipe_c = multiprocessing.Pipe(duplex=True)
    feed_pipe_p, feed_pipe_c = multiprocessing.Pipe(duplex=True)

    parent_pipes = [log_pipe_p, candle_pipe_p, feed_pipe_p]

    log_proc = multiprocessing.Process(target=log_handler, args=(log_queue, log_pipe_c))

    candle_proc = multiprocessing.Process(target=candlestick_maker,
                                          args=('EUR_USD', tick_queue, event_queue, log_queue, candle_pipe_c))

    feed_proc = multiprocessing.Process(target=live_feed,
                                        args=('EUR_USD', oa_api, oa_cfg, tick_queue, log_queue, feed_pipe_c))

    all_procs = [candle_proc, feed_proc, log_proc]

    log_proc.start()
    candle_proc.start()
    feed_proc.start()

    while True:

        # Checking all child proc pipes for incoming messages
        for pipe in parent_pipes:
            if pipe.poll():
                msg = pipe.recv()

                # If any child proc sends a FATAL message, kill all the others and exit
                if msg == 'FATAL':
                    log_queue.put(['ERROR', 'main', 'FATAL received! Exiting...'])
                    for p_pipe in parent_pipes:
                        p_pipe.send('KILL')

                    for proc in all_procs:
                        proc.join()

                    exit(1)


if __name__ == "__main__":
    main()


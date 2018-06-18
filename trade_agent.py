import logging
import logging.handlers
import trade_config
import multiprocessing
from datetime import datetime
import pandas as pd
import trade_candle
import time
import sys


def hist_feed(inst, hist_file, log_queue, feed_pipe_c):

    log_queue.put(['INFO', 'hist_feed', 'Loading history data for - ' + inst])
    hist_df = pd.read_csv(hist_file)

    test = 0

    # itertuples is faster than iterlist?
    for row in hist_df.itertuples():

        if test == 9:
            feed_pipe_c.send('FATAL')
            return

        if feed_pipe_c.poll():
            msg = feed_pipe_c.recv()
            if msg == 'KILL':
                return

        print(row)
        time.sleep(1)
        test += 1


def live_feed(inst, oa_api, oa_cfg, tick_queue, log_queue, feed_pipe_c):
    """

    Connects to the Oanda v20 API and initiates a stream of quotes for
    the specified instrument. Each tick received from Oanda is put on
    the tick_queue for further processing.

    :param inst: the forex instrument to retrieve (i.e. EUR_USD)
    :param oa_api: a previously initiated Oanda API object
    :param oa_cfg: an object containing Oanda configuration attributes
    :param tick_queue: this function writes each quote to this queue
    :param log_queue: a general log queue
    :param feed_pipe_c: an IPC pipe to talk to the main process
    :return: None

    """

    # TODO capture timeout error

    log_queue.put(['INFO', 'live_feed', 'Starting live feed...'])

    try:

        response = oa_api.pricing.stream(oa_cfg.active_account,
                                         snapshot=False,
                                         instruments=inst)

    # TODO why is this not in scope?
    except V20ConnectionError:

        log_queue.put(['ERROR', 'live_feed', 'connection error'])
        feed_pipe_c.send('FATAL')
        return

    # if the stream call gets back anything but 200 throw an error and exit
    if response.status != 200:
        log_queue.put(['ERROR', 'live_feed', 'Oanda API call returned - ' + str(response.status)])
        feed_pipe_c.send('FATAL')
        return

    # main loop for retrieving tick data
    for msg_type, msg in response.parts():

        # if a KILL message is received from the main process then exit
        if feed_pipe_c.poll():
            msg = feed_pipe_c.recv()
            if msg == 'KILL':
                return

        if msg_type == "pricing.Price":
            tick_queue.put(msg)


def candlestick_maker(inst, tick_queue, event_queue, log_queue, candle_pipe_c):
    """

    Creates standard OHLC candlesticks from tick data.  Tick data is retrieved from the
    tick_queue and stored in a pandas dataframe.  Once the specified timeframe has passed,
    calculations are made on the dataframe to create OHLC values and a candle is sent to
    the event_queue for further processing.

    :param inst:
    :param tick_queue:
    :param event_queue:
    :param log_queue:
    :param candle_pipe_c:
    :return: None

    """

    # TODO add a parameter to control the timeframe of the candle
    # TODO the inst parameter shouldn't be needed, that data should be part of the tick msg

    log_queue.put(['INFO', 'candlestick_maker', 'Starting candle maker ...'])

    tick_df = pd.DataFrame(columns=['tick_date',
                                    'bid',
                                    'ask'
                                    ])

    tick_df_idx = 0
    last_m1 = -1

    while True:

        # Checking for messages from the main process
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
    """
    Process to handle logging to a file.  All other processes send their log messages to
    this one.

    :param log_queue: queue for all incoming log message
    :param log_pipe_c: pipe to communicate with the main process
    :return: None

    """

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


def cleanup(parent_pipes, all_procs, log_queue):

    print('starting cleanup')
    log_queue.put(['INFO', 'main', 'Sending the KILL signal to child processes...'])
    for p_pipe in parent_pipes:
        print(p_pipe)
        p_pipe.send('KILL')
    for proc in all_procs:
        print('joining')
        proc.join()

    sys.exit(0)


def main():

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


    log_proc = multiprocessing.Process(target=log_handler, args=(log_queue, log_pipe_c))

    # TODO parameterize this and the instruments below
    # TODO make this section more pythonic
    go_live = 0

    if go_live:

        feed_proc = multiprocessing.Process(target=live_feed,
                                            args=('EUR_USD', oa_api, oa_cfg, tick_queue, log_queue, feed_pipe_c))

        candle_proc = multiprocessing.Process(target=candlestick_maker,
                                              args=('EUR_USD', tick_queue, event_queue, log_queue, candle_pipe_c))

        all_procs = [candle_proc, feed_proc, log_proc]
        parent_pipes = [log_pipe_p, candle_pipe_p, feed_pipe_p]

        candle_proc.start()

    else:
        feed_proc = multiprocessing.Process(target=hist_feed,
                                            args=('EUR_USD',  'data/EUR_USD_2017_M1.csv', log_queue, feed_pipe_c))

        all_procs = [feed_proc, log_proc]
        parent_pipes = [log_pipe_p, feed_pipe_p]

    log_proc.start()
    feed_proc.start()

    # TODO use "import daemon" to manage all of this
    # TODO Need a graceful exit code from child processes in addition to FATAL

    while True:

        try:
            # Checking all child proc pipes for incoming messages
            for pipe in parent_pipes:
                if pipe.poll():
                    msg = pipe.recv()

                    # If any child proc sends a FATAL message, kill all the others and exit
                    if msg == 'FATAL':
                        log_queue.put(['ERROR', 'main', 'FATAL received! Exiting...'])
                        sys.exit(0)

        except SystemExit:
            print('caught system exit')
            cleanup(parent_pipes, all_procs, log_queue)

        except KeyboardInterrupt:
            cleanup(parent_pipes, all_procs, log_queue)
            sys.exit(0)


if __name__ == "__main__":
    main()


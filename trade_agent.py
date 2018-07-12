import logging
import logging.handlers
import config
import pandas as pd
import candle
import sys
import time
from multiprocessing import Queue, Pipe, Process
from datetime import datetime
import v20


class HistFeed(Process):

    def __init__(self, inst, hist_file, log_queue, event_queue, comm_queue, **kwargs):
        super(HistFeed, self).__init__()
        self.inst = inst
        self.hist_file = hist_file
        self.log_queue = log_queue
        self.event_queue = event_queue
        self.comm_queue = comm_queue

    def run(self):

        self.log_queue.put(['INFO', self.name, 'Loading history file %s data for %s ' % (self.hist_file, self.inst)])
        hist_df = pd.read_csv(self.hist_file)

        test = 0

        for row in hist_df.itertuples():

            candle = trade_candle.Candle(row.datetime,
                                         row.instrument,
                                         'M1',
                                         row.bid_open,
                                         row.bid_high,
                                         row.bid_low,
                                         row.bid_close,
                                         row.ask_open,
                                         row.ask_high,
                                         row.ask_low,
                                         row.ask_close
                                         )

            self.event_queue.put({'msg_type': 'CANDLE', 'msg': candle})

            if test == 10:
                self.log_queue.put(['INFO', self.name, 'History processing complete'])
                self.comm_queue.put(['FATAL', self.name])
                break
                # return
            # if self.feed_pipe_c.poll():
            #     msg = self.feed_pipe_c.recv()
            #     if msg == 'KILL':
            #         return

            test += 1

        return


class LiveFeed(Process):

    def __init__(self, inst, oa_api, oa_cfg, tick_queue, log_queue, **kwargs):
        super(LiveFeed, self).__init__()
        self.inst = inst
        self.oa_api = oa_api
        self.oa_cfg = oa_cfg
        self.tick_queue = tick_queue
        self.log_queue = log_queue

    def run(self):

        self.log_queue.put(['INFO', self.name, 'Starting live feed...'])

        try:
            response = self.oa_api.pricing.stream(self.oa_cfg.active_account,
                                                  snapshot=False,
                                                  instruments=self.inst)

        except v20.errors.V20ConnectionError:

            self.log_queue.put(['ERROR', self.name, 'API connection error'])
            # self.feed_pipe_c.send('FATAL')
            return

        except v20.errors.V20Timeout:
            self.log_queue.put(['ERROR', self.name, 'API timeout error'])
            # self.feed_pipe_c.send('FATAL')
            return

        # if the stream call gets back anything but 200 throw an error and exit
        if response.status != 200:
            self.log_queue.put(['ERROR', self.name, 'Oanda API call returned - ' + str(response.status)])
            # self.feed_pipe_c.send('FATAL')
            return

        # main loop for retrieving tick data
        for msg_type, msg in response.parts():

            # if a KILL message is received from the main process then exit
            # if self.feed_pipe_c.poll():
            #     msg = self.feed_pipe_c.recv()
            #     if msg == 'KILL':
            #         return

            if msg_type == "pricing.Price":
                print(msg)
                self.tick_queue.put(msg)


class CandleMaker(Process):

    def __init__(self, inst, tick_queue, event_queue, log_queue, candle_pipe_c, **kwargs):
        super(CandleMaker, self).__init__()
        self.inst = inst
        self.tick_queue = tick_queue
        self.event_queue = event_queue
        self.log_queue = log_queue
        self.candle_pipe_c = candle_pipe_c

    def run(self):

        # TODO add a parameter to control the timeframe of the candle
        # TODO the inst parameter shouldn't be needed, that data should be part of the tick msg

        self.log_queue.put(['INFO', self.name, 'Starting candle maker ...'])

        tick_df = pd.DataFrame(columns=['tick_date',
                                        'bid',
                                        'ask'
                                        ])

        tick_df_idx = 0
        last_m1 = -1

        while True:

            # Checking for messages from the main process
            if self.candle_pipe_c.poll():
                msg = self.candle_pipe_c.recv()
                if msg == 'KILL':
                    return

            while not self.tick_queue.empty():

                msg = self.tick_queue.get()

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
                                                    self.inst,
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

                    self.event_queue.put(m1_candle)

                    tick_df = pd.DataFrame(columns=['tick_date',
                                                    'bid',
                                                    'ask'
                                                    ])

                    self.log_queue.put(['INFO', self.name, 'M1 candle generated for: ' + str(tick_date)])

                tick_df.loc[tick_df_idx] = [tick_date, bid, ask]
                tick_df_idx += 1
                last_m1 = minute


class LogHandler(Process):

    def __init__(self, log_queue, **kwargs):
        super(LogHandler, self).__init__()
        self.log_queue = log_queue
        # self.log_pipe_c = log_pipe_c
        self.log_levels = {'CRITICAL': 50,
                           'ERROR': 40,
                           'WARNING': 30,
                           'INFO': 20,
                           'DEBUG': 10,
                           'NOTSET': 0
                           }

        self.logfile = 'trade_agent.log'

        self.logger = logging.getLogger('trade_agent')
        self.logger.setLevel(logging.DEBUG)

        self.file_handler = logging.handlers.RotatingFileHandler(self.logfile, maxBytes=5000000, backupCount=5)
        self.file_handler.setLevel(logging.DEBUG)

        self.stream_handler = logging.StreamHandler()
        self.stream_handler.setLevel(logging.ERROR)

        self.formatter = logging.Formatter('%(asctime)s:%(message)s')
        self.file_handler.setFormatter(self.formatter)
        self.stream_handler.setFormatter(self.formatter)

        self.logger.addHandler(self.file_handler)
        self.logger.addHandler(self.stream_handler)

    def run(self):

        self.logger.info('INFO: %s: Logging Initialized...' % self.name)

        while True:

            while not self.log_queue.empty():
                log_msg = self.log_queue.get()
                log_level = log_msg[0]
                log_final_msg = ': '.join(log_msg)
                self.logger.log(self.log_levels[log_level], log_final_msg)

            # if self.log_pipe_c.poll():
            #     msg = self.log_pipe_c.recv()
            #     if msg == 'KILL':
            #         return


class MainProc(Process):

    def __init__(self, fx_info, oa_cfg, oa_api, go_live, **kwargs):
        super(MainProc, self).__init__()
        self.fx_info = fx_info
        self.oa_cfg = oa_cfg
        self.oa_api = oa_api
        self.go_live = go_live

        self.log_queue = Queue()
        self.tick_queue = Queue()
        # self.event_queue = Queue()
        # self.comm_queue = Queue()

        # self.log_pipe_p, self.log_pipe_c = Pipe(duplex=True)
        # self.candle_pipe_p, self.candle_pipe_c = Pipe(duplex=True)
        # self.feed_pipe_p, self.feed_pipe_c = Pipe(duplex=True)

        self.candle_proc = None
        self.feed_proc = None
        self.log_proc = None

        self.all_procs = []
        # self.parent_pipes = []

        # self.event = None

    def run(self):

        self.log_proc = LogHandler(self.log_queue)

        self.log_proc.start()

        self.log_queue.put(['INFO', self.name, 'Starting main process'])

        if self.go_live:

            self.feed_proc = LiveFeed('EUR_USD', self.oa_api, self.oa_cfg,
                                      self.tick_queue, self.log_queue)

            # self.candle_proc = CandleMaker('EUR_USD', self.tick_queue,
            #                                self.log_queue)

            self.all_procs = [self.candle_proc, self.feed_proc]

        else:
            self.feed_proc = HistFeed('EUR_USD', 'data/EUR_USD_2017_M1.csv', self.log_queue,
                                      self.event_queue, self.comm_queue)

            self.all_procs = [self.feed_proc]
            # self.parent_pipes = [self.log_pipe_p, self.feed_pipe_p]

        for proc in self.all_procs:
            proc.start()

        self.all_procs.append(self.log_proc)

        # TODO use "import daemon" to manage all of this
        # TODO Need a graceful exit code from child processes in addition to FATAL

        # while True:
        #
        #     try:
        #         msg = self.comm_queue.get_nowait()
        #
        #     except Queue.Empty:
        #         print('queue empty')
        #         pass
        #
        #     finally:
        #         if msg[0] == 'FATAL':
        #             self.log_queue.put(['ERROR', self.name, 'FATAL received from process %s!' % msg[1]])
        #             # self.cleanup()
        #             return
        #
        #
        #     # for pipe in self.parent_pipes:
        #     #     if pipe.poll():
        #     #         msg = pipe.recv()
        #     #
        #     #         # If any child proc sends a FATAL message, kill all the others and exit
        #     #         if msg == 'FATAL':
        #     #             self.log_queue.put(['ERROR', self.name, 'FATAL received!'])
        #     #             self.cleanup()
        #     #             return
        #
        #     while not self.event_queue.empty():
        #         self.event = self.event_queue.get()

        for proc in self.all_procs:
            proc.join()

    def cleanup(self):

        self.log_queue.put(['INFO', self.name, 'Sending the KILL signal to child processes...'])

        for p_pipe in self.parent_pipes:
            p_pipe.send('KILL')

        for proc in self.all_procs:
            proc.join()

        return


def main():

    # Retrieving data for each forex instrument
    fx_info = trade_config.init_fx_info('fx_inst.json')

    # Setting up the Oanda API
    oa_cfg = trade_config.OAConf('/home/aklaum/v20.conf')
    oa_api = trade_config.init_oa_api(oa_cfg)

    main_proc = MainProc(fx_info, oa_cfg, oa_api, True)
    main_proc.start()

    main_proc.join()
    sys.exit(0)


if __name__ == "__main__":
    main()


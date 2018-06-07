class Candle(object):

    def __init__(self, dt, inst, time_frame,
                 bid_op, bid_hi, bid_lo, bid_cl,
                 ask_op, ask_hi, ask_lo, ask_cl ):

        self.dt = dt
        self.inst = inst
        self.time_frame = time_frame
        self.bid_op = bid_op
        self.bid_hi = bid_hi
        self.bid_lo = bid_lo
        self.bid_cl = bid_cl
        self.ask_op = ask_op
        self.ask_hi = ask_hi
        self.ask_lo = ask_lo
        self.ask_cl = ask_cl
        self.spread = abs(ask_cl - bid_cl)



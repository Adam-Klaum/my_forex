from pytest import *
from candle import Candle, CandleMaker, ProcessKilled
from datetime import datetime
from decimal import Decimal, DecimalException
from multiprocessing import Queue, Pipe, Process

tick_queue = Queue()
candle_proc = CandleMaker(tick_queue)

candle_proc.start()

tick_queue.put('KILL')

candle_proc.join()





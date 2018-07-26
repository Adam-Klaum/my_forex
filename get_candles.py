from tradeagent.config import config
import sys
import v20
from datetime import datetime, timedelta
from tradeagent.utils import db_connect


def main():

    con = db_connect()
    cur = con.cursor()

    start_date = datetime(2017, 10, 16)
    end_date = datetime(2017, 12, 31)

    oa_api = v20.Context(config.hostname,
                         config.port,
                         token=config.token)

    candle_sql = '''
        INSERT OR IGNORE INTO raw_candle 
        (time, type, instrument, bid_open, bid_high, bid_low, bid_close, ask_open, ask_high, ask_low, ask_close) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        '''

    while start_date < end_date:

        from_time = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        to_time = (start_date + timedelta(hours=23, minutes=59, seconds=59)).strftime('%Y-%m-%dT%H:%M:%SZ')

        response = oa_api.instrument.candles('EUR_USD', granularity='M1', price='BA',
                                             fromTime=from_time,
                                             toTime=to_time)

        candles = response.get('candles')

        for candle in candles:
            cur.execute(candle_sql, (candle.time, 'EUR_USD',
                                     candle.bid.o, candle.bid.h, candle.bid.l, candle.bid.c,
                                     candle.ask.o, candle.ask.h, candle.ask.l,
                                     candle.ask.c))

            print(candle.time, 'bid', 'EUR_USD',
                  candle.bid.o, candle.bid.h, candle.bid.l,
                  candle.bid.c)

        print("Processed: {}".format(start_date))

        start_date = start_date + timedelta(days=1)

    sys.exit(0)


if __name__ == "__main__":
    main()


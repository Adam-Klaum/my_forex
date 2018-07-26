from tradeagent.utils import db_connect

con = db_connect()
cur = con.cursor()

customers_sql = """
CREATE TABLE IF NOT EXISTS raw_candle (
    time TEXT,
    type TEXT,
    instrument TEXT,
    open REAL,
    high REAL,
    low REAL,
    close,
    PRIMARY KEY (time, type, instrument)
);    
"""
cur.execute(customers_sql)





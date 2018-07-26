from sqlite3 import connect


def db_connect(db_file):
    # TODO which isolation level should be used here
    con = connect(db_file, isolation_level=None)
    return con

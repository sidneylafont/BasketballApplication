import sqlite3
import sys


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except:
        print(sys.exc_info()[0])

    return conn

conn = create_connection('BBSQLitedb.db')

print(conn)

cur = conn.cursor()
cur.execute("SELECT * FROM PerGame")

rows = cur.fetchall()

for row in rows:
    print(row)
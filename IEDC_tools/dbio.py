"""
Database Input / Output functions
"""

import pandas as pd
import pymysql

import IEDC_pass


def db_conn(fn):
    """
    Decorator function to provide a connection to a function. This was originally inspired by
     http://initd.org/psycopg/articles/2010/10/22/passing-connections-functions-using-decorator/

    This ensures that the connection is closed after the connection is closed after the function completes gracefully or
    non-gracefully, i.e. when some kind of exception occurs. This is good practice to ensure there are not too many open
    connections on the server. It also does a rollback() in case of an exception.
    Possibly this could also be solved more simply using `with pymysql.connect()`.
    """

    def db_conn_(*args, **kwargs):
        conn = pymysql.connect(host=IEDC_pass.IEDC_server,
                               port=IEDC_pass.IEDC_port,
                               user=IEDC_pass.IEDC_user,
                               passwd=IEDC_pass.IEDC_pass,
                               db=IEDC_pass.IEDC_database,
                               charset="utf8")
        try:
            rv = fn(conn, *args, **kwargs)
        except (KeyboardInterrupt, SystemExit):
            conn.rollback()
            # conn.close()
            print("Keyboard interupt - don't worry connection was closed")
            raise
        except BaseException as e:
            conn.rollback()
            # conn.close()
            print("Exception: %s" % e)
            print("Something went wrong! But I was smart and closed the connection!")
            raise
        finally:
            pass
            conn.close()
        return rv
    return db_conn_


@db_conn
def get_sql_as_df(conn, table, db=IEDC_pass.IEDC_database, addSQL=''):
    """
    Download a table from the SQL database and return it as a nice dataframe.

    :param conn: Database connection. No need to worry. The decorator takes care of this.
    :param table: table name
    :param db: database name
    :param addSQL: Add more arguments to the SQL query, e.g. "WHERE classification_id = 1"
    :return: Dataframe of SQL table
    """
    # Don't show this to anybody, please. SQL injections are a big nono...
    # https://www.w3schools.com/sql/sql_injection.asp
    df = pd.read_sql("SELECT * FROM %s.%s %s;" % (db, table, addSQL),
                     conn, index_col='id')
    return df

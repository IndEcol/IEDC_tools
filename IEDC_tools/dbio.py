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
                                port=int(IEDC_pass.IEDC_port),
                                user=IEDC_pass.IEDC_user,
                                passwd=IEDC_pass.IEDC_pass,
                                db=IEDC_pass.IEDC_database,
                                charset='utf8')
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


def db_cursor_write(fn):
    """
    Decorator function for the database cursor (writing)
    http://initd.org/psycopg/articles/2010/10/22/passing-connections-functions-using-decorator/
    """

    def db_cursor_write_(*args, **kwargs):
        conn = pymysql.connect(host=IEDC_pass.IEDC_server,
                               port=int(IEDC_pass.IEDC_port),
                               user=IEDC_pass.IEDC_user,
                               passwd=IEDC_pass.IEDC_pass,
                               db=IEDC_pass.IEDC_database,
                               charset='utf8')
        curs = conn.cursor()
        try:
            #print curs, args, kwargs
            rv = fn(curs, *args, **kwargs)
        except (KeyboardInterrupt, SystemExit):
            #print args, kwargs
            conn.rollback()
            conn.close()
            print("Keyboard interupt - don't worry connection was closed")
            raise
        except BaseException as error:
            #print args, kwargs
            conn.rollback()
            conn.close()
            print("Exception: %s" % error)
            print ("But I was smart and closed the connection!")
            raise
        else:
            conn.commit()
            curs.close()
        return rv
    return db_cursor_write_


@db_conn
def get_sql_table_as_df(conn, table, columns=['*'], db=IEDC_pass.IEDC_database,
                        index='id', addSQL=''):
    """
    Download a table from the SQL database and return it as a nice dataframe.

    :param conn: Database connection. No need to worry. The decorator takes care of this.
    :param table: table name
    :param columns: List of columns to get from the SQL table
    :param db: database name
    :param index: Column name to be used as dataframe index. String.
    :param addSQL: Add more arguments to the SQL query, e.g. "WHERE classification_id = 1"
    :return: Dataframe of SQL table
    """
    # Don't show this to anybody, please. SQL injections are a big nono...
    # https://www.w3schools.com/sql/sql_injection.asp
    columns = ', '.join(c for c in columns if c not in "'[]")
    df = pd.read_sql("SELECT %s FROM %s.%s %s;" % (columns, db, table, addSQL),
                     conn, index_col=index)
    return df


@db_cursor_write
def run_this_command(curs, sql_cmd):
    curs.execute(sql_cmd)


@db_cursor_write
def dict_sql_insert(curs, table, d):
    # https://stackoverflow.com/a/14834646/2075003
    placeholder = ", ".join(["%s"] * len(d))
    sql = "INSERT INTO `{table}` ({columns}) VALUES ({values});".format(table=table, columns=",".join(d.keys()),
                                                                        values=placeholder)
    curs.execute(sql, list(d.values()))

@db_cursor_write
def bulk_sql_insert(curs, table, cols, data):
    """

    :param curs:
    :param table:
    :param cols:
    :param data: data as list
    :return:
    """
    sql = """
          INSERT INTO %s
          (%s)
          VALUES (%s);
          """ % (table, ', '.join(cols), ','.join([' %s' for _ in cols]))
    curs.executemany(sql, data)


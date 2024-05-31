from flask import session
import pymysql


class DBConnectionError(Exception):
    pass


# ===================================================
# DB 연결
# ===================================================
def get_db():
    try:
        connection = pymysql.connect(
            host=session['host'],
            port=session['port'],
            user=session['user'],
            password=session['password'],
            database=session['database'],
        )
        return connection
    except pymysql.Error:
        raise DBConnectionError
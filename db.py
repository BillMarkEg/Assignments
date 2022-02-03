import cx_Oracle

def get_db():
        dsn = cx_Oracle.makedsn('localhost','1521','XE')
        conn=cx_Oracle.connect(user= r'HR',password=r'xmaster' ,dsn=dsn)
        cur = conn.cursor()
        return cur


def close_db(cur):
        cur.close()

import pymysql


def connect():
    con = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='zrshi250')
    cur = con.cursor()
    values = []
    for i in range(1000):
        value = f"""('qwe{i}')"""
        values.append(value)
    sql = f"""insert into test values {','.join(values)}"""

    cur.execute("use haha")
    cur.execute(sql)
    cur.execute('select count(*) from test')
    print(cur.fetchall())
    con.commit()
    cur.execute('select count(*) from test')
    con.close()


if __name__ == '__main__':
    connect()

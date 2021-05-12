# -*- encoding:utf-8 -*-

"""
@Author  : chenxingcong
@Contact : chenxingcong@mail01.huawei.com
@File    : GbaseSQLEngine.py
@Time    : 2021/2/26 10:00
@Desc    : 
"""

import logging
import sys

from GBaseConnector import connect, GBaseError
from random import choice

logger = logging.getLogger()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)


class GbaseSQLEngine(object):

    def __init__(self):
        self.conn = connect()  # 数据库连接
        self.cursor = self.init_conn()

    def init_conn(self):
        host_list = ['133.95.12.1', '133.95.12.11', '133.95.12.21', '133.95.12.31', '133.95.12.41']
        host = choice(host_list)
        logger.info("Choose Host: " + host)
        # 初始化数据库连接
        config = {'host': host,
                  'user': 'hw',
                  'passwd': 'Create@1209',
                  'port': 5258,
                  'db': 'mk',
                  'charset': 'utf8',
                  'connect_timeout': 10800}
        try:
            self.conn.connect(**config)
            self.cursor = self.conn.cursor()
            logger.info("Connect Success")
        except:
            logger.error("Connect Database failed")
            self.conn.close()
            sys.exit(2)
        return self.cursor

    def close(self):
        self.cursor.close()
        self.conn.close()

    def commit(self):
        self.conn.commit()


if __name__ == '__main__':
    gbase = GbaseSQLEngine()
    cursor = gbase.cursor
    # sql = "delete from pub.tdet_hb_modeldatamonitor_d where table_name='dwh.td_rs_cm_subs_reward_d' and record_cycle='20210224' and target_name='oid' and statis_date='20210224'"
    sql = "select count(1) from mk.tm_sc_user_tmnl_m_202101"
    print(sql)
    cursor.execute(sql)
    rows = cursor.fetchall()
    print(str(rows))
    gbase.close()

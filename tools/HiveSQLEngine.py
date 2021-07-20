# -*- encoding:utf-8 -*-

"""
@Author  : chenxingcong
@Contact : chenxingcong@mail01.huawei.com
@File    : HiveSQLEngine.py
@Time    : 2021/2/22 15:36
@Desc    : 
"""

import re
import logging
import hive_init_cc
from kazoo.client import KazooClient
from random import choice
import datetime
import sys

logger = logging.getLogger()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)


class HiveSQLEngine(object):
    def __init__(self):
        # self.conn=None    # 数据库连接
        # self.cursor=None  # 数据库游标
        # self.sql_list =[]  # SQL列表
        # # self.date_str = datestr # 目标表日期
        # self.input=input
        # # self.log_path = log_path
        # self.para_dic = {}  # 参数列表

        # _console_handler = logging.StreamHandler()
        # _console_handler.setLevel(level=logging.DEBUG)
        # _console_handler.setFormatter(formatter)
        # logger.addHandler(_console_handler)

        self.queuename = "root.hbyz"
        self.init_conn()

    def init_conn(self):
        zkClient = KazooClient(hosts="127.0.0.3:2181,"
                                     "127.0.0.4:2181,"
                                     "127.0.0.5:2181,"
                                     "127.0.0.12:2181,"
                                     "127.0.0.13:2181")
        zkClient.start()
        result = zkClient.get_children("/hiveserver2")
        zkClient.stop()

        if len(result) == 0:
            logger.error("No Available Hive Server")
            sys.exit(9)

        host_list = []
        for element in result:
            host_tmp = re.findall(r"serverUri=(.+?):", element)

            if len(host_tmp) > 0:
                host_list.append(host_tmp[0])

        if len(host_list) == 0:
            logger.error("No Available Hive Server")
            sys.exit(9)

        host_mapping = {
            "bdmn003": '127.0.0.3',
            "bdmn004": '127.0.0.4',
            "bdmn005": '127.0.0.5',
            "bdmn008": '127.0.0.8',
            "bdmn012": '127.0.0.12',
            "bdmn013": '127.0.0.13'
        }
        host = host_mapping[choice(host_list)]
        logger.info("Available Host:" + ",".join(host_list))
        logger.info("Choose Host:" + host)
        # 初始化数据库连接
        try:
            self.conn = hive_init_cc.Connection(host=host,
                                                port=10000,
                                                auth="KERBEROS",
                                                kerberos_service_name="hive",
                                                database="dwh")
            self.cursor = self.conn.cursor()
            self.cursor.execute('set mapreduce.job.queuename = ' + self.queuename)
            self.cursor.execute('set hive.exec.compress.output=true')
            self.cursor.execute('set mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec')
            logger.info("set queuneame " + self.queuename + " finished!")
            logger.info("set mapred.output.compression.codec=SnappyCodec" + " finished!")
        except Exception as e:
            print(e)
            logger.error("Connect Database failed")
            if self.conn:
                self.conn.close()
            sys.exit(2)

    def close(self):
        self.cursor.close()
        self.conn.close()


if __name__ == '__main__':
    pass

# -*- encoding:utf-8 -*-

"""
@Author  : zhangrui
@Contact : zhangrui2@mail01.huawei.com
@Time    : 2021/6/1w
@Desc    :
"""
import sys

sys.path.append('/data/script/hbdb/py')
from HiveSQLEngine import HiveSQLEngine as sqlEngine
import re
import logging
import os
import threading
import calendar
import datetime
import queue


class dataAnalyze:
    def __init__(self, table=None, date_str=None):
        self.table = table
        self.script_root_dir = '/data/script/hbdb/'
        self.script = ''
        self.queue = queue.Queue()
        self.dateStr = str(date_str)
        self.__logger, self.__err_logger = self.__get_logger()
        if re.findall(r'\.sql', self.table):
            table = os.path.basename(table)
            self.script = self.script_root_dir + re.split('_', self.table, 1)[0] + '/' + table
            self.__logger.info(self.script)
            if os.path.isfile(self.script):
                self.__logger.info('脚本存在，将查询所有依赖')
            else:
                self.__logger.info('脚本不存在，请检查参数')
                sys.exit()

    def __execute_sql(self, sql, __cursor):
        self.__logger.info('开始执行sql :' + sql)
        try:
            __cursor = __cursor
            __cursor.execute(sql)
            __rows = __cursor.fetchall()  # list[tuple]
            self.__logger.info('%s 查询成功', sql)
            return __rows
        except Exception as e:
            if re.findall(r'Table not found', str(e), re.I):
                self.__err_logger.info('%s 查询失败： \n 没有这个表' % sql)
                return []
            elif re.findall('is not a partitioned table', str(e)):
                self.__err_logger.info('%s 查询结束：\n 没有分区' % sql)
                return [('1=1',)]
            else:
                self.__err_logger.info('%s 查询失败： \n%s' % (sql, e))
                sys.exit(e)

    def __get_logger(self, ):
        logger = logging.getLogger('info')
        err_logger = logging.getLogger('err')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        logger.setLevel(logging.DEBUG)
        err_logger.setLevel(logging.DEBUG)
        input_file_name = os.path.basename('./log.log').split(".")[0]
        basedir = os.path.dirname('./log.log')
        log_path = os.path.join(basedir, input_file_name + ".log")
        err_path = os.path.join(basedir, input_file_name + ".err")
        _file_handler = logging.FileHandler(log_path)
        err_handler = logging.FileHandler(err_path)
        _file_handler.setLevel(level=logging.INFO)
        err_handler.setLevel(level=logging.INFO)
        _file_handler.setFormatter(formatter)
        err_handler.setFormatter(formatter)
        logger.addHandler(_file_handler)
        err_logger.addHandler(err_handler)

        # 添加日志控制台输出
        _console_handler = logging.StreamHandler()
        _console_handler.setLevel(level=logging.INFO)
        _console_handler.setFormatter(formatter)
        logger.addHandler(_console_handler)
        err_logger.addHandler(_console_handler)
        return logger, err_logger

    def get_partitions(self, table_name, __cursor):
        self.__logger.info('开始分析分区')
        table_name = table_name
        sql = 'show partitions %s' % table_name
        __rows = self.__execute_sql(sql, __cursor)
        if len(self.dateStr) == 8:
            vl_month = datetime.datetime.strptime(self.dateStr[:6] + '01', '%Y%m01') - datetime.timedelta(days=1)
            pattern = r'=%s$|=%s$|1=1' % (self.dateStr, vl_month)
        elif len(self.dateStr) == 6:
            weekday, monthdays = calendar.monthrange(int(self.dateStr[:4]), int(self.dateStr[4:6]))
            vi_month_last = self.dateStr[:6] + str(monthdays)
            pattern = r'=%s$|%s$|1=1' % (self.dateStr, vi_month_last)
        else:
            sys.exit('日期输入不正确')
        for row in __rows:
            p_date, = map(str, row)
            if re.findall(pattern, p_date):
                self.__err_logger.info('*' * 100 + '\n分区:%s' % p_date)
                return p_date
        return 0

    def get_count(self, table_name):
        try:
            __connect_db = sqlEngine()
            __cursor = __connect_db.cursor
        except Exception as e:
            self.__err_logger.info('连接数据库失败,退出')
            sys.exit(e)

        self.__logger.info('开始计数')
        table_name = table_name
        partition_date = self.get_partitions(table_name, __cursor)
        if partition_date:
            sql = 'select count(*) from %s where %s ' % (table_name, partition_date)
            result = self.__execute_sql(sql, __cursor)  # list[tuple]
            count = str(result[0][0])
            # count, = result[0] 可读性低
            self.queue.put((table_name, count))
        else:
            self.queue.put((table_name, 0))
        try:
            __connect_db.close()
            __cursor.close()
            self.__logger.info('数据库连接已关闭')
        except Exception as e:
            self.__err_logger.info(e)
            self.__err_logger.info('数据库连接关闭失败')

    def get_dep(self, file_name, pattern=r'mk\.|pub\.|dis\.|dw\.|dwh\.|am\.|det\.'):
        self.__logger.info('开始分析依赖表们')
        lis = []
        pattern = r'(?=%s)[a-zA-Z0-9_\.]*(?=|;|,|\s|_\$|\))' % pattern
        # pattern = r'(?=%s)[a-zA-Z0-9_\.]*_[a-zA-Z0-9]+(?=|;|,|\s|_\$|\))' % pattern
        try:
            '''去掉注释'''
            # fl = open(file_name, 'r', encoding='gbk')
            fl = open(file_name, 'r')
            ff = re.sub(r'\n+', '\n', re.sub(r'--\s*.*\n', '\n', fl.read()))
        except Exception as e:
            self.__err_logger.info('脚本文件读取失败,退出')
            sys.exit(e)
        for sql in ff.split(';')[:-1]:
            find_result = re.findall(pattern, sql, re.I)
            if find_result:
                for table in find_result:
                    self.__logger.info('dep:' + table)
                    lis.append((table.strip()).lower())
        fl.close()
        nl = set(lis)
        return nl

    def run(self, ):
        result = []
        if self.script:
            deps = self.get_dep(self.script)
            process_pool = []
            for dep in deps:
                process = threading.Thread(target=self.get_count, args=(dep,))
                process_pool.append(process)
            for p in process_pool:
                p.start()
            for p in process_pool:
                p.join()
            for i in range(len(process_pool)):
                result.append(self.queue.get())
            return result

        else:
            try:
                __connect_db = sqlEngine()
                __cursor = __connect_db.cursor
            except Exception as e:
                self.__err_logger.info('连接数据库失败,退出')
                sys.exit(e)
            self.get_count(self.table)
            result.append(self.queue.get())
        for i in result:
            self.__logger.info(i)
        return result


if __name__ == '__main__':
    try:

        table_name = sys.argv[1]
        date_str = sys.argv[2]
    except Exception as e:
        sys.exit('参数不正确')

    d = dataAnalyze(table_name, date_str)
    r = d.run()


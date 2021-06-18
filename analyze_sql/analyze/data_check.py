# -*- encoding:utf-8 -*-

"""
Author  : zhangrui
Contact : zhangrui2@mail01.huawei.com
Desc    : 查询一个脚本中的所有表的数据信息，或查询单个表的数据信息
Size: 11.55 kB
Type: Python
Modified: 2021/6/18 9:26
Created: 2021/6/12 9:33
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
        self.queue = queue.Queue()
        self.dateStr = str(date_str)
        self.result_file = './result_%s.txt' % datetime.date.today()
        self.__logger, self.__err_logger = self.__get_logger()
        self.history = self.__get_history()
        self.script, self.date_pattern = self.__get_some__args()

    def __get_some__args(self):
        if len(self.dateStr) == 8:
            vl_month = (datetime.datetime.strptime(self.dateStr[:6] + '01', '%Y%m01') - datetime.timedelta(
                days=1)).strftime('%Y%m')
            pattern = ('[a-zA-Z_]+=%s' % self.dateStr, '[a-zA-Z_]+=%s' % vl_month, '1=1')
        elif len(self.dateStr) == 6:
            weekday, monthdays = calendar.monthrange(int(self.dateStr[:4]), int(self.dateStr[4:6]))
            vi_month_last = self.dateStr[:6] + str(monthdays)
            pattern = ('[a-zA-Z_]+=%s' % self.dateStr, '[a-zA-Z_]+=%s' % vi_month_last, '1=1')
        else:
            self.__err_logger.error('日期输入不正确')
            sys.exit()
        if re.findall(r'\.sql', self.table):
            table = os.path.basename(self.table)
            script = self.script_root_dir + re.split('_', table, 1)[0] + '/' + table
            self.__logger.info(script)
            if os.path.isfile(script):
                self.__logger.info('脚本存在，将查询所有依赖')
            else:
                self.__logger.info('脚本不存在，请检查参数')
                sys.exit()
        else:
            script = False
        return script, pattern

    def __get_history(self):
        history = ''
        try:
            f = open(self.result_file, 'r')
            history = f.read()
            self.__logger.info('获取今天历史查询记录成功')
            f.close()
        except Exception:
            self.__err_logger.info('获取今天历史记录失败')

        return history

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
        formatter = logging.Formatter('%(asctime)s - [%(levelname)s]: %(message)s')
        logger.setLevel(logging.DEBUG)
        err_logger.setLevel(logging.DEBUG)
        input_file_name = os.path.basename('./log.log').split(".")[0]
        basedir = os.path.dirname('./log.log')
        log_path = os.path.join(basedir, input_file_name + ".log")
        err_path = os.path.join(basedir, input_file_name + ".err")
        _file_handler = logging.FileHandler(log_path)
        err_handler = logging.FileHandler(err_path)
        _file_handler.setLevel(level=logging.DEBUG)
        err_handler.setLevel(level=logging.DEBUG)
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

    def get_partitions(self, table_name='', __cursor=''):

        sql = 'show partitions %s' % table_name
        pattern = '$|'.join(self.date_pattern) + '$'
        self.__logger.debug('开始分析 %s的分区,查找 %s' % (table_name, pattern))
        __rows = self.__execute_sql(sql, __cursor)
        for row in __rows:
            p_date, = map(str, row)
            if re.findall(pattern, p_date):
                self.__logger.debug(' %s的分区:%s' % (table_name, p_date))
                return p_date
        return ''

    def get_hdfs_time(self, table_name='', p_date=''):
        ip = os.popen('hostname -i').read().strip()
        if ip == '133.95.9.92':
            hdfs_path = '/user/bdoc/7/services/hive'
        else:
            hdfs_path = '/user/bdoc/6289/hive'
        table_str1 = '/'.join(table_name.split('.'))

        if p_date == '1=1':
            p_date = ''
        hdfs_cmd = "hdfs dfs -ls %s/%s|grep '%s'|awk '{print $6,$7}'|sort -r|head -1" % (hdfs_path, table_str1, p_date)
        self.__logger.debug(hdfs_cmd)
        try:
            rs = os.popen(hdfs_cmd)
            time = rs.read().strip()
        # 报错后捕获不到异常
        except Exception as e:
            print('我捕获到异常了吗?')
            time = ''
        # 当获取不到hdfs文件时间时，可能是因为路径是大写，再尝试一下
        if time == '':
            table_str2 = table_name.split('.')[0] + '/' + table_name.split('.')[1].upper()
            hdfs_cmd2 = "hdfs dfs -ls %s/%s|grep '%s'|awk '{print $6,$7}'|sort -r|head -1" % (
                hdfs_path, table_str2, p_date)
            self.__logger.debug(' %s 执行失败，改为执行:\n%s' % (hdfs_cmd, hdfs_cmd2))
            try:
                rs = os.popen(hdfs_cmd2)
                time = rs.read().strip()
            except Exception as e:
                self.__err_logger.error('获取 %s 数据时间失败:%s' % (table_name, e))
                time = ''
        return time

    def get_count(self, table_name=''):

        # 从当天的结果文件中查找历史查询记录
        self.__logger.info('尝试从文件中获取记录：%s %s ' % (table_name, self.date_pattern))
        his_pattern = r'([0-9 :\-]+\s*\|%s\s*\|(%s)\s*\|\d+)' % (table_name, '|'.join(self.date_pattern))
        his = re.findall(his_pattern, self.history)  # his:[()]
        self.__logger.debug(his_pattern)
        if his:
            result = list(map(str.strip, his[0][0].split('|')))
            self.queue.put(result)
            self.__logger.info('%s 从历史记录中获取到了，就不重新查了' % table_name)
        else:
            self.__logger.info('%s 未获取到历史查询记录,将重新执行查询流程' % table_name)
            try:
                __connect_db = sqlEngine()
                __cursor = __connect_db.cursor
            except Exception as e:
                self.__err_logger.error('连接数据库失败,退出')
                sys.exit(e)

            partition_date = self.get_partitions(table_name, __cursor)
            if partition_date:
                # 获取表数据的hdfs数据时间
                time = self.get_hdfs_time(table_name, partition_date)
                sql = 'select count(*) from %s where %s ' % (table_name, partition_date)
                result = self.__execute_sql(sql, __cursor)  # list[tuple]
                count = str(result[0][0])
                # count, = result[0] 可读性低
                self.queue.put([time, table_name, partition_date, count])
            else:
                # 需要的分区不存在，数据为0
                self.__err_logger.error('%s 需要的分区[%s]不存在，数据为0' % (table_name, partition_date))
                self.queue.put(['', table_name, partition_date, '0'])

            try:
                __connect_db.close()
                self.__logger.debug('数据库连接已关闭')
            except Exception as e:
                self.__err_logger.info(e)
                self.__err_logger.error('数据库连接关闭失败')

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
            self.__err_logger.error('脚本文件读取失败,退出')
            sys.exit(e)
        for sql in ff.split(';')[:-1]:
            find_result = re.findall(pattern, sql, re.I)
            if find_result:
                for table in find_result:
                    lis.append((table.strip()).lower())
        fl.close()
        nl = list(set(lis))
        return nl

    def run(self, ):
        result = []
        if self.script:
            deps = self.get_dep(self.script)
            process_pool = []
            for dep in deps:
                self.__logger.info('table:' + dep)
                # 如果使用union all，需要增加表名字段， 会降低select count(*)效率，
                process = threading.Thread(target=self.get_count, args=(dep,))
                process_pool.append(process)
            for p in process_pool:
                p.start()
            for p in process_pool:
                p.join()
            for i in range(len(process_pool)):
                result.append(self.queue.get())
        else:

            self.get_count(self.table)
            result.append(self.queue.get())
        result.sort()

        # 格式化文件的准备
        d0 = max(list(map(len, list(map(lambda x: x[0], result)))))
        d1 = max(list(map(len, list(map(lambda x: x[1], result)))))
        d2 = max(list(map(len, list(map(lambda x: x[2], result)))))
        d3 = max(list(map(len, list(map(lambda x: x[3], result)))))
        fm = lambda x, y: format(x, '<%d' % y)
        fmt = lambda x: '|'.join([fm(x[0], d0), fm(x[1], d1), fm(x[2], d2), fm(x[3], d3)])
        print('\n' * 2 + '*' * 33)
        for i in result:
            i = fmt(i)
            with open(self.result_file, 'a') as f:
                f.write(i + '\n')
            self.__logger.info(i)
        print('*' * 33 + '\n' * 2)
        return result


if __name__ == '__main__':
    try:
        table = sys.argv[1]
        date_str = sys.argv[2]

        # 简单判断,触发异常
        str1, str2 = table.split('.')
    except Exception as e:
        sys.exit('参数不正确')

    d = dataAnalyze(table, date_str)
    r = d.run()

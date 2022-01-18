# -*- encoding:utf-8 -*-

"""
Author  : zhangrui
Contact : zhangrui2@mail01.huawei.com
Desc    : 查询一个脚本中的所有表的数据信息，或查询单个表的数据信息
Size: 12.29 kB
Type: Python
Modified: 2021/6/18 18:03
Created: 2021/6/12 9:33
"""
import sys

sys.path.append('/data/script/hbdb/py')

import re
import logging
import os
import threading
import calendar
import datetime
import queue


class dataAnalyze:
    def __init__(self, table=None):

        self.table = table
        self.script_root_dir = '/data/script/hbdb/'
        self.queue = queue.Queue()
        self.result_file = './hdfs_%s.txt' % datetime.date.today().strftime('%Y%m%d')
        self.__logger, self.__err_logger = self.__get_logger()

        self.script = self.__get_some__args()

    def __get_some__args(self):

        if re.findall(r'\.sql', self.table):
            table = os.path.basename(self.table)
            script = self.script_root_dir + re.split('_', table, 1)[0] + '/' + table
            if os.path.isfile(script):
                self.__logger.info('脚本存在，将查询所有依赖')
            else:
                self.__logger.info('正式脚本%s不存在,使用当前目录文件内容' % script)
                local_script = self.table
                if os.path.isfile(local_script):
                    script = local_script
                else:
                    self.__logger.info('当前目录也不存在%s，请检查参数' % local_script)
                    sys.exit()
        else:
            script = False
        return script

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

    def get_hdfs_time(self, table_name=''):
        ip = os.popen('hostname -i').read().strip()
        if ip == '133.95.9.92' or ip == '133.95.9.93':
            hdfs_path = '/user/bdoc/7/services/hive'
        else:
            hdfs_path = '/user/bdoc/6289/hive'
        table_str1 = '/'.join(table_name.split('.'))

        hdfs_cmd = """hdfs dfs -ls %s/%s|awk  '{print $6,$7,$8}'|awk -F '/' '{print $1"|"$NF}'|sort |tail -1 """ % (
            hdfs_path, table_str1)

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
            hdfs_cmd2 = """hdfs dfs -ls %s/%s|awk  '{print $6,$7,$8}'|awk -F '/' '{print $1"|"$NF}'|sort |tail -1 """ % (
                hdfs_path, table_str2)
            self.__logger.debug(' %s 执行失败，改为执行:\n%s' % (hdfs_cmd, hdfs_cmd2))
            try:
                rs = os.popen(hdfs_cmd2)
                time = rs.read().strip()
            except Exception as e:
                self.__err_logger.error('获取 %s 数据时间失败:%s' % (table_name, e))
                time = ''
        return time

    def print_fmt(self, data_list, start=0, end=0):
        """

        :param data_list: 传入的 数据列表 type:[[],()...]
        :param start: 打印的开始列
        :param end: 打印的结束列
        :return: 无返回值(可以返回格式化好的列表)
        """
        data = data_list

        # 局部命名空间
        __var = locals()

        # 存储自定义变量，及其序号
        var_ds = []

        if end == 0 or (bool(data) and end > len(data[0])):
            end = len(data[0])
        if start:
            # 将自然序号变为数据序号
            start = start - 1
        for i in range(start, end):
            __var['d' + str(i)] = max(list(map(len, list(map(lambda x: str(x[i]), data)))))
            var_ds.append((i, __var['d' + str(i)]))
        fm = lambda x: format(x[0], '<%d' % x[1])
        fmt = lambda x: '|'.join(map(fm, map(lambda x1: (x[x1[0]], x1[1]), var_ds)))

        print('\n' + '*' * 33)
        for index, i in enumerate(data):
            # 将数据格式化
            data[index] = fmt(i)
            self.__logger.info(data[index])
        print('*' * 33 + '\n')

    def get_hdfs_latest_time(self, table_name=''):

        time = self.get_hdfs_time(table_name)

        self.queue.put([table_name, time])

    def get_dep(self, file_name, pattern=r'mk\.|pub\.|dis\.|dw\.|dwh\.|am\.|det\.'):
        self.__logger.info('开始分析依赖表们')
        lis = []
        pattern = r'(?=%s)[a-zA-Z0-9_\.]*(?=|;|,|\s|_\$|\))' % pattern
        # pattern = r'(?=%s)[a-zA-Z0-9_\.]*_[a-zA-Z0-9]+(?=|;|,|\s|_\$|\))' % pattern
        try:
            '''去掉注释'''
            # fl = open(file_name, 'r', encoding='gbk')
            fl = open(file_name, 'r')
            ff = re.sub(r'\n+', '\n', re.sub(r'--.*', '\n', fl.read()))
        except Exception as e:
            self.__err_logger.error('脚本文件读取失败,退出')
            sys.exit(e)
        for sql in ff.split(';'):
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
                process = threading.Thread(target=self.get_hdfs_latest_time, args=(dep,))
                process_pool.append(process)
            for p in process_pool:
                p.start()
            for p in process_pool:
                p.join()
            for i in range(len(process_pool)):
                result.append(self.queue.get())
        else:
            self.get_hdfs_latest_time(self.table)
            result.append(self.queue.get())

        result.sort()

        # 格式化数据
        self.print_fmt(result)

        for i in result:
            with open(self.result_file, 'a') as f:
                f.write(i + '\n')

        return result


if __name__ == '__main__':
    try:
        table = sys.argv[1]

        # 简单判断,触发异常
        str1, str2 = table.rsplit('.', 1)
    except Exception as e:
        sys.exit('参数不正确,文件名需要以".sql"结尾;或者直接输入一个表名称')

    d = dataAnalyze(table)
    r = d.run()

# -*- encoding:utf-8 -*-
# print(string,end='') 是python3之后才可用
from __future__ import print_function
import logging, os
import time


def __get_logger():
    logger = logging.getLogger('info')
    err_logger = logging.getLogger('err')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    logger.setLevel(logging.DEBUG)
    err_logger.setLevel(logging.DEBUG)
    log_path = os.path.join('./', "hive_analyze" + ".log")
    err_path = os.path.join('./', "hive_analyze" + ".err")
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


def desc_tables(__logger, tables):
    desc_dict = {}
    __logger.info("获取所有表字段信息开始...")
    for table in tables:
        sql = 'desc %s' % table
        time.sleep(0.1)
        desc_dict.setdefault(table, '1')
        # 进度条
        amount = len(tables)
        done_count = len(desc_dict)
        done_line = int(done_count * 100 / amount)
        undone_line = 100 - done_line
        __logger.info(sql)
        print('\r%{0}:{1}{2}'.format(done_line, '●' * done_line, '⊙' * undone_line), end='')
    print('\n')
    __logger.info("获取获取所有表字段信息结束,共计%s个表" % len(desc_dict))
    return desc_dict


if __name__ == '__main__':
    l, el = __get_logger()

    desc_tables(l, range(100))

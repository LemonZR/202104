# -*- encoding:utf-8 -*-

"""
@Author  : zhangrui
@Contact : zhangrui2@mail01.huawei.com
@Time    : 2021/5/11
@Desc    : 查询Gbase 所有dis表的指标字段信息
"""

from GbaseSQLEngine import GbaseSQLEngine
import re
import logging
import os
import sys
from collections import Counter


class DisAnalyze:
    def __init__(self, file='./result.txt'):
        self.__file = file
        self.__logger, self.__err_logger = self.__get_logger()
        self.d_partition_columns = {
            # 如果一个表含有多个分区类型字段，优先取key值较小的
            'statis_date': 1,
            'deal_date': 2,
            'day_id': 3,
            'statis_hour': 4
        }
        self.m_partition_columns = {
            'statis_month': 5,
            'photo_month': 6

        }
        self.all_partitions = dict(Counter(self.d_partition_columns) + Counter(self.m_partition_columns))
        self.data_types = ('int', 'bigint', 'double', 'decimal')

        try:
            self.__gbase_db = GbaseSQLEngine()
            self.__cursor_gbase = self.__gbase_db.cursor
        except Exception as e:
            self.__err_logger.info('连接数据库失败,退出')
            sys.exit(e)

    def __get_logger(self, ):
        logger = logging.getLogger('info')
        err_logger = logging.getLogger('err')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        logger.setLevel(logging.DEBUG)
        err_logger.setLevel(logging.DEBUG)
        input_file_name = os.path.basename(self.__file).split(".")[0]
        basedir = os.path.dirname(self.__file)
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

    def __get_colum_info(self, ):
        cursor_gbase = self.__cursor_gbase

        # 分区字段不一定是数值类型，sql不要加额外的条件了s
        sql = """
            select TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,DATA_TYPE 
            from 
            information_schema.COLUMNS 
            where TABLE_SCHEMA='dis'
            """
        try:
            cursor_gbase.execute(sql)
            gbase_rows = cursor_gbase.fetchall()  # list[tuple]
            self.__logger.info('%s 查询成功', sql)
        except Exception as e:
            self.__err_logger.info('%s 查询失败： \n%s' % (sql, e))
            sys.exit()

        result_dict = {}
        partition_pattern = r'%s' % ('|'.join(list(self.all_partitions.keys())))
        type_pattern = r'%s' % ('|'.join(self.data_types))

        for row in gbase_rows:
            table_schema, table_name, column_name, data_type = map(str, row)
            whole_name = table_schema + '.' + table_name

            result_dict.setdefault(whole_name, {}).setdefault('column_name', [])

            is_partition_field = re.findall(partition_pattern, column_name, re.I)
            is_need_type = re.findall(type_pattern, data_type, re.I)
            if is_partition_field:
                partition_field = is_partition_field[0]
                result_dict[whole_name].setdefault('partition_fields', []).append(partition_field)
            elif is_need_type:
                result_dict[whole_name]['column_name'].append(column_name)

        return result_dict  # dict[str,dict[]]

    def __init_file_stream(self):
        result_file_name = os.path.basename(self.__file).split(".")[0]
        result_file_dir = os.path.dirname(self.__file)
        result_file = os.path.join(result_file_dir, result_file_name + "_result")
        self.r_f_D = open(result_file + '_d.txt', 'w')
        self.r_f_M = open(result_file + '_m.txt', 'w')
        self.r_f_N = open(result_file + '_n.txt', 'w')

    def __close(self):
        try:
            self.r_f_D.close()
        except Exception:
            self.__err_logger.info('file_D 未关闭')
        try:
            self.r_f_M.close()
        except Exception:
            self.__err_logger.info('file_M 未关闭')
        try:
            self.r_f_N.close()
        except Exception:
            self.__err_logger.info('file_N 未关闭')
        try:
            self.__gbase_db.close()
            self.__logger.info('数据库连接已关闭')
        except Exception as e:
            self.__err_logger.info(e)
            self.__err_logger.info('数据库连接关闭失败')

    def __write_result(self, result_dict):

        self.__init_file_stream()
        for table, info in result_dict.items():
            is_yyyymm = re.findall(r'20\d{4}$', table)
            if is_yyyymm:  # 判断是否月分表,并取202103之后的表
                if int(is_yyyymm[0]) < 202103:
                    self.__logger.info(table + ' 是202103之前的表,不列入配置项')
                    continue
            partition_field = min(info.get('partition_fields', ['S']),
                                  key=lambda partition: self.all_partitions.get(partition, 9))
            columns = info['column_name']
            if partition_field.lower() in self.d_partition_columns:
                if partition_field.lower() == 'statis_hour':
                    partition_field = 'substr(statis_hour,1,8)'
                p = 'D'

            elif partition_field.lower() in self.m_partition_columns:
                p = 'M'
            else:
                p = 'N'

            r_file = getattr(self, 'r_f_' + p)
            if columns:
                for column in columns:
                    line = '|'.join((table, partition_field, p, column))
                    r_file.write(line + '\n')
            else:
                line = '|'.join((table, partition_field, p, 'N'))
                r_file.write(line + '\n')
                # self.__logger.info(table + ' 没有指标')

    def run(self, ):
        result = self.__get_colum_info()
        self.__write_result(result)
        self.__close()


if __name__ == '__main__':
    try:
        file_path = sys.argv[1]
        print(file_path)
    except Exception as e:
        sys.exit('文件路径不正确')

    DisAnalyze(file_path).run()

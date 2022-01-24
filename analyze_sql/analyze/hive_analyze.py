# -*- encoding:utf-8 -*-
# print(string,end='') 是python3之后才可用
# from __future__ import print_function
from HiveSQLEngine import HiveSQLEngine
from GbaseSQLEngine import GbaseSQLEngine
import re
import logging
import os
import sys
import datetime

file_tail_date = (datetime.date.today() - datetime.timedelta(days=2)).strftime("%Y%m%d")
today_str = datetime.date.today().strftime("%Y%m%d")


class HiveAnalyze:
    def __init__(self, file_dir='输出文件目录'):
        self.__file_dir = file_dir
        self.__result_file = os.path.join(self.__file_dir, "hb_hive_tables_meta_%s.txt" % file_tail_date)

        self.__logger, self.__err_logger = self.__get_logger()

        try:
            self.__hive_db = HiveSQLEngine()
            self.__cursor_hive = self.__hive_db.cursor
        except Exception as e:
            self.__err_logger.info('连接hive数据库失败,退出')
            sys.exit(e)
        try:
            self.__gbase_db = GbaseSQLEngine()
            self.__cursor_gbase = self.__gbase_db.cursor
        except Exception as e:
            self.__err_logger.info('连接Gbase数据库失败,退出')
            sys.exit(e)
        try:
            self.__fh = open(self.__result_file, 'w+')
        except Exception as e:
            self.__err_logger.info(e)
            sys.exit("创建结果文件失败")

    def __get_logger(self, ):
        logger = logging.getLogger('info')
        err_logger = logging.getLogger('err')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        logger.setLevel(logging.DEBUG)
        err_logger.setLevel(logging.DEBUG)
        log_path = os.path.join(self.__file_dir, "hive_analyze" + ".log")
        err_path = os.path.join(self.__file_dir, "hive_analyze" + ".err")
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

    def run_sql(self, cursor, sql):

        try:
            cursor.execute(sql)
            # self.__logger.info('%s 执行成功' % sql)
        except Exception as e1:
            self.__logger.error('%s 执行失败:%s ' % (sql, e1))
            sys.exit(e1)

        try:
            data_rows = cursor.fetchall()  # list[tuple]
        except Exception as e2:
            self.__logger.info(sql + ' ' + str(e2))
            data_rows = [()]
        return data_rows

    def get_tables(self, schema):
        self.__logger.info("通过 show tables 获取所有表:")
        self.run_sql(self.__cursor_hive, "use {schema}".format(schema=schema))
        tables = self.run_sql(self.__cursor_hive, "show tables")  # [(u'str',)]
        tables = list(map(lambda x: schema + '.' + str(x[0]), tables))
        self.__logger.info("获取%s所有表成功,共计%s个表" % (schema, len(tables)))
        return tables  # [str,]

    def desc_tables(self, tables):
        desc_dict = {}
        self.__logger.info("获取所有表字段信息开始...")
        for table in tables:

            sql = 'desc %s' % table

            desc_result = self.run_sql(self.__cursor_hive, sql)
            __columns = []
            is_partition_filed = 0
            for tuple_info in desc_result:
                try:
                    field, type_str = map(str, tuple_info[:2])
                except Exception as e:
                    self.__err_logger.info('解释失败:%s' % table + ':' + desc_result + str(e))
                    field, type_str = '', ''
                if len(field.strip()) == 0 or re.findall('#', field):
                    is_partition_filed = 1
                    continue
                elif is_partition_filed:
                    try:
                        index = __columns.index((field, type_str, '0'))
                        __columns[index] = (field, type_str, '1')
                    except ValueError:
                        self.__err_logger.info('%s 分区字段分析错误' % table)
                else:
                    __columns.append((field, type_str, '0'))

            desc_dict.setdefault(table, __columns)

            amount = len(tables)
            done_count = len(desc_dict)
            done_line = int(done_count * 100 / amount)
            self.__logger.info("%s--%%%d" % (done_line, sql))
            # 进度条
            # undone_line = 100 - done_line
            # 打印内容吧，不打印动态进度条了
            # print('\r%{0}:{1}{2}'.format(done_line, '●' * done_line, '⊙' * undone_line), end='')

        self.__logger.info("获取获取所有表字段信息结束,共计%s个表" % len(desc_dict))
        return desc_dict

    def export_to_gbase(self, schema, table_info):

        create_table = """create table if not exists pub.tdet_hive_tablemeta(
        table_name varchar(100),col_name varchar(100),data_type varchar(100),is_partition_field varchar(1)
        ,sch varchar(10),date_str varchar(8)
        )ENGINE=EXPRESS DISTRIBUTED BY('col_name') DEFAULT CHARSET=utf8 TABLESPACE='sys_tablespace' 
        """
        delete_old = """delete from  pub.tdet_hive_tablemeta where sch ='%s' and date_str ='%s'""" % (schema, today_str)
        self.run_sql(self.__cursor_gbase, create_table)
        self.run_sql(self.__cursor_gbase, delete_old)
        values = []
        for table_name, info in table_info.items():
            for col_name, data_type, is_partition in info:
                values.append(
                    """('%s','%s','%s','%s','%s','%s')""" % (
                        table_name, col_name, data_type, is_partition, schema, today_str))
        insert_sql = """insert into pub.tdet_hive_tablemeta values %s """ % ','.join(values)
        self.run_sql(self.__cursor_gbase, insert_sql)
        print(len(values))

    def export_to_file(self, schema, table_info):
        self.__logger.info("将 %s 结果写入文件开始" % schema)
        values = []
        for table_name, info in table_info.items():
            for col_name, data_type, is_partition_field in info:
                values.append('|'.join((table_name, col_name, data_type, is_partition_field, schema, today_str)) + '\n')
        self.__fh.writelines(values)
        self.__logger.info("此次写入 %s 行" % len(values))
        self.__logger.info("将 %s 结果写入文件结束" % schema)

    def __close(self):
        try:
            self.__hive_db.close()
            self.__gbase_db.close()
            self.__fh.close()
            self.__logger.info('数据库/文件连接已关闭')
        except Exception as e:
            self.__err_logger.info(e)
            self.__err_logger.info('数据库/文件连接关闭失败')

    def run(self, schemas):
        for schema in schemas:
            tables = self.get_tables(schema)
            table_info = self.desc_tables(tables)
            self.export_to_file(schema, table_info)
            # self.export_to_gbase(schema,table_info)
        self.__close()


if __name__ == '__main__':
    try:
        result_file_dir = sys.argv[1]
        print(result_file_dir)
    except Exception as e:
        sys.exit('文件路径不正确')
    ha = HiveAnalyze(file_dir=result_file_dir)
    schemas = ['am', 'mk', 'dwh', 'pub']
    ha.run(schemas)

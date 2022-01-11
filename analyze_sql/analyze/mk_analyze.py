# -*- encoding:utf-8 -*-

from HiveSQLEngine import HiveSQLEngine
from GbaseSQLEngine import GbaseSQLEngine
import re
import logging
import os
import sys


class HiveAnalyze:
    def __init__(self, file_dir='输出文件目录'):
        self.__file_dir = file_dir
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
            self.__logger.info('%s 执行成功' % sql)
        except Exception as e1:
            self.__logger.error('%s 执行失败:%s ' % (sql, e1))

        try:
            data_rows = cursor.fetchall()  # list[tuple]
        except Exception as e2:
            self.__logger.info(sql + str(e2))
            data_rows = [()]
        return data_rows

    def get_tables(self, schema):
        self.run_sql(self.__cursor_hive, "use {schema}".format(schema=schema))
        tables = self.run_sql(self.__cursor_hive, "show tables")  # [(u'str',)]
        tables = list(map(lambda x: schema + '.' + str(x[0]), tables))
        return tables  # [str,]

    def desc_tables(self, cursor, tables):
        desc_dict = {}
        for table in tables:
            sql = 'desc %s' % table
            desc_result = self.run_sql(cursor,sql)

            for tuple_info in desc_result:
                try:
                    field, type_str = map(str, tuple_info[:2])
                except Exception as e:
                    self.__err_logger.error('解释失败:%s' % table + ':' + desc_result +str(e))
                    field, type_str = '', ''
                if len(field.strip()) == 0 or re.findall('#', field):
                    continue
                else:
                    desc_dict.setdefault(table, set()).add((field, type_str))
        return desc_dict

    def export_result_to_gbase(self,table_info):
        create_table ="""create table if not exists pub.tdet_hive_tablemeta(
        tablename varchar(100),col_name varchar(100),data_type varchar(100)
        )ENGINE=EXPRESS DISTRIBUTED BY('col_name') DEFAULT CHARSET=utf8 TABLESPACE='sys_tablespace' 
        """

        
                      



    def __close(self):
        try:
            self.__hive_db.close()
            self.__gbase_db.close()
            self.__logger.info('数据库连接已关闭')
        except Exception as e:
            self.__err_logger.info(e)
            self.__err_logger.info('数据库连接关闭失败')


    def __write_result(self, tmp_result):
        result_file_name = os.path.basename(self.__file).split(".")[0]
        result_file_dir = os.path.dirname(self.__file)
        result_file = os.path.join(result_file_dir, result_file_name + "_result")
        result_dict = {}

        for t_name, info in tmp_result.items():
            result_dict.setdefault(t_name, {}).setdefault('field', [])
            for field_info in info:
                try:
                    field, typ = map(str, field_info[:2])
                except Exception:
                    self.__err_logger.info('%s 解释失败' % t_name + ':' + field_info)
                    field, typ = '', ''
                if len(field.strip()) == 0 or re.findall('#', field):
                    print("不要这个" + field)
                    continue


    def run(self, ):
        tmp_result = self.__get_colum_info(self.__file)
        self.__write_result(tmp_result, self.__file)
        self.__close()


if __name__ == '__main__':
    try:
        result_file_dir = sys.argv[1]
        print(result_file_dir)
    except Exception as e:
        sys.exit('文件路径不正确')

    HiveAnalyze(file_dir=result_file_dir).get_tables('am')

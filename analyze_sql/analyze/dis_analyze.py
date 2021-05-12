# -*- encoding:utf-8 -*-

from GbaseSQLEngine import GbaseSQLEngine
import re
import logging
import os, sys


class DisAnalyze:
    def __init__(self, file=''):
        self.__file = file
        self.__logger, self.__err_logger = self.__get_logger()
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

    def __get_colum_info(self, file):
        cursor_gbase = self.__cursor_gbase
        tmp_result = {}
        f = open(file, 'r')
        for table_name in f:
            table_name = table_name.strip()
            if len(table_name) == 0:
                self.__err_logger.info('这有一个空行')
                continue
            sql = 'desc %s ;' % table_name
            try:
                cursor_gbase.execute(sql)
                gbase_rows = cursor_gbase.fetchall()  # list[tuple]
                self.__logger.info('%s 读取成功' % table_name)
            except Exception as e:
                self.__err_logger.info('%s 读取失败：%s \n%s' % (table_name, sql, e))
                continue
            tmp_result.setdefault(table_name, gbase_rows)
        return tmp_result  # dict[str,list[tuple]]

    def __init_file_stream(self):
        result_file = self.__file.split('.')[0] + '_result'
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

    def __write_result(self, tmp_result, file):

        partition_pattern = r'statis_date|statis_month'
        type_pattern = r'int|double|decimal'
        result_dict = {}
        for t_name, info in tmp_result.items():
            result_dict.setdefault(t_name, {}).setdefault('field', [])
            for field_info in info:
                try:
                    field, typ = map(str, field_info[:2])
                except Exception:
                    self.__err_logger.info('%s 解释失败' % t_name + ':' + field_info)
                    field, typ = '', ''

                is_partition_field = re.findall(partition_pattern, field, re.I)

                if is_partition_field:
                    partition_field = is_partition_field[0]
                    result_dict[t_name].setdefault('partition_field', partition_field)

                is_need_type = re.findall(type_pattern, typ, re.I)
                if is_need_type and not is_partition_field:
                    result_dict[t_name]['field'].append(field)
        self.__init_file_stream()
        for table, info in result_dict.items():
            partition_field = info.get('partition_field', 'N')
            fields = info['field']
            if partition_field.lower() == 'statis_date':
                p = 'D'

            elif partition_field.lower() == 'statis_month':
                p = 'M'
            else:
                p = 'N'
            r_file = getattr(self, 'r_f_' + p)
            if fields:
                for field in fields:
                    line = '|'.join((table, partition_field, p, field))
                    r_file.write(line + '\n')
            else:
                line = '|'.join((table, partition_field, p, 'N'))
                r_file.write(line + '\n')
                self.__logger.info(table + '没有指标')

    def run(self, ):
        tmp_result = self.__get_colum_info(self.__file)
        self.__write_result(tmp_result, self.__file)
        self.__close()


if __name__ == '__main__':
    try:
        file_path = sys.argv[1]
        print(file_path)
    except Exception as e:
        sys.exit('文件路径不正确')

    DisAnalyze(file_path).run()

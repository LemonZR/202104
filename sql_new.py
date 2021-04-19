# coding=utf-8

import re


def read_file(file_name, date_num, target_table):
    sql_list = []
    with open(file_name, 'r') as f:
        for line in f:
            table_name, date_str = re.split('[<>=]|\s+', line.strip())[:2]
            if date_str == 'full':
                sql = 'insert into hw_tmp.select_record_zhangrui select \'{1}\' ,\'{0}\',\'{2}\',count(*),\'{3}\' from {1} ;'.format(
                    date_str, table_name, date_num, target_table)
                sql_list.append(sql)
                continue
            sql = 'insert into hw_tmp.select_record_zhangrui select \'{1}\' ,\'{0}\',\'{2}\',count(*),\'{3}\' from {1} where {0} ' \
                  '=20201101  ;'.format(date_str, table_name, date_num, target_table)
            sql_list.append(sql)
    return sql_list


def sql_generator(sql_list, file_name):
    result_file_name = file_name.split('.')[0] + 'new.txt'
    result_file = open(result_file_name, 'w')
    # sql_list=[]
    for sql in sql_list:
        result_file.write(sql + '\n')


if __name__ == '__main__':
    date_num = 20201101
    target_table = 'mk.tm_gc_cluster_broaband_d'

    file_name = 'tmp'
    lists = read_file(file_name, date_num, target_table)
    sql_generator(lists, file_name)

# coding=utf-8

import os
import re
import shutil
import sys
import datetime
from tools import excelOp


def ergodic_dirs(root_dir='D:\\sql_gen\\bd_hive') -> list[str]:
    file_list = []
    for root, dirs, files in os.walk(root_dir):

        for file in files:
            file = os.path.join(root, file)
            file_list.append(file)

    return file_list


def get_sql_dict(file) -> dict[int, str]:
    try:
        '''去掉注释'''
        fl = open(file, 'r', encoding='gbk')
        ff = re.sub(r'\n+', '\n', re.sub(r'--.*', '\n', fl.read()))
    except Exception as e1:
        try:
            fl = open(file, 'r', encoding='utf-8')
            ff = re.sub(r'\n+', '\n', re.sub(r'--.*', '\n', fl.read()))
        except Exception as e2:
            print(f'{file}' + str(e1) + '\n' + str(e2))
            ff = ''
    '''以';'为分隔拆分,忽略最后一个空语句块'''
    sql_blks = ff.split(';')[:-1]

    sql_dict = {}

    for sql_blk_id, sql_blk in enumerate(sql_blks):
        new_sql_blk = re.sub(r'\s*', ' ', sql_blk)
        sql_dict.setdefault(sql_blk_id, new_sql_blk)
    return sql_dict


def get_files_sql_dict(dir_name='D:\\tmp\\') -> dict[str, dict]:
    file_list = ergodic_dirs(dir_name)
    file_sql_dict = {}
    for file in file_list:
        fil_name = os.path.basename(file)
        sql_info = get_sql_dict(file)
        file_sql_dict[fil_name] = sql_info
    return file_sql_dict


def compare_file(jt_dir_name='', prov_dir_name=''):
    jt_files_sql_dict = get_files_sql_dict(jt_dir_name)
    prov_files_sql_dict = get_files_sql_dict(prov_dir_name)
    jt_files = set(jt_files_sql_dict.keys())
    prov_files = set(prov_files_sql_dict.keys())
    # prov_miss = jt_files.difference(prov_files)
    jt_miss = prov_files.difference(jt_files)
    # jt_and_prov = jt_files.intersection(prov_files)
    prov_miss = [('省侧没有的',)]
    jt_miss = [('集团没有的',)] + [tuple(jt_miss)]
    differences = [('脚本名', '集团sql', '省sql')]
    same = [('脚本一致',)]
    for file_name, sql_dict in jt_files_sql_dict.items():
        pro_file_sql_dict = prov_files_sql_dict.get(file_name, None)
        if not pro_file_sql_dict:
            prov_miss.append((file_name,))
            continue
        for sql_id, sql in sql_dict.items():
            prov_sql = pro_file_sql_dict.get(sql_id)
            if sql != prov_sql:
                differences.append((file_name, sql, prov_sql))
            else:
                same.append((file_name,))
    return differences, same, jt_miss, prov_miss


if __name__ == '__main__':
    filename = 'D:\\compare_jt_prov.xlsx'
    jt_dir = 'D:\\tmp\\compare'
    prov_dir = 'D:\\tmp\\compare'
    diff, sam, jt_mis, prov_mis = compare_file(jt_dir, prov_dir)
    data = [('脚本不同', diff), ('脚本一致', sam), ('集团没有的脚本', jt_mis), ('省没有的脚本', prov_mis)]
    excelOp.write_many_sheets_xlsx(filename, data, edit=True)

# coding=utf-8

import re
import os
from tools import excelOp


def ergodic_dirs(root_dir='D:\\sql_gen\\bd_hive') -> list[str]:
    file_list = []
    for root, dirs, files in os.walk(root_dir):

        for file in files:
            file = os.path.join(root, file)
            file_list.append(file)

    return file_list


def get_sql_blks(file) -> list[str]:
    try:
        fl = open(file, 'r', encoding='gbk')
        ff = fl.read()
    except Exception as e1:
        try:
            fl = open(file, 'r', encoding='utf-8')
            ff = fl.read()
        except Exception as e2:
            print(f'{file}' + str(e1) + '\n' + str(e2))
            ff = ''

    '''以';'为分隔拆分,忽略最后一个空语句块'''
    sql_blks = ff.split(';')
    return sql_blks


def alter_files_sql_blks(dir_name='D:\\tmp\\', find_pattern='', old_pattern='', new_str='', pattern_mod=re.IGNORECASE):
    file_list = ergodic_dirs(dir_name)
    file_sql_blks_dict = {}
    alter_info = [('脚本名', '需要替换的语句', '替换后的语句')]
    for file in file_list:
        fil_name = os.path.basename(file)
        sql_blks = get_sql_blks(file)
        for sql_blk_id, sql_blk in enumerate(sql_blks):
            new_sql = re.sub(r'--\s*.*\n', '\n', sql_blk)
            if re.findall(find_pattern, new_sql, flags=pattern_mod):
                if re.findall(old_pattern, new_sql, flags=pattern_mod):
                    new_sql_blk = re.sub(old_pattern, new_str, sql_blk, flags=pattern_mod)
                    alter_info.append((fil_name, sql_blks[sql_blk_id], new_sql_blk))
                    sql_blks[sql_blk_id] = new_sql_blk
                    file_sql_blks_dict[fil_name] = sql_blks

    return file_sql_blks_dict, alter_info


def write_new_file(file_dict: dict[str, list], alter_info: list[tuple], dirname='D:\\tmp\\alter', ):
    for file_name, sql_blks in file_dict.items():
        with open(f'{dirname}\\{file_name}', 'w', encoding='utf-8') as f:
            # for sql_blk in sql_blks:
            #     f.write(sql_blk + ';')
            f.write(';'.join(sql_blks))
    record_file = f'{dirname}\\alter_record.xlsx'
    excelOp.write_xlsx(record_file, alter_info, edit=True)


if __name__ == '__main__':
    dirName = 'D:\\tmp\\dis\\'

    tmp_pattern = r'create\s*temporary'
    old_p = r'tablespace\s*=\s*\'[a-zA-Z_]*\''
    new_string = ''
    alter_files_dict, alter_infos = alter_files_sql_blks(dir_name=dirName, find_pattern=tmp_pattern, old_pattern=old_p,
                                                         new_str=new_string)
    write_new_file(alter_files_dict, alter_infos)

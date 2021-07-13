# coding=utf-8

import os
import re
import shutil
import sys
import datetime

sep = os.sep


def ergodic_dirs(root_dir='D:\\sql_gen\\bd_hive'):
    file_list = []
    for root, dirs, files in os.walk(root_dir):

        for file in files:
            file = os.path.join(root, file)
            file_list.append(file)

    return file_list


def get_sql_blks(file):
    try:
        fl = open(file, 'r')
        ff = fl.read()
    except Exception as e1:
        print(file + ':' + str(e1))
        ff = ''

    '''以';'为分隔拆分,不忽略最后一个空语句块'''
    sql_blks = ff.split(';')
    return sql_blks


def alter_files_sql_blks(dir_name='D:\\tmp\\', find_pattern='', old_pattern='', new_str='', pattern_mod=re.IGNORECASE):
    file_list = ergodic_dirs(dir_name)
    file_sql_blks_dict = {}

    for file in file_list:
        fil_name = os.path.basename(file)
        sql_blks = get_sql_blks(file)
        for sql_blk_id, sql_blk in enumerate(sql_blks):
            new_sql = re.sub(r'--.*', '\n', sql_blk)
            if re.findall(find_pattern, new_sql, flags=pattern_mod):
                if re.findall(old_pattern, new_sql, flags=pattern_mod):
                    new_sql_blk = re.sub(old_pattern, new_str, sql_blk, flags=pattern_mod)

                    sql_blks[sql_blk_id] = new_sql_blk
                    file_sql_blks_dict[fil_name] = sql_blks

    return file_sql_blks_dict


def backup_file(file_dict, dirname_origion='D:\\tmp\\dis', dirname_bak='D:\\tmp\\alter_bak'):
    if not os.path.exists(dirname_origion):
        sys.exit('原目录不存在')
    if not os.path.exists(dirname_bak):
        try:
            os.makedirs(dirname_bak)
        except Exception as e:
            sys.exit(e)
    for file_name in file_dict:
        shutil.copy2(dirname_origion + sep + file_name, dirname_bak)


def write_new_file(file_dict, dirname='D:\\tmp\\alter', ):
    if not os.path.exists(dirname):
        try:
            os.makedirs(dirname)
        except Exception as e:
            sys.exit(e)
    print(len(file_dict))
    for file_name, sql_blks in file_dict.items():
        with open(dirname + sep + file_name, 'w') as f:
            f.write(';'.join(sql_blks))


if __name__ == '__main__':
    time_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    # 原脚本目录
    dirName = '/data/script/hbdb/dis'

    # 备份脚本目录
    dirname_bak = '/data/script/hbdb/work/lixiaolong/dis_bak' + time_str

    # 修改后的脚本目录
    dirname_alter_files = '/data/script/hbdb/work/lixiaolong/dis_new' + time_str

    print(dirname_bak)

    # 查找的东西
    find_pattern = r'create\s*temporary'
    old_p = r'tablespace\s*=\s*\'[a-zA-Z_]*\''

    new_string = ''
    alter_files_dict = alter_files_sql_blks(dir_name=dirName, find_pattern=find_pattern, old_pattern=old_p,
                                            new_str=new_string)
    backup_file(alter_files_dict, dirname_origion=dirName, dirname_bak=dirname_bak)
    write_new_file(alter_files_dict, dirname=dirname_alter_files)

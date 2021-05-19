# -*- encoding:utf-8 -*-

"""
@Author  : chenxingcong
@Contact : chenxingcong@mail01.huawei.com
@File    : dis_pc_mt_sqlparse_first_dp_d.py
@Time    : 2021/2/23 10:57
@Desc    :
"""

import re
import glob
import os
import chardet
import cx_Oracle
import datetime
from pylogger import pylogger

dwh = re.compile(r'(dwh\.\w+)')
mk = re.compile(r'(mk\.\w+)')
dis = re.compile(r'(dis\.\w+)')
pub = re.compile(r'(pub\.\w+)')
rex_num = re.compile('\_\d*_*$')
# 'tm_sc_user_fuse_d_pre2_2' 'tm_sc_broaband_out_d_pre_4'
# rex_pre= re.compile('(\_pre\d*)*$')
# rex_pre= re.compile('\_pre\d\_*\d*$')
rex_pre = re.compile('\_pre\d*\_*\d*$')
rex_per = re.compile('\_per\d*\_*\d*$')
tb_pre = re.compile('\.t')


# 遍历文件夹,返回文件夹列表
def get_dirs(path):
    dirs = []
    dirs.append(path)
    for dirpath, dirnames, filenames in os.walk(path):
        for dirname in dirnames:
            dirs.append(os.path.join(dirpath, dirname))
            # print(os.path.join(dirpath, dirname))
    return dirs


def getEncoding(testfile):
    with open(testfile, 'rb') as file:
        encoding = chardet.detect(file.read())
    return encoding['encoding']


# 解析脚本
def get_src_schema_table(sqlscript):
    '''
    返回脚本模式与脚本名(mk,tm_sc_user_busi_use_m)
    :param sqlscript: mk_pm_sc_user_busi_use_m.sql
    :return: (mk,tm_sc_user_busi_use_m)
    '''
    sqlscriptname = sqlscript.replace('.sql', '')
    src_table_list = sqlscriptname.split('_')

    schemaname = src_table_list[0]
    tablenamestr = '_'.join(src_table_list[1:])
    tablenamelist = list(tablenamestr)
    tablenamelist[0] = 't'
    tablename = ''
    for i in tablenamelist:
        tablename += i
    return (sqlscriptname, schemaname, tablename)


# 获取脚本依赖表
def get_tablenames(sqlscript):
    # 依赖表容器
    tables = []
    encode = getEncoding(sqlscript)
    # 读取sql文件
    # with open(sqlscript, 'r', encoding=encode) as sqlfile:
    with open(sqlscript, 'r') as sqlfile:
        # with open(sqlscript, 'r', encoding='gb2312') as sqlfile:
        lines = sqlfile.readlines()
        # sql脚本格式化；去掉注释’--‘，去掉换行，空格替换制表符，脚本放入一行
        sql = ''
        for line in lines:
            line = line.lower()
            if '--' in line:
                line = line[:line.index('--')]
            sql += line.replace('\r', ' ').replace('\n', ' ').replace('\t', ' ') + ' '
        # 整行sql格式化：以’;‘分割，
        rows = sql.split(';')
        for row in rows:
            # 获取关键字'from'的代码段
            # if 'from' in row and 'delete' not in row:---------------------------------------------------------------
            if 'from' in row:
                # 获取第一个'from'之后的代码段
                keys = row[row.index('from'):]
                # 正则获取对应依赖表
                results = dwh.findall(keys) + mk.findall(keys) + dis.findall(keys) + pub.findall(keys)
                for result in results:
                    # print(result)
                    # print(rex_num.sub('', result))
                    tables.append((rex_num.sub('', result), row))
    return tables


# 写入文件
def extracttable(root_path):
    args_period = datetime.datetime.now().strftime('%Y%m%d')
    # 遍历文件夹，获取包含根目录在内的所有目录
    dirs = get_dirs(root_path)
    # os.remove('d:\\bd_hive.txt')
    encodeERROR = []
    for dir in dirs:
        # 切换到当前目录
        os.chdir(dir)
        # 获取所有sql脚本
        sqlscripts = glob.glob('*.sql')
        for sqlscript in sqlscripts:
            errorCode = getEncoding(sqlscript)
            if errorCode != 'utf-8':
                encodeERROR.append({sqlscript: errorCode})
            # print(sqlscript)
            rows = []
            # 处理脚本名称
            sqlscriptname, src_schema, src_tablename = get_src_schema_table(sqlscript)
            src_table = rex_pre.sub('', src_tablename)
            src_table = rex_per.sub('', src_table)
            src_tablenames = src_schema + '.' + src_table
            # 获取sql脚本依赖表
            results = get_tablenames(sqlscript)
            # 写入文件中去
            for result in results:
                tables, sql = result
                depend_schema, depended_table = tables.split('.')
                if src_table != depended_table:
                    row = [sqlscript, src_schema, src_tablenames, depend_schema, tables, sql, args_period]
                    rows.append(row)
            for row in rows:
                sqlfile = """insert into dis.tc_mt_sqlparse_first_dp_d (script_type,script_name,target_schema,target_tablename,depended_schema,depended_tablename,code_block,statis_date) values('模型脚本',:s,:s,:s,:s,:s,:s,:s)"""
                cursor.execute(sqlfile, row)
                db.commit()

    print(encodeERROR)


if __name__ == '__main__':
    file_paths = ['/data/script/hbdb/mk', '/data/script/hbdb/dwh', '/data/script/hbdb/dis', '/data/script/hbdb/pub', '/data/script/hbdb/am']
    logPath = "/data/script/hbdb/py_log/dis_pc_mt_sqlparse_first_dp_d.log"
    logger = pylogger(logPath)
    db = cx_Oracle.connect('dis/DwCh_0514@bdportal_9_36')
    cursor = db.cursor()
    logger.info("连接数据库成功")
    sql_delete = 'truncate table dis.tc_mt_sqlparse_first_dp_d'
    cursor.execute(sql_delete)
    db.commit()
    logger.info("清理完历史数据")
    for file_path in file_paths:
        print(file_path)
        extracttable(file_path)

    db.commit()
    cursor.close()
    db.close()
    logger.info("脚本执行完成")
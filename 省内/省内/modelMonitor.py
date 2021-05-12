# -*- encoding:utf-8 -*-

"""
@Author  : chenxingcong
@Contact : chenxingcong@mail01.huawei.com
@File    : modelMonitor.py
@Time    : 2021/2/23 10:57
@Desc    : 
"""

from datetime import datetime
from HiveSQLEngine import HiveSQLEngine
import logging
import os
import sys
import math


def initConfig(configFile, args_period):
    # 转化为字典
    dictConfig = []
    with open(configFile, 'r') as config:
        title = ['table_name', 'period_col', 'period', 'column']
        lines = config.readlines()
        for l in lines:
            line = l.replace('\r', '').replace('\n', '')
            i = line.split("|")
            dictTable = dict(zip(title, i))
            if len(args_period) == 6 and dictTable['period'] == 'M':
                dictConfig.append(dictTable)
            elif len(args_period) == 8 and dictTable['period'] == 'D':
                dictConfig.append(dictTable)

            else:
                print(str(line) + '：该行不符合规范，请检查配置文件')
                sys.exit()

    # print(dictConfig)
    # return dictConfig

    if len(dictConfig) == 0:
        print("配置文件格式不正确")
        sys.exit()

    configColumns = {}
    for i in dictConfig:
        table_name = i['table_name']
        period_col = i['period_col']
        period = i['period']
        configColumns.setdefault(table_name + period_col + period, {
            'table_name': table_name,
            'period_col': period_col,
            'period': period,
            'column': []
        })['column'].append(i['column'])
    return list(configColumns.values())


def initSql(dictCofnig, args_period):
    column = dictCofnig['column']
    sumColumns = []
    for col in column:
        sumColumn = "'%s',':',nvl(sum(cast(%s as decimal(38,0))),0)" % (col, col)
        sumColumns.append(sumColumn)
    summer = ",'|',".join(sumColumns)
    if 'N' in column:
        sumSql = "select '%s','T',count(1) from %s where %s=%s" % (dictCofnig['table_name'], dictCofnig['table_name'], dictCofnig['period_col'], args_period)
    else:
        sumSql = "select '%s',concat(%s),count(1) from %s where %s=%s" % (dictCofnig['table_name'], summer, dictCofnig['table_name'], dictCofnig['period_col'], args_period)
    # print(sumSql)
    return sumSql


def partition(lst, step):
    # division = len(lst) / float(n)
    # return [lst[int(round(division * i)): int(round(division * (i + 1)))] for i in range(n)]

    division = int(math.ceil(float(len(lst)) / step))
    return [lst[i * step:i * step + step] for i in range(division)]


def unionAllSqls(dictCofnigs, args_period, step):
    unionSqls = []
    for dictCofnig in dictCofnigs:
        sql = initSql(dictCofnig, args_period)
        unionSqls.append(sql)
    unionSql = []
    for i in partition(unionSqls, step):
        unionall = ' union all '.join(i)
        unionSql.append(unionall)
    return unionSql


def mainFunc(args):
    # configFileName = "D:\pythonfile\mk.txt"
    # args_period = '202101'
    # configFileName = "/home/chenxc/pythonfile/mk.txt"
    # monitorResult = "/home/chenxc/pythonfile/monitorResult.txt"
    step = 30

    if len(args[0]) == 6 or len(args[0]) == 8:
        args_period = args[0]
    else:
        print("周期不正确")
        sys.exit(2)
    if os.path.isfile(args[1]) and os.path.isfile(args[1]):
        configFileName = args[1]
        monitorResult = args[2]
    else:
        print("配置文件/结果文件不正确")
        sys.exit()

    logger = logging.getLogger()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    logger.setLevel(logging.DEBUG)

    input_file_name = os.path.basename(monitorResult).split(".")[0]
    basedir = os.path.dirname(monitorResult)
    log_path = os.path.join(basedir, input_file_name + ".log")
    errorModel = os.path.join(basedir, input_file_name + ".error")

    _file_handler = logging.FileHandler(log_path)
    _file_handler.setLevel(level=logging.INFO)
    _file_handler.setFormatter(formatter)
    logger.addHandler(_file_handler)
    # 添加日志控制台输出
    _console_handler = logging.StreamHandler()
    _console_handler.setLevel(level=logging.INFO)
    _console_handler.setFormatter(formatter)
    logger.addHandler(_console_handler)

    start_time = datetime.now()
    fw = open(monitorResult, 'w')
    ferror = open(errorModel, 'w')
    db = HiveSQLEngine()
    cursor = db.cursor

    dictCofnigs = initConfig(configFileName, args_period)
    unionSqls = unionAllSqls(dictCofnigs, args_period, step)
    for sql in unionSqls:
        func_start_time = datetime.now()
        try:
            logger.info(sql)
            cursor.execute(sql)
            rows = cursor.fetchall()
        except:
            logger.exception('%s执行异常，请检查sql' % str(sql))
            ferror.write(str(sql))
            ferror.write('\n')
            ferror.flush()
            break
        for row in rows:
            # print(row)
            # (u'dwh.td_ac_adjustbill_log_m', u'region:406559272|billcycle:260866716174|oldcycle:260866716174',1290774)
            # (u'dwh.td_sc_subs_m', u'T', 334605675)
            try:
                if 'T' in row:
                    tableTitle = ['table_name', 'total']
                    tableData = [row[0], row[2]]
                    dictResult = dict(zip(tableTitle, tableData))
                    # mk.tm_ac_balance_d | st20210201 | 5000 | acct_balance | 30000
                    result = dictResult['table_name'] + '|' + args_period + '|' + str(dictResult['total']) + '|total|' + str(dictResult['total'])
                    # print(result)
                    fw.write(result)
                    fw.write('\n')
                    fw.flush()
                else:
                    # (u'dwh.td_ac_adjustbill_log_m', u'region:406559272|billcycle:260866716174|oldcycle:260866716174',1290774)
                    tableName = row[0]
                    tableData = row[1]
                    tableRownum = row[2]
                    if 'None' in tableData:
                        pass
                    else:
                        columnDatas = str(tableData).split('|')
                        for i in columnDatas:
                            columnData = str(i).split(':')
                            column = columnData[0]
                            rownum = columnData[1]
                            # mk.tm_ac_balance_d | st20210201 | 5000 | acct_balance | 30000
                            result = tableName + '|' + args_period + '|' + str(tableRownum) + '|' + column + '|' + str(rownum)
                            # print(result)
                            fw.write(result)
                            fw.write('\n')
                            fw.flush()
            except:
                logger.exception('%s数据处理异常' % str(row))
                ferror.write(str(row))
                ferror.write('\n')
                ferror.flush()

        func_end_time = datetime.now()
        func_duration_seconds = (func_end_time - func_start_time).seconds % 60
        func_duration_munites = (func_end_time - func_start_time).seconds / 60 % 60
        func_duration_hours = (func_end_time - func_start_time).seconds / 60 / 60
        print("func_Duration    : " + str(func_duration_hours) + ":" + str(func_duration_munites) + ":" + str(func_duration_seconds))

    db.close()
    fw.close()
    ferror.close()
    end_time = datetime.now()
    duration_seconds = (end_time - start_time).seconds % 60
    duration_munites = (end_time - start_time).seconds / 60 % 60
    duration_hours = (end_time - start_time).seconds / 60 / 60
    # print("Duration    : " + str(duration_hours) + ":" + str(duration_munites) + ":" + str(duration_seconds))
    logger.info("Duration    : " + str(duration_hours) + ":" + str(duration_munites) + ":" + str(duration_seconds))


if __name__ == '__main__':
    mainFunc(sys.argv[1:])

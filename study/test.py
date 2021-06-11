# -*- encoding:utf-8 -*-

"""
@Author  : chenxingcong
@Contact : chenxingcong@mail01.huawei.com
@File    : modelMonitorDisGbase.py
@Time    : 2021/2/23 10:57
@Desc    :
"""

import datetime
# from GbaseSQLEngine import GbaseSQLEngine
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
            if len(i) == 0:
                break
            dictTable = dict(zip(title, i))
            if len(args_period) == 6 and dictTable['period'] == 'M':
                dictConfig.append(dictTable)
            elif len(args_period) == 8 and dictTable['period'] == 'D':
                dictConfig.append(dictTable)
            else:
                print(len(args_period))
                print(dictTable['period'])
                print(str(line) + '：该行不符合规范，请检查配置文件')
                sys.exit()
    # print(dictConfig)
    # return dictConfig

    if len(dictConfig) == 0:
        print("周期参数与文件格式不符，请检查！")
        sys.exit()

    configColumns = {}
    tableNames = set()
    for i in dictConfig:
        table_name = i['table_name']
        tableNames.add(table_name)
        period_col = i['period_col']
        period = i['period']
        configColumns.setdefault(table_name + period_col + period, {
            'table_name': table_name,
            'period_col': period_col,
            'period': period,
            'column': []
        })['column'].append(i['column'])
    return list(configColumns.values()), tableNames


def initSql(dictCofnig, args_period):
    column = dictCofnig['column']
    period_col = dictCofnig['period_col']
    sumColumns = []
    for col in column:
        sumColumn = "'%s',':',nvl(sum(cast(%s as decimal(38,2))),0)" % (col, col)
        sumColumns.append(sumColumn)
    summer = ",'|',".join(sumColumns)
    if 'N' in column and period_col == 'S':
        sumSql = "select '%s','T',count(1),'%s' from %s" % (
            dictCofnig['table_name'], args_period, dictCofnig['table_name'])
    elif 'N' in column:
        sumSql = "select '%s','T',count(1),'%s' from %s where %s=%s" % (
            dictCofnig['table_name'], args_period, dictCofnig['table_name'], dictCofnig['period_col'], args_period)
    elif period_col == 'S':
        sumSql = "select '%s',concat(%s),count(1),'%s' from %s" % (
            dictCofnig['table_name'], summer, args_period, dictCofnig['table_name'])
    else:
        sumSql = "select '%s',concat(%s),count(1),'%s' from %s where %s=%s" % (
            dictCofnig['table_name'], summer, args_period, dictCofnig['table_name'], dictCofnig['period_col'],
            args_period)
    # print(sumSql)
    return sumSql


def partition(lst, step):
    # division = len(lst) / float(n)
    # return [lst[int(round(division * i)): int(round(division * (i + 1)))] for i in range(n)]

    division = int(math.ceil(float(len(lst)) / step))
    return [lst[i * step:i * step + step] for i in range(division)]


def args_periods(args_period):
    # periods = [args_period]
    periods = []
    if len(args_period) == 8:
        end = datetime.datetime.strptime(args_period, "%Y%m%d")
        start = datetime.datetime.strptime('20210301', "%Y%m%d")
        # print((end - start).days)
        #for i in range((end - start).days ):
        for i in range((end - start).days + 1):
            aa = (start + datetime.timedelta(days=i)).strftime("%Y%m%d")
            # print(aa)
            periods.append(aa)
        print(periods)
        return periods

    else:
        num = (int(args_period[:4]) - 2021) * 12 + (int(args_period[-2:]) - 2)
        for i in range(num):
            print(i)
            periods.append(add_month('202103', i))
            print(periods)
        return periods


def add_month(datamonth, num):
    """
    月份加减函数,返回字符串类型
    :param datamonth: 时间(201501)
    :param num: 要加(减)的月份数量
    :return: 时间(str)
    """
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    datamonth = int(datamonth)
    num = int(num)
    year = datamonth // 100
    new_list = []
    s = math.ceil(abs(num) / 12)
    for i in range(int(-s), int(s + 1)):
        new_list += [str(year + i) + x for x in months]
    new_list = [int(x) for x in new_list]
    return str(new_list[new_list.index(datamonth) + num])


def unionAllSqls(dictCofnigs, step):
    unionSqls = []
    for dictCofnig, args_period in dictCofnigs:
        sql = initSql(dictCofnig, args_period)
        unionSqls.append((sql, dictCofnig['table_name']))
    unionSql = []
    for i in partition(unionSqls, step):
        sql = []
        tables = []
        for d in i:
            sql.append(d[0])
            tables.append(d[1])
        unionall = ' union all '.join(sql)
        unionSql.append((unionall, tables))

    return unionSql


def mainFunc(args):
    # configFileName = "D:\pythonfile\dwh_day.txt"
    # args_period = '20210205'
    # configFileName = "/home/chenxc/pythonfile/mk.txt"
    # monitorResult = "/home/chenxc/pythonfile/monitorResult.txt"
    step = 1
    # 时间戳
    timeNow = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    print(args)

    if len(args[0]) == 6 or len(args[0]) == 8:
        args_period = args[0]
    else:
        print("周期不正确")
        sys.exit()
    if os.path.isfile(args[1]):
        configFileName = args[1]
        # monitorResult = args[2]
    else:
        print("配置文件/结果文件不正确")
        sys.exit()

    if len(args_period) == 8:
        insertTable = 'pub.tdet_jt_modeldatamonitor_dis_d'
        errorTable = 'pub.tdet_jt_modeldatamonitor_dis_error_d'
        statisDate = 'statis_date'
    else:
        insertTable = 'pub.tdet_jt_modeldatamonitor_dis_m'
        errorTable = 'pub.tdet_jt_modeldatamonitor_dis_error_m'
        statisDate = 'statis_month'

    logger = logging.getLogger()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    logger.setLevel(logging.DEBUG)

    input_file_name = os.path.basename(configFileName).split(".")[0]
    basedir = os.path.dirname(configFileName)
    log_path = os.path.join(basedir, input_file_name + ".log")

    _file_handler = logging.FileHandler(log_path)
    _file_handler.setLevel(level=logging.INFO)
    _file_handler.setFormatter(formatter)
    logger.addHandler(_file_handler)
    # 添加日志控制台输出
    _console_handler = logging.StreamHandler()
    _console_handler.setLevel(level=logging.INFO)
    _console_handler.setFormatter(formatter)
    logger.addHandler(_console_handler)

    start_time = datetime.datetime.now()
    gbase_db = GbaseSQLEngine()
    cursor_gbase = gbase_db.cursor

    gbase_lishi = "select d1.table_name,d1.record_cycle from %s d1 group by d1.table_name,d1.record_cycle" % insertTable
    logger.info(gbase_lishi)
    cursor_gbase.execute(gbase_lishi)
    gbase_rows = cursor_gbase.fetchall()

    dictCofnigs, tableNames = initConfig(configFileName, args_period)

    dictAllCofnigs = []
    for tableName in tableNames:
        for i in args_periods(args_period):
            aa = (tableName, i)
            if aa not in gbase_rows:
                for d in dictCofnigs:
                    if tableName == d['table_name']:
                        dictAllCofnigs.append((d, i))

    unionSqls = unionAllSqls(dictAllCofnigs, step)

    deleteError = "delete from %s where error_type='SqlError' and %s='%s'" % (errorTable, statisDate, args_period)
    logger.info(deleteError)
    cursor_gbase.execute(deleteError)

    for dd in unionSqls:
        sql, talbes = dd
        logger.info(talbes)
        func_start_time = datetime.datetime.now()
        try:
            cursor_gbase.execute(sql)
            rows = cursor_gbase.fetchall()
        except:
            logger.exception('Sql执行异常：%s' % str(talbes))
            insertError = "insert into %s values ('SqlError','%s','%s','%s')" % (
                errorTable, '|'.join(talbes), args_period, args_period)
            print(insertError)
            cursor_gbase.execute(insertError)
            gbase_db.conn.commit()
            continue

        for row in rows:
            logger.info(row)
            # print(row)
            # (u'dwh.td_ac_adjustbill_log_m', u'region:406559272|billcycle:260866716174|oldcycle:260866716174',1290774)
            # (u'dwh.td_sc_subs_m', u'T', 334605675)

            try:
                if 'T' in row:
                    tableTitle = ['table_name', 'total']
                    tableData = [row[0], row[2]]
                    dictResult = dict(zip(tableTitle, tableData))
                    # mk.tm_ac_balance_d | st20210201 | 5000 | acct_balance | 30000
                    # result = dictResult['table_name'] + '|' + args_period + '|' + str(dictResult['total']) + '|total|' + str(dictResult['total'])
                    insertSql = "insert into %s values('%s','%s','%s','%s','%s','%s')" % (
                        insertTable, dictResult['table_name'], row[3], str(dictResult['total']), 'total',
                        str(dictResult['total']), row[3])
                    print(insertSql)
                    cursor_gbase.execute(insertSql)
                    gbase_db.conn.commit()
                else:
                    # (u'dwh.td_ac_adjustbill_log_m', u'region:406559272|billcycle:260866716174|oldcycle:260866716174',1290774)
                    tableName = row[0]
                    tableData = row[1]
                    tableRownum = row[2]
                    columnDatas = str(tableData).split('|')
                    for i in columnDatas:
                        columnData = str(i).split(':')
                        column = columnData[0]
                        rownum = columnData[1]
                        # mk.tm_ac_balance_d | st20210201 | 5000 | acct_balance | 30000
                        # result = tableName + '|' + args_period + '|' + str(tableRownum) + '|' + column + '|' + str(rownum)
                        insertSql = "insert into %s values('%s','%s','%s','%s','%s','%s')" % (
                            insertTable, tableName, row[3], str(tableRownum), column, str(rownum), row[3])
                        print(insertSql)
                        cursor_gbase.execute(insertSql)
                        gbase_db.conn.commit()
            except:
                logger.exception('%s数据处理异常' % str(row[0]))
                insertError = "insert into %s values ('dataError','%s','%s','%s')" % (
                    errorTable, str(row[0]), row[3], args_period)
                print(insertError)
                cursor_gbase.execute(insertError)

        func_end_time = datetime.datetime.now()
        func_duration_seconds = (func_end_time - func_start_time).seconds % 60
        func_duration_munites = (func_end_time - func_start_time).seconds / 60 % 60
        func_duration_hours = (func_end_time - func_start_time).seconds / 60 / 60
        print("func_Duration    : " + str(func_duration_hours) + ":" + str(func_duration_munites) + ":" + str(
            func_duration_seconds))

    gbase_db.conn.commit()
    gbase_db.close()
    end_time = datetime.datetime.now()
    duration_seconds = (end_time - start_time).seconds % 60
    duration_munites = (end_time - start_time).seconds / 60 % 60
    duration_hours = (end_time - start_time).seconds / 60 / 60
    print("Duration    : " + str(duration_hours) + ":" + str(duration_munites) + ":" + str(duration_seconds))
    logger.info("Duration    : " + str(duration_hours) + ":" + str(duration_munites) + ":" + str(duration_seconds))


if __name__ == '__main__':
    # mainFunc(sys.argv[1:])

    args_periods('20210428')

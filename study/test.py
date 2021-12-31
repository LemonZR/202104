# -*- encoding:utf-8 -*-

"""
@Author  : chenxingcong
@Contact : chenxingcong@mail01.huawei.com
@File    : modelRecordCheck.py
@Time    : 2021/6/29 14:18
@Desc    : 通过校验程序的报错，阻止调度成功状态的生成，避免后续任务受影响，导致大批量调度重跑，影响bdi性能，影响数据核对。
"""

from GbaseSQLEngine import GbaseSQLEngine
from HiveSQLEngine import HiveSQLEngine
from pylogger import pylogger
import datetime
import math
import sys


def getday(period, day):
    # today = datetime.date.today()
    today = datetime.datetime.strptime(period, "%Y%m%d")
    oneday = datetime.timedelta(days=day)
    yesterday = today + oneday
    return yesterday.strftime('%Y%m%d')


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


def modelRecordCheck(args):
    """
    通过校验程序的报错，阻止调度成功状态的生成，避免后续任务受影响，导致大批量调度重跑，影响bdi性能，影响数据核对。
    :param args: 模型表名、分区字段、执行账期、波动阈值
    :return:
    """
    log_path = "/data/script/hbdb/py_log/modelRecordCheck.log"
    logger = pylogger(log_path)

    if len(args) != 4:
        raise Exception('输入参数有误，请重新输入')

    tablename = args[0]
    statis_cycle = args[1]
    period = args[2]
    checkrecord = args[3]

    if len(args[2]) == 6:
        yesterday = add_month(period, -1)
    elif len(args[2]) == 8:
        yesterday = getday(period, -1)
    else:
        logger.info("执行周期参数错误")
        raise Exception('执行周期参数错误')

    db_hive = HiveSQLEngine()
    cursor_hive = db_hive.cursor

    sql_hive_select = "select count(1) from %s where %s=%s" % (tablename, statis_cycle, period)
    cursor_hive.execute(sql_hive_select)
    hive_rows = cursor_hive.fetchall()
    record_vi_day = hive_rows[0][0]
    logger.info("模型数据统计完成，记录数：%s" % record_vi_day)

    """
        pub.tdet_modelrecord_his_d
        tablename       模型表名
        statis_date     执行账期
        record_cnt      执行账期的记录数
        createtime      该条记录插入的时间戳，格式:20210623122347
    """
    timeNow = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

    db_gbase = GbaseSQLEngine()
    cursor_gbase = db_gbase.cursor
    # sql_gbase_delete = "delete from pub.tdet_modelrecord_his_d where statis_date=%s and tablename='%s'" % (period, tablename)
    # cursor_gbase.execute(sql_gbase_delete)
    # db_gbase.commit()
    sql_gbase_insert = "insert into pub.tdet_modelrecord_his_d values('%s','%s','%s','%s')" % (tablename, period, record_vi_day, timeNow)
    cursor_gbase.execute(sql_gbase_insert)
    db_gbase.commit()
    logger.info("记录模型当天数据完成")

    # sql_gbase_select = "select record_cnt from pub.tdet_modelrecord_his_d where statis_date=%s and tablename='%s'" % (yesterday, tablename)
    sql_gbase_select = """
        select
            u1.record_cnt
        from 
        (
            select
                d1.tablename
                ,d1.statis_date
                ,d1.record_cnt
                ,row_number() over(partition by d1.tablename,d1.statis_date order by d1.createtime desc) rn
            from pub.tdet_modelrecord_his_d d1
            where d1.statis_date=%s
            and d1.tablename='%s'
        ) u1
        where u1.rn=1
    """ % (yesterday, tablename)
    cursor_gbase.execute(sql_gbase_select)
    gbase_rows = cursor_gbase.fetchall()
    if len(gbase_rows) == 0:
        logger.info("暂无模型昨天记录")
    else:
        record_vl_day = gbase_rows[0][0]
        if record_vl_day == 0:
            record_vl_day = 1
        record_check = abs(record_vi_day * 1.0 / record_vl_day * 1.0 - 1)
        if record_check > float(checkrecord):
            cursor_gbase.close()
            db_gbase.close()
            cursor_hive.close()
            db_hive.close()
            logger.info("波动值大于波动阈值，波动值：%s，波动阈值：%s，请核实数据；" % (record_check, checkrecord))
            raise Exception("波动值大于波动阈值，波动值：%s，波动阈值：%s，请核实数据；" % (record_check, checkrecord))
        else:
            """
                pub.tdet_modelrecord_check_d
                tablename           模型表名
                statis_date         执行账期
                record_cnt          执行账期的记录数
                record_check        波动值
                record_check_exp    波动阈值
                createtime          该条记录插入的时间戳，格式:20210623122347
            """

            # sql_gbase_insert_check_delete = "delete from pub.tdet_modelrecord_check_d where statis_date=%s and tablename='%s'" % (period, tablename)
            # cursor_gbase.execute(sql_gbase_insert_check_delete)
            # db_gbase.commit()
            sql_gbase_insert_check = "insert into pub.tdet_modelrecord_check_d values('%s','%s','%s','%s','%s','%s')" % (
                tablename, period, record_vi_day, record_check, checkrecord, timeNow)
            cursor_gbase.execute(sql_gbase_insert_check)
            db_gbase.commit()
            logger.info("波动值不大于波动阈值，波动值：%s，波动阈值：%s" % (record_check, checkrecord))
    cursor_gbase.close()
    db_gbase.close()
    cursor_hive.close()
    db_hive.close()


if __name__ == '__main__':
    modelRecordCheck(sys.argv[1:])

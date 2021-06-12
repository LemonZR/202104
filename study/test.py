import datetime, calendar, sys, re


def get_partitions():
    dateStr = '20210512'
    if len(dateStr) == 8:
        vl_month = datetime.datetime.strptime(dateStr[:6] + '01', '%Y%m01') - datetime.timedelta(days=1)
        pattern = r'=%s$|=%s$|1=1' % (dateStr, vl_month)
    elif len(dateStr) == 6:
        weekday, monthdays = calendar.monthrange(int(dateStr[:4]), int(dateStr[4:6]))
        vi_month_last = dateStr[:6] + str(monthdays)
        pattern = r'=%s$|%s$|1=1' % (dateStr, vi_month_last)
    else:
        sys.exit('asd')

    return 0

get_partitions()
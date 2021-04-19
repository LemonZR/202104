# coding=utf-8


def read_file(file_name):
    dic = {}
    with open(file_name, 'r', encoding='utf-8') as f:
        i = 0
        for line in f:
            column, statis_str, table_name, date_str = map(str.strip, line.strip().split('\t'))

            dic[table_name] = dic.get(table_name, {})
            dic[table_name]['statis_str'] = statis_str
            dic[table_name]['column'] = dic[table_name].get('column', [])
            if column != 'zr':
                dic[table_name]['column'].append(column)
            dic[table_name]['date_str'] = date_str
    return dic


def sql_generator(dic, file_name):
    result_file_name = file_name.split('.')[0] + '_new.txt'
    result_file = open(result_file_name, 'w')
    sql_list = []
    for table_name, info_dic in dic.items():
        columns = info_dic['column']
        new_columns = []
        for column in columns:
            new_column = "'{0}:',nvl(sum(cast({0} as bigint)),0)".format(column)
            new_columns.append(new_column)
        statis_str = info_dic['statis_str']
        date_str = dic[table_name]['date_str']
        sum_str = ",'|',".join(new_columns)
        sql = '''select '{0}',count(1),concat({1}),{3} ,'{2}'from {0} where {2}={3}'''.format(
            table_name, sum_str, statis_str, date_str)
        sql_list.append(sql)

        # result_file.write(sql + '\n')
    print(sql_list.__len__())

    s = 0
    new_sql_list = []
    m = 30
    n = int(len(sql_list) / m)
    if n == 0:
        n = 1
    for i in range(0, n):
        if i == n - 1:
            nl = sql_list[s:]
        else:
            nl = sql_list[s:s + m]
        new_sql_list.append(nl)
        s = s + m
    for i in new_sql_list:
        sql = 'insert into hw_tmp.tm_sc_checking_data\n' + '\nunion all\n'.join(i) + ';'
        result_file.write(sql + '\n')


if __name__ == '__main__':
    file_name = 'data'

    dic = read_file(file_name)
    sql_generator(dic, file_name)

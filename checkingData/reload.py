# coding=utf-8


def read_file(file_name):
    dic = {}
    with open(file_name, 'r', encoding='utf-8') as f:

        for line in f:
            table_name, cnt, columns, date_str, statis_str = map(str.strip, line.strip().split('\t'))
            dic[table_name] = dic.get(table_name, {})
            dic[table_name]['statis_str'] = statis_str
            dic[table_name]['cnt'] = cnt
            dic[table_name]['columns'] = dic[table_name].get('columns', {})
            for column in columns.split('|'):
                try:
                    c_name, val = column.split(':')
                    dic[table_name]['columns'][c_name] = dic[table_name]['columns'].get(c_name, 0)
                    dic[table_name]['columns'][c_name] = val.strip()
                except Exception as e:
                    pass
            dic[table_name]['date_str'] = date_str
    return dic


def wri(p_dic, g_dic, file_name):
    result_sum_name = file_name.split('.')[0] + '_sum.csv'
    result_cnt_name = file_name.split('.')[0] + '_cnt.csv'
    sum_file = open(result_sum_name, 'w')
    cnt_file = open(result_cnt_name, 'w')
    # cnt_file.write('表名,总记录数,数据周期,周期字段\n')
    # sum_file.write('表名,指标字段,指标值,数据周期,周期字段\n')
    cnt_file.write('表名,省总记录数,集团记录数,数据周期,周期字段\n')
    sum_file.write('表名,指标字段,省指标值,集团指标值,数据周期,周期字段\n')
    for table_name, info_dic in p_dic.items():
        cnt = info_dic['cnt']
        date_str = info_dic['date_str']
        statis_str = info_dic['statis_str']
        column_info = info_dic['columns']

        gc_cnt = g_dic[table_name]['cnt']
        gc_cloumn_info = g_dic[table_name]['columns']
        cnt_file.write('%s,%s,%s,%s,%s\n' % (table_name, cnt, gc_cnt, date_str, statis_str))
        for c_name, val in column_info.items():
            gc_val = gc_cloumn_info[c_name]
            sum_file.write("%s,%s,%s,%s,%s,%s\n" % (table_name, c_name, val, gc_val, date_str, statis_str))


if __name__ == '__main__':
    prov_file_name = 'data_dir/prov_checking_data'
    gc_file_name = 'data_dir/gc_checking_data'
    result_file = 'data_dir/result_checking_data'
    prov_dict = read_file(prov_file_name)
    gc_dict = read_file(gc_file_name)

    wri(prov_dict, gc_dict, result_file)

# coding=utf-8

import re
import os
import copy
from tools import excelOp


def ergodic_dirs(root_dir='D:\\sql_gen\\bd_hive') -> list[str]:
    file_list = []
    for root, dirs, files in os.walk(root_dir):

        for file in files:
            file = os.path.join(root, file)
            file_list.append(file)

    return file_list


def get_sql_dict(file) -> list:
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

    return sql_blks


def get_files_sql_dict(dir_name='D:\\tmp\\') -> dict[str, list]:
    file_list = ergodic_dirs(dir_name)
    file_layer_dict = {}
    for file in file_list:
        fil_name = os.path.basename(file)
        sql_info = get_sql_dict(file)
        file_layer_dict[fil_name] = sql_info
    return file_layer_dict


def cal_deps(dict_data: dict[str, list]):
    for key, values in dict_data.items():

        for value in values:

            index = dict_data[key].index(value)

            new_value = dict_data.get(value, [])

            if new_value:
                dict_data[key][index] = {value: new_value}

            else:
                continue


def find_pattern(sql_list, pattern=r'union\s*all', pattern_mod=re.IGNORECASE) -> list:
    pattern = pattern
    result_list = []
    for sql_blk in sql_list:
        result = re.findall(pattern, sql_blk, flags=pattern_mod)
        if result:
            for r in result:
                result_list.append(r)

    return result_list


def analyze(dir_name, pattern_mod=re.IGNORECASE) -> dict:
    file_sqls_info = get_files_sql_dict(dir_name)
    insert_pattern = r"insert\s*into\s*table\s*\S*|insert\s*into\s*\S*|insert\s*overwrite\s*table\s*\S*"
    from_sql_pattern = r'select[\s\S]*from[\s\S]*'
    heads = r'mk\.|pub\.|dis\.|dw\.|dwh\.|am\.|det\.'
    # 包含_${gbvar:vi_day}
    table_pattern = r'(?=%s)[a-zA-Z0-9_\.\$\{:\}"]*' % heads
    result = {}
    for file_name, sql_info in file_sqls_info.items():
        insert_sql_list = find_pattern(sql_info, pattern=insert_pattern, pattern_mod=pattern_mod)
        from_sql_list = find_pattern(sql_info, pattern=from_sql_pattern, pattern_mod=pattern_mod)
        # 将pre脚本与主脚本合并,转小写
        # pre 或 per 。。。
        # file_name = re.sub(r'_pre_*\d+[_\d]*\.sql|_per_*\d+[_\d]*\.sql', '.sql', file_name, flags=re.I).lower()
        file_name = file_name.lower()
        # 部分脚本不合常识比如mk_pm_sc_user_lte_d，前置脚本有其他词语
        result.setdefault(file_name, {})
        if insert_sql_list:
            for insert_sql in insert_sql_list:
                target_table = re.findall(table_pattern, insert_sql, re.I)
                if target_table:
                    target_table = re.sub('"', '', target_table[0]).lower()
                    result[file_name].setdefault('target_table', set()).add(target_table)
        if from_sql_list:
            for from_sql in from_sql_list:
                dep_tables = re.findall(table_pattern, from_sql, re.I)
                if dep_tables:
                    for dep_table in dep_tables:
                        result[file_name].setdefault('deps', []).append(dep_table.lower())

        result[file_name]['deps'] = list(set(result[file_name].get('deps', [])))

    return result


def run(dir_name):
    print('analyze start' + '*' * 100)
    data = analyze(dir_name)
    print('analyze end' + '*' * 100)

    excel_data_direct_deps = [('脚本名', '目标表名')]

    print('生成excel data2 start' + '*' * 100)
    for file, info in data.items():
        # deps = info.get('deps', ['no_dep'])
        target_table = info.get('target_table', ('没有插入正式表',))

        for tt in target_table:
            # 再加一列，去掉dis中的月分区表。
            tt_with_no_mon_tail = re.sub(r'_\$\{gbvar:.*month\}$', '', tt)
            excel_data_direct_deps.append([file, tt, tt_with_no_mon_tail])
    print('生成excel data2 end\n' + '*' * 100)
    return excel_data_direct_deps


if __name__ == '__main__':
    mk_dirName = r'D:\bd_hive\mk'
    dis_dirName = r'D:\bd_hive\dis'
    pub_dirName = r'D:\bd_hive\pub'
    am_dirName = r'D:\bd_hive\am'
    dwh_dirName = r'D:\bd_hive\dwh'
    data1 = run(mk_dirName)
    data2 = run(dis_dirName)
    data3 = run(pub_dirName)
    data4 = run(am_dirName)
    data5 = run(dwh_dirName)
    data_info = [('mk', data1), ('dis', data2), ('pub', data3), ('am', data4), ('dwh', data5)]
    result_xlsx = 'D:\\脚本中插入的表_20220115.xlsx'
    excelOp.write_many_sheets_xlsx(filename=result_xlsx, data_info=data_info, edit=True)

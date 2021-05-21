# coding=utf-8

import re
import os
import sys

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
        ff = re.sub(r'\n+', '\n', re.sub(r'--\s*.*\n', '\n', fl.read()))
    except Exception as e1:
        try:
            fl = open(file, 'r', encoding='utf-8')
            ff = re.sub(r'\n+', '\n', re.sub(r'--\s*.*\n', '\n', fl.read()))
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
                dict_data[key][index] = new_value
            else:
                continue
    return dict_data


def find_pattern(sql_list, pattern=r'union\s*all', pattern_mod=re.IGNORECASE) -> list:
    pattern = pattern
    result_list = []
    for sql_blk in sql_list:
        result = re.findall(pattern, sql_blk, flags=pattern_mod)
        if result:
            for r in result:
                result_list.append(r)

    return result_list


def analyze(dir_name, pattern_mod=re.IGNORECASE) -> dict[str, dict]:
    file_sqls_info = get_files_sql_dict(dir_name)
    insert_pattern = r"insert\s*into\s*table\s*\S*|insert\s*into\s*\S*|insert\s*overwrite\s*table\s*\S*"
    from_sql_pattern = r'from[\s\S]*'
    heads = r'mk\.|pub\.|dis\.|dw\.|dwh\.|am\.|det\.'
    table_pattern = r'(?=%s)[a-zA-Z0-9_\.\$\{:\}]*' % heads
    result = {}
    for file_name, sql_info in file_sqls_info.items():
        insert_sql_list = find_pattern(sql_info, pattern=insert_pattern, pattern_mod=pattern_mod)
        from_sql_list = find_pattern(sql_info, pattern=from_sql_pattern, pattern_mod=pattern_mod)
        # 将pre脚本与主脚本合并,转小写
        file_name = re.sub(r'_pre\d+\.sql', '.sql', file_name, flags=re.I).lower()
        result.setdefault(file_name, {})
        if insert_sql_list:
            for insert_sql in insert_sql_list:
                target_table = re.findall(table_pattern, insert_sql, re.I)
                if target_table:
                    result[file_name].setdefault('target_table', target_table[0].lower())
        if from_sql_list:
            for from_sql in from_sql_list:
                dep_tables = re.findall(table_pattern, from_sql, re.I)
                if dep_tables:
                    for dep_table in dep_tables:
                        result[file_name].setdefault('deps', []).append(dep_table.lower())

    return result


def iterates(deps_list, result=[], id_list=[], depth=0):
    #id_list.append(id(deps_list))
    for value in deps_list:
        dep_id = id(value)
        id_list.append(dep_id)

        if isinstance(value, list):
            if dep_id in id_list:
                result.append(('self', depth))
            else:
                iterates(value, result, id_list, depth + 1)
        else:
            result.append((value, depth))
    result = list(set(result))
    return result


def run(dir_name):
    reult_data = analyze(dir_name)
    t_dict = {}
    for file, info in reult_data.items():
        target_table = info.get('target_table', '')
        if not target_table:
            print(file)
            sys.exit('没有目标表')
        t_dict[target_table] = info.get('deps', [])
    # 将依赖表查找至最底层
    t_result = cal_deps(t_dict)

    for file, info in reult_data.items():
        target_table = info.get('target_table', '')
        reult_data[file]['deps'] = iterates(t_result.get(target_table, []))

    excel_data = [('脚本名', '目标表名', '依赖表名')]
    for file, info in reult_data.items():
        deps_list = info.get('deps', [('no_dep', 'no_')])
        target_table = info.get('target_table', 'erro')
        for dep_tuple in deps_list:
            excel_data.append([file, target_table] + list(dep_tuple))
    return excel_data


if __name__ == '__main__':
    dirName = 'D:\\tmp_mk\\'
    data = run(dirName)
    result_xlsx = 'D:\\数据核对\\all_dependent_table.xlsx'
    excelOp.write_xlsx(result_xlsx, data, edit=True)

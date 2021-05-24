# coding=utf-8

import re
import os
import copy
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
        # pre 或 per 。。。
        file_name = re.sub(r'_pre_*\d+[_\d]*\.sql|_per_*\d+[_\d]*\.sql', '.sql', file_name, flags=re.I).lower()
        # 部分脚本不合常识比如mk_pm_sc_user_lte_d，前置脚本有其他词语
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

        result[file_name]['deps'] = list(set(result[file_name].get('deps', [])))

    return result


def iterates(my_name, deps_list, __id_dict={}):
    result = []
    myself_id = id(deps_list)
    __id_dict = copy.deepcopy(__id_dict)
    parents = __id_dict.setdefault('parents', {})
    parents.setdefault('p_tables', []).append(my_name)
    __p_id_list = parents.setdefault('p_id', [])
    __p_id_list.append(myself_id)
    depth_dict = __id_dict.setdefault('depth', {})
    depth = depth_dict.setdefault(myself_id, 0)
    for value in deps_list:

        if isinstance(value, dict):
            dep_name = list(value.keys())[0]
            dep_value = list(value.values())[0]
            child_id = id(dep_value)
            result.append([depth] + parents['p_tables'] + [dep_name])
            if child_id in __p_id_list:
                # result.append([depth] + parents + ['self' + str(depth)])
                result.append([depth] + parents['p_tables'] + [dep_name])
            else:
                __id_dict.setdefault('depth', {}).setdefault(child_id, depth + 1)
                result += iterates(dep_name, dep_value, __id_dict)
        elif depth == 0:

            result.append([depth] + parents['p_tables'] + [value])

    return result


def run(dir_name):
    print('analyze start' + '*' * 100)
    data = analyze(dir_name)
    print('analyze end' + '*' * 100)

    t_data = copy.deepcopy(data)
    t_dict = {}
    result = {}

    # 如果脚本没有目标表，则用脚本名替代（作为后续的键值对的键）
    for file, info in t_data.items():  # 由于不是深复制，t_data的列表的'指针'复制给了t_dict 在循环中会发生了变化
        target_table = info.get('target_table', file)
        t_dict[target_table] = info.get('deps', [])

    # 将依赖关系查找至最底层
    # 由于不是深复制，t_data的列表值会因为t_dict 在cal_deps中变化而变化
    print('cal_deps start' + '*' * 100)
    cal_deps(t_dict)
    print('cal_deps end' + '*' * 100)
    print('将各层依赖逐一查找并替换为最底层依赖 start' + '*' * 100)
    # 将各层依赖逐一查找并替换为最底层依赖
    for file, info in t_data.items():
        target_table = info.get('target_table', file)

        result.setdefault(file, {'target_table': target_table,
                                 'deps': iterates(target_table, t_dict.get(target_table, []))})
    print('将各层依赖逐一查找并替换为最底层依赖 end' + '*' * 100)
    excel_data_all_deps = [('脚本名', '目标表名', '目标表层级', '依赖表深度', '依赖表名',)]
    excel_data_direct_deps = [('脚本名', '目标表名', '依赖表名')]

    print('生成excel data1 start' + '*' * 100)
    for file, info in result.items():
        deps_list = info.get('deps', [('no_dep', 0)])
        target_table = info.get('target_table', 'erro')
        try:
            layer_level = max((i[0] for i in deps_list)) + 1
        except:
            layer_level = '需要看前置脚本'
        for dep in deps_list:
            excel_data_all_deps.append([file, target_table] + [layer_level] + dep)
    print('生成excel data1 end\n' + '*' * 100)
    print('生成excel data2 start' + '*' * 100)
    for file, info in data.items():
        deps = info.get('deps', ['no_dep'])
        target_table = info.get('target_table', 'erro')

        for dep in deps:
            excel_data_direct_deps.append([file, target_table, dep])
    print('生成excel data2 end\n' + '*' * 100)
    return excel_data_all_deps, excel_data_direct_deps


if __name__ == '__main__':
    dirName = 'D:\\tmp\\mk'
    data1, data2 = run(dirName)
    print(len(data1))
    sys.exit()
    # 先写小的，避免重复打开大数据表
    result_xlsx = 'D:\\数据核对\\all_dependent_table_20210524_依赖.xlsx'
    print('写入excel：直接依赖 start' + '*' * 100)
    excelOp.write_xlsx(result_xlsx, data2, edit=True, sheet_name='direct_合并_new')
    print('写入excel：直接依赖 end' + '*' * 100)
    print('写入excel：所有依赖 start' + '*' * 100)
    excelOp.write_xlsx(result_xlsx, data1, edit=True, sheet_name='all_new')
    print('写入excel：所有依赖 end' + '*' * 100)

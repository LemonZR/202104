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
    from_sql_pattern = r'select[\s\S]*from[\s\S]*'
    heads = r'mk\.|pub\.|dis\.|dw\.|dwh\.|am\.|det\.'
    # table_pattern = r'(?=%s)[a-zA-Z0-9_\.\$\{:\}]*' % heads
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
                insert_sql = re.sub(r'[\"\']', '', insert_sql)
                target_table = re.findall(table_pattern, insert_sql, re.I)
                if target_table:
                    result[file_name].setdefault('target_table', set()).add(target_table[0].lower())
        # if from_sql_list:
        #     for from_sql in from_sql_list:
        #         dep_tables = re.findall(table_pattern, from_sql, re.I)
        #         if dep_tables:
        #             for dep_table in dep_tables:
        #                 result[file_name].setdefault('deps', []).append(dep_table.lower())

        # result[file_name]['deps'] = list(set(result[file_name].get('deps', [])))

    return result


def run(dir_name):
    print('analyze start' + '*' * 100)
    data = analyze(dir_name)
    print('analyze end' + '*' * 100)

    excel_data_direct_deps = [('脚本名', '目标表个数', '目标表')]

    for file, info in data.items():
        deps = info.get('deps', ['no_dep'])
        target_tables = info.get('target_table', ['erro'])

        for target_table in target_tables:
            excel_data_direct_deps.append([file, len(target_tables), target_table])
    print('生成excel data2 end\n' + '*' * 100)
    return excel_data_direct_deps


if __name__ == '__main__':
    dirName = 'D:\\bd_hive\\dis'
    data = run(dirName)
    print(len(data))

    result_xlsx = 'D:\\dis_目标表们.xlsx'
    # 先写小的，避免第二次打开大数据表
    # print('写入excel：直接依赖 start' + '*' * 100)
    # excelOp.write_xlsx(result_xlsx, data2, edit=True, sheet_name='直接依赖')
    # print('写入excel：直接依赖 end' + '*' * 100)
    # print('写入excel：所有依赖 start' + '*' * 100)
    # excelOp.write_xlsx(result_xlsx, data1, edit=True, sheet_name='所有依赖')
    # print('写入excel：所有依赖 end' + '*' * 100)
    excelOp.write_many_sheets_xlsx(filename=result_xlsx, data_info=[('目标表', data)], edit=True)

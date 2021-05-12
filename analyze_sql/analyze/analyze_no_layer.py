# coding=utf-8

import re
import os
from tools import excelOp


def ergodic_dirs(root_dir='D:\\sql_gen\\bd_hive') -> list[str]:
    file_list = []
    for root, dirs, files in os.walk(root_dir):

        for file in files:
            file = os.path.join(root, file)
            file_list.append(file)

    return file_list


def get_sql_dict(file) -> dict[int, str]:
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

    sql_dict = {}

    for sql_blk_id, sql_blk in enumerate(sql_blks):
        sql_dict.setdefault(sql_blk_id, '')
        for c in sql_blk:
            sql_dict[sql_blk_id] += c

    return sql_dict


def get_files_layer(dir_name='D:\\tmp\\') -> dict[str, dict[int, str]]:
    file_list = ergodic_dirs(dir_name)
    file_layer_dict = {}
    for file in file_list:
        fil_name = os.path.basename(file)
        layer_info = get_sql_dict(file)
        file_layer_dict[fil_name] = layer_info
    return file_layer_dict


def has_pattern(sql_dict, pattern=r'union\s*all', pattern_mod=re.IGNORECASE) -> dict[int, str]:
    pattern = pattern
    result_dict = {}
    for blk_id, sql_layer_info in sql_dict.items():
        if re.findall(pattern, sql_layer_info, flags=pattern_mod):
            result_dict.setdefault(blk_id, sql_layer_info)

    return result_dict


def run(dir_name, pattern=r'union\s*all', pattern_mod=re.IGNORECASE) -> list[tuple]:
    """
    通用函数,查询脚本中 layer_num 层包含指定pattern的脚本,layer_num=None时,相当于查询全脚本语句
    :param dir_name: 需要遍历的脚本根目录
    :param pattern: 匹配正则表达式,例如 pattern='union\s*all'
    :param pattern_mod:正则中的flags ,如re.I,re.M等
    :return: list[tuple]

    """
    file_layers_info = get_files_layer(dir_name)
    pattern = pattern
    xlsx_data = [('脚本名', 'sql语句块')]
    for file_name, sql_info in file_layers_info.items():
        layer = has_pattern(sql_dict=sql_info, pattern=pattern, pattern_mod=pattern_mod)
        if layer:
            for sql_blk_id, sql in layer.items():
                sql = re.sub(r'^\s+', '', sql)  # 去掉开头的空白字符
                print(file_name, sql)
                xlsx_data.append((file_name, sql))
    return xlsx_data


if __name__ == '__main__':
    dirName = 'D:\\tmp\\'
    # 获取union all
    # result_file = 'D:\\数据核对\\union_target.xlsx'
    # union_all_data = run(dirName)
    # excelOp.write_xlsx(result_file, union_all_data, sheet_name='脚本和目标表')

    tmp_pattern = r'ded_dur[\s\S]*mk\.tm_cs_LG_OM_ORDER_D'
    data = run(dirName, tmp_pattern,  pattern_mod=re.I)
    result_file = 'D:\\数据核对\\analyze_mk_vl.xlsx'
    excelOp.write_xlsx(result_file, data, edit=True, sheet_name='脚本和语句')

    # run(dirName, tmp_pattern, layer_num=0)

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


def get_layer(file) -> dict[int, dict[int, str]]:
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

    layer_dict = {}
    layer_dict: dict[int, dict[int, str]]
    for sql_blk_id, sql_blk in enumerate(sql_blks):

        layer = 0
        for c in sql_blk:
            layer_dict.setdefault(sql_blk_id, {}).setdefault(layer, '')
            if c == '(':
                layer_dict[sql_blk_id][layer] += c + '...'
                layer -= 1
                continue
            if c == ')':
                layer += 1
            layer_dict.setdefault(sql_blk_id, {}).setdefault(layer, '')
            layer_dict[sql_blk_id][layer] += c

    return layer_dict


def get_files_layer(dir_name='D:\\tmp\\') -> dict[str, dict[int, dict[int, str]]]:
    file_list = ergodic_dirs(dir_name)
    file_layer_dict = {}
    for file in file_list:
        fil_name = os.path.basename(file)
        layer_info = get_layer(file)
        file_layer_dict[fil_name] = layer_info
    return file_layer_dict


def has_pattern(layer_dict, layer_num=None, pattern=r'union\s*all', pattern_mod=re.IGNORECASE) -> dict[int, str]:
    pattern = pattern
    result_dict = {}
    if layer_num is not None:
        for blk_id, sql_layer_info in layer_dict.items():
            layer_info = sql_layer_info.get(layer_num, '')
            if re.findall(pattern, layer_info, flags=pattern_mod):
                result_dict.setdefault(blk_id, layer_info)
    else:
        for blk_id, sql_layer_info in layer_dict.items():
            for layer_num, layer_info in sql_layer_info.items():
                if re.findall(pattern, layer_info, flags=pattern_mod):
                    print(layer_num)
                    result_dict.setdefault(blk_id, layer_info)
    return result_dict


def get_target_table(layer_dict: dict, layer_num=0) -> dict[str, str]:
    """
    特化函数,只查询insert语句中含有的table
    :param layer_dict:脚本文件分层信息
    :param layer_num:
    :return:
    """
    insert_pattern = r"insert\s*into\s*table\s*\S*|insert\s*into\s*\S*|insert\s*overwrite\s*table\s*\S*"
    heads = r'mk\.|pub\.|dis\.|dw\.|dwh\.|am\.|det\.'
    target_pattern = r'(?=%s)[a-zA-Z0-9_\.\$\{:\}]*' % heads
    result_dict = {}
    for blk_id, sql_layer_info in layer_dict.items():
        layer_info = sql_layer_info[layer_num]
        insert_blk = re.findall(insert_pattern, layer_info, re.I)
        if insert_blk:
            target_table_name: list = re.findall(target_pattern, insert_blk[0], re.I)
            if target_table_name:
                result_dict.setdefault(target_table_name[0], layer_info)
    return result_dict


def run(dir_name, pattern=r'union\s*all', pattern_mod=re.IGNORECASE, layer_num=None) -> list[tuple]:
    """
    通用函数,查询脚本中 layer_num 层包含指定pattern的脚本,layer_num=None时,相当于查询全脚本语句
    :param dir_name: 需要遍历的脚本根目录
    :param pattern: 匹配正则表达式,例如 pattern='union\s*all'
    :param pattern_mod:正则中的flags ,如re.I,re.M等
    :param layer_num: sql对象的分层序号,最外层为0,详见 get_layer(file)
    :return: list[tuple]

    """
    file_layers_info = get_files_layer(dir_name)
    pattern = pattern
    xlsx_data = [('脚本名', 'sql语句块')]
    for file_name, layer_info in file_layers_info.items():
        layer = has_pattern(layer_dict=layer_info, layer_num=layer_num, pattern=pattern, pattern_mod=pattern_mod)
        if layer:
            for sql_blk_id, sql in layer.items():
                sql = re.sub(r'^\s+', '', sql)  # 去掉开头的空白字符
                print(file_name, sql)
                xlsx_data.append((file_name, sql))
    return xlsx_data


def run_all_target(dir_name):
    file_layers_info = get_files_layer(dir_name)
    xlsx_data = [('脚本名', '目标表', 'sql语句块第0层')]
    for file_name, layer_info in file_layers_info.items():
        result = get_target_table(layer_info)
        for target_table, sql_layer_info in result.items():
            xlsx_data.append((file_name, target_table.lower(), sql_layer_info))
    return xlsx_data


if __name__ == '__main__':
    dirName = 'D:\\tmp\\test\\'
    # 获取union all
    # result_file = 'D:\\数据核对\\union_target.xlsx'
    # union_all_data = run(dirName)
    # excelOp.write_xlsx(result_file, union_all_data, sheet_name='脚本和目标表')

    tmp_pattern = r'acct_id[\s\S]*td_lg_grp_vip_credit_info_d'
    data = run(dirName, tmp_pattern, layer_num=None, pattern_mod=re.I)
    result_file = 'D:\\数据核对\\analyze_mk_vl.xlsx'
    excelOp.write_xlsx(result_file, data, edit=True, sheet_name='脚本和语句')

    # run(dirName, tmp_pattern, layer_num=0)

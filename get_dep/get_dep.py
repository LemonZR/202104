# coding=utf-8

import re
import os


def get_dep(file_name, pattern=r'mk\.|pub\.|dis\.|dw\.|dwh\.|am\.|det\.'):
    lis = []
    pattern = r'(?=%s)[a-zA-Z0-9_\.]*(?=|;|,|\s|_\$|\))' % pattern
    # pattern = r'(?=%s)[a-zA-Z0-9_\.]*_[a-zA-Z0-9]+(?=|;|,|\s|_\$|\))' % pattern
    try:
        '''去掉注释'''
        fl = open(file_name, 'r', encoding='gbk')
        ff = re.sub(r'\n+', '\n', re.sub(r'--\s*\S*\n', '\n', fl.read()))
    except Exception as e1:
        try:
            fl = open(file, 'r', encoding='utf-8')
            ff = re.sub(r'\n+', '\n', re.sub(r'--\s*\S*\n', '\n', fl.read()))
        except Exception as e2:
            print(f'{file}' + str(e1) + '\n' + str(e2))
            ff = ''
    for sql in ff.split(';')[:-1]:
        find_result = re.findall(pattern, sql, re.I)
        if find_result:
            for table in find_result:
                lis.append((table.strip()).lower())
    nl = set(lis)
    for i in nl:
        print(i)


if __name__ == '__main__':
    file_name = 'select.txt'
    file = os.getcwd() + '\\' + file_name
    get_dep(file, )

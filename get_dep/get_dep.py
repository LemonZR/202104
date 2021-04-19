# coding=utf-8

import re
import os


def get_dep(file_name, pattern=r'mk\.|pub\.|dis\.|dw\.|dwh\.|am\.|det\.'):
    f = open(file_name, 'r', encoding='utf-8')
    lis = []
    pattern = r'(?=%s)[a-zA-Z0-9_\.]*(?=;|,|\s|_\$|\))' % pattern
    for line in f:
        match = re.findall(pattern, line, re.I)
        if match:
            for mt in match:
                if re.match('session', mt):
                    continue
                else:
                    lis.append((mt.strip()).lower())
    nl = set(lis)
    for i in nl:
        print(i)


if __name__ == '__main__':
    file_name = 'select.txt'
    file = os.getcwd() + '\\' + file_name
    get_dep(file)

# encoding=utf8

import os
import re


def ergodic_dirs(root_dir='D:\\tmp'):
    file_list = []
    for root, dirs, files in os.walk(root_dir):
        # for dir_name in dirs:
        #     print(os.path.join(root, dir_name))
        # print(root)

        for file in files:
            file = os.path.join(root, file)
            file_list.append(file)

        # print(dirs)
        # print(files)
        # print('----------------')
    return file_list


def cal(files):
    pattern = r'union\s*all'  # union all 之间可能存在不同的分隔
    match_files = []
    for file in files:
        try:
            fl = open(file, 'r', encoding='gbk')
            lines = fl.readlines()
        except Exception as e1:
            try:
                fl = open(file, 'r', encoding='utf-8')
                lines = fl.readlines()
            except Exception as e2:
                print(f'{file}' + str(e1)+'\n'+str(e2))
                lines = []
        for line in lines:
            if re.findall(pattern, line.split('--')[0], re.I):  # 忽略大小写
                print(os.path.basename(file))
                match_files.append(file)
                break
    fl.close()
    return match_files


if __name__ == '__main__':

    dir = 'D:\\tmp\\'
    files = ergodic_dirs(dir)
    match_files = cal(files)
    with open('D:\\数据核对\\union_all_test.csv', 'a') as f:
        for file in match_files:
            dir_name = os.path.basename(os.path.dirname(file))
            file_name = os.path.basename(file)

            f.write(file_name + '\n')

# encoding=utf8

import os
import re


def ergodic_dirs(root_dir='D:\\sql_gen\\bd_hive'):
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
    heads = r'mk\.|pub|dis\.|dw\.|dwh\.|am\.|det\.'
    pattern = r'(?=%s)[a-zA-Z0-9_\.]*(?=;|,|\s|_\$|\))' % heads
    match_files = {}
    for fi in files:
        lis = []
        target_table_name = ''
        with open(fi, 'r', encoding='utf-8') as f:
            for line in f.readlines():
                line = (line.lower().split('--')[0].split('insert')[0]).split('alter')
                dependent_table_names = re.findall(pattern, line[0], re.I)

                if len(line) == 2:
                    try:
                        target_table_name = re.findall(pattern, line[1], re.I)[0] if re.findall(pattern, line[1],
                                                                                                re.I) else target_table_name
                        print(target_table_name)
                    except Exception as e:
                        print(fi + str(e))
                        exit()
                if dependent_table_names:
                    for dependent_table_name in dependent_table_names:
                        lis.append((dependent_table_name.strip()))

        nl = set(lis)
        for i in nl:
            # dep_name = i.split('_$')[0]
            file_name = os.path.basename(fi)

            key = f'{file_name},{target_table_name}'
            match_files[key] = match_files.get(key, [])
            match_files[key].append(i)

    return match_files


if __name__ == '__main__':
    root_dir = 'D:\\bd_hive\\mk'
    files = ergodic_dirs(root_dir)
    match_files = cal(files)
    with open('D:\\数据核对\\dependies_mk.csv', 'w') as f:
        f.write('脚本名,目标表,依赖表\n')
        for file, deps in match_files.items():
            for dep in deps:
                f.write(file + ',' + dep + '\n')

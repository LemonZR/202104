import re
import os


def ergodic_dirs(root_dir='D:\\bd_hive'):
    file_list = []
    for root, dirs, files in os.walk(root_dir):

        for file in files:
            file = os.path.join(root, file)
            file_list.append(file)

    return file_list


if __name__ == '__main__':
    result = ergodic_dirs()
    for i in result:
        print(i)
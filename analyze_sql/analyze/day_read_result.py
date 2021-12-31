# -*- encoding:utf-8 -*-

import time
# mk表相关比对文件的目录

def read_file_and_handle(in_file, out_file):
    datas = []
    f2 = open(out_file, 'w+', encoding='utf-8')
    with open(in_file, 'r', encoding='utf-8') as f1:
        for line in f1:
            line1 = line.replace(' ', '').replace('\n', '')
            # 去掉开头和结尾的竖线
            line2 = line1[1:len(line1) - 1].replace('|', '\t')
            datas.append(line2)
    if f1:
        f1.close()
        # print('close f1')
    f2.write('\n'.join(datas))
    if f2:
        f2.close()
        # print('close f2')
    print(in_file + ' 处理完毕！' + ' ' + time.strftime('%Y%m%d%H%M%S', time.localtime(time.time())))


if __name__ == '__main__':
    # 处理未通过的mk日表
    inp = 'D:\\数据核对\\mk_month_qingdan.txt'
    out ='D:\\数据核对\\mk_month_qingdan.result'
    read_file_and_handle(inp, out)

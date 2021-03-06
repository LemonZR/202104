# coding=utf-8

def print_fmt(data_list, start=0, end=0):
    """

    :param data_list: 传入的 数据列表 type:[[],()...]
    :param start: 打印的开始列
    :param end: 打印的结束列
    :return: 无返回值(可以返回格式化好的列表)
    """
    data = data_list

    # 局部命名空间
    __var = locals()

    # 存储自定义变量，及其序号
    var_ds = []
    if end == 0 or (bool(data) and end > len(data[0])):
        end = len(data[0])
    if start:
        # 将自然序号变为数据序号
        start = start - 1
    for i in range(start, end):
        # 生成变量名d0,d1,d2... 并保存到var_ds中 :d0 第0列的宽度
        # max(list(map(len, list(map(lambda x: str(x[i]), data))))) 求data列表中所有数组(列表)中第i个元素的最大长度 ，即打印时该列的宽度。
        __var['d' + str(i)] = max(list(map(len, list(map(lambda x: str(x[i]), data)))))
        var_ds.append((i, __var['d' + str(i)]))
    # 将文本格式化为指定宽度的函数
    fm = lambda x: format(x[0], '^%d' % x[1])

    # 将一个列表中的所有数据格式化，并用制表符将数据分隔，处理中文与英文不等长的问题
    fmt = lambda x: '\t| '.join(map(fm, map(lambda x1: (x[x1[0]], x1[1]), var_ds)))

    print('\n' + '*' * 33)
    for i in data:
        i = fmt(i)
        # 这里可以把 格式化后的i 存起来并返回
        print(i)
    print('*' * 33 + '\n')


if __name__ == '__main__':
    data = [
        ['阿萨德啊实打实', 'asdasdasdsd', 'a'],
        ['好好好好阿萨德 ', 'd', 123],
        ('爱迪生撒阿萨德', 'asd', '1')
    ]
    print_fmt(data)

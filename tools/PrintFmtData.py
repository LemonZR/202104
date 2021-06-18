# coding=utf-8

def print_fmt(data_list, start=0, end=0):
    data = data_list
    __var = locals()

    var_ds = []
    if end == 0 or end > len(data[0]):
        end = len(data[0])
    for i in range(start, end):
        __var['d' + str(i)] = max(list(map(len, list(map(lambda x: str(x[i]), data)))))
        var_ds.append(__var['d' + str(i)])
    fm = lambda x: format(x[0], '<%d' % x[1])
    fmt = lambda x: '|'.join(map(fm, map(lambda x1: (x[x1[0]], x1[1]), list(enumerate(var_ds)))))

    print('\n' * 2 + '*' * 33)
    for i in data:
        i = fmt(i)
        print(i)
    print('*' * 33 + '\n' * 2)

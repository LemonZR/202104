def cf(n, y=0.0):
    x = float(y)
    if x ** 2 >= n or len(str(x).split('.')[1])>=18:
        return y

    a = float(str(x).split('.')[0])
    b1 = float(str(x).split('.')[1]) + 1
    b2 = float(str(x).split('.')[1]) + 0.1

    lens = len(str(x).split('.')[1])
    c = 10 ** (-lens)
    x = round(a + b1 * c, lens)
    if x ** 2 > n:
        x = round(a + b2 * c, lens + 1)
    return cf(n, x)


print(cf(2))

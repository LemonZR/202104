i = 0


def cf(n, y=0.0):
    global i
    y = float(y)
    if y ** 2 >= n or len(str(y).split('.')[1]) >= 18:
        return y

    a = float(str(y).split('.')[0])
    b1 = float(str(y).split('.')[1]) + 1
    b2 = float(str(y).split('.')[1]) + 0.1

    lens = len(str(y).split('.')[1])
    c = 10 ** (-lens)
    y = round(a + b1 * c, lens)
    if y ** 2 > n:
        y = round(a + b2 * c, lens + 1)
    i += 1
    print(i)
    print(y)
    return cf(n, y)


if __name__ == '__main__':

    print(223.60679776**2)

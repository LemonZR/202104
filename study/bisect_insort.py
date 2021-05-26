from bisect import bisect, insort
import datetime

def __bisect():
    a = 'qazwsxedcrfvtgb'
    time=datetime.time
    b = sorted(a)

    c = bisect(b, 'p')
    print(c)
    b.insert(c, '-')
    print(b)


def __insort():
    myList = []
    a = 'qazwsxedcrfvtgb'
    for i in a:
        insort(myList, i)
    print(myList)

if __name__ == '__main__':
    __bisect()
    __insort()

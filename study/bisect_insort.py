import get_dep
import re


def assert_test(value):
    print(value)


if __name__ == '__main__':
    assert_test(None)
    from bisect import bisect, insort

    a = 'asddfgfhdfyujjkljkbnti6'
    b = list(a)
    c = sorted(b)

    print(c)

    d = bisect(c, 'g')
    print(d)
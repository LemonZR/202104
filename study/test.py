# for i in range(10):
#     print(f'i--------------:{i}')
#     for j in range(3, 10):
#         if j % 5 == 0:
#             continue
#         print(f'j:{j}')
#

class A:
    def __init__(self):
        self.__a = '1'
        self.b = 2

    def a(self,name):
        a = getattr(self, name)
        print(a)


if __name__ == '__main__':
    a=A()
    a.a('__a')
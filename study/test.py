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

    def a(self, name):
        a = getattr(self, name)
        print(a)


a = {
    # 如果一个表含有多个分区类型字段，优先取key值较小的
    'statis_date': 1,
    'deal_date': 2,
    'day_id': 3,
    'statis_hour': 4
}
b = 'asdasdasd'
c = 'aaaaaaaaa'
if __name__ == '__main__':
    import xlrd.sheet

    q = xlrd.open_workbook('../dustbin/role.xls')
    sheet_n = q.sheet_names()[1]
    sheet = q.sheet_by_name(sheet_n)

    for i in sheet:
        for j in i:
            print(type(j))

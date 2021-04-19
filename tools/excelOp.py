# coding=utf-8

import openpyxl

'''
import openpyxl
# 创建一个工作簿
wb = openpyxl.Workbook()
# 创建一个test_case的sheet表单
wb.create_sheet('test_case')
# 保存为一个xlsx格式的文件
wb.save('cases.xlsx')
# 读取excel中的数据
# 第一步：打开工作簿
wb = openpyxl.load_workbook('cases.xlsx')
# 第二步：选取表单
sh = wb['test_case']
# 第三步：读取数据
# 参数 row:行  column：列
ce = sh.cell(row = 1,column = 1)   # 读取第一行，第一列的数据
print(ce.value)
# 按行读取数据 list(sh.rows)
print(list(sh.rows)[1:])     # 按行读取数据，去掉第一行的表头信息数据

for cases in list(sh.rows)[1:]:
    case_id =  cases[0].value
    case_excepted = cases[1].value
    case_data = cases[2].value
    print(case_excepted,case_data)
# 关闭工作薄
wb.close()
'''


def write_xlsx(filename, data, edit=False, sheet_name='newSheet'):
    if edit:
        try:
            workbook = openpyxl.load_workbook(filename)
            is_sheet = workbook[sheet_name]
            workbook.remove(is_sheet)
            print(f'remove sheet[{sheet_name}] in workbook[{filename}],I\'ll create new one ')
        except FileNotFoundError as fe:
            print(f'{fe},\nI\'ll create a new workbook named {filename}')
            workbook = openpyxl.Workbook()
        except KeyError as ke:
            print(f'{ke},\nI\'ll create a new sheet named [{sheet_name}] in workbook [{filename}]')
        except Exception as e:
            exit(e)
    else:
        if os.path.exists(filename):
            exit(f'Workbook [{filename}] already exists,you can\'t write it.\nPlease set [edit=True],then try again!')
        else:
            workbook = openpyxl.Workbook()

            print(f'Create a new workbook named[{filename}]')
    new_sheet = workbook.create_sheet(sheet_name)
    for row_id, row_data in enumerate(data):
        for col_id, cell_data in enumerate(row_data):
            new_sheet.cell(row_id + 1, col_id + 1, cell_data)
    workbook.save(filename)
    workbook.close()


def read_xlsx(filename, sheet_name='newSheet'):
    try:
        workbook = openpyxl.load_workbook(filename)
        sheet = workbook[sheet_name]
    except FileNotFoundError as fe:

        exit(fe)

    except KeyError as ke:
        exit(ke)

    result_list = []
    for row_data in sheet.rows:
        row_list = []
        for cell in row_data:
            row_list.append(cell.value)
        result_list.append(row_list)
    workbook.close()
    return result_list


if __name__ == '__main__':
    import os

    basedir = os.path.dirname(os.path.dirname(__file__)) + '\\dustbin'
    fileName = f'{basedir}\\test1d.xlsx'
    write_xlsx(fileName, data=[['abc'], ['asdc', 'asd'], 'asdas'], edit=True, sheet_name='b')
    result = read_xlsx(fileName, sheet_name='b')

    for i in result:
        print(i)

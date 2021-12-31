from collections import Counter
import re
import sys
import openpyxl
import pprint


class Report(dict):
    def __init__(self):
        super(Report, self).__init__()

    def __setitem__(self, key, value: dict):
        super(Report, self).__setitem__(key, value)
        self[key].setdefault(0, 0)
        self[key].setdefault(1, 0)
        self[key].setdefault(2, 0)
        self[key].setdefault(3, 0)
        self[key].setdefault(4, 0)
        self[key].setdefault(5, 0)
        self[key].setdefault('总量', 0)
        self[key].setdefault('已完成', 0)
        self[key].setdefault('剩余数量', 0)


class Reporter:
    def __init__(self, **kwargs):
        self.check_date = kwargs.get('check_date', '')
        self.file_path = kwargs.get('file_path', '')
        self.sheet_name = kwargs.get('sheet_name', '')
        # 比对结果列名的匹配规则
        self.info_column_pattern = kwargs.get('info_column_pattern', '找不到')
        # 处理结果列名的匹配规则
        self.result_column_pattern = kwargs.get('result_column_pattern', '找不到')
        # 用于区分模型类别列名的匹配规则
        self.type_column_pattern = kwargs.get('type_column_pattern', '找不到')

        self.excel_data = self.__get_excel_data()
        if len(self.excel_data) >= 2:
            self.tile_data = self.excel_data[1]

            self.info_data = self.excel_data[2:]
        else:
            raise Exception('获取数据异常')
        self.type_column_index, _ = self.__find_type_column()
        self.info_column_index, _ = self.__find_info_column()
        self.result_column_index, _ = self.__find_result_column()
        self.report = Report()

    def __get_excel_data(self, file_path=None, sheet_name=None):
        filepath = file_path if file_path else self.file_path
        sheet_name = sheet_name if sheet_name else self.sheet_name
        try:
            workbook = openpyxl.load_workbook(filepath, data_only=True)
            sheet = workbook[sheet_name]
        except FileNotFoundError as fe:
            sys.exit(fe)

        return list(sheet.rows)  # 是由excel row类型组成的列表

    @staticmethod
    def __find_the_column(row_data, pattern) -> tuple:
        # 对该行每个列元素进行匹配查找，匹配的则记录列序号和列值，不匹配的记录(0,'')
        # find_result : [(0,''),(1,''),(2,'找到了'),(3,'')]
        find_result = list(
            map(
                lambda x: (x[0], re.findall(pattern, str(x[1]))[0])
                if re.findall(pattern, str(x[1])) else (x[0], '')
                , enumerate(cell.value for cell in row_data)
            ))
        # 在众多查找结果中找出最大值，也就是找出匹配结果不是空的那列。
        column = max(find_result, key=lambda x: x[1])
        return column  # (2,'找到了')

    def __find_info_column(self) -> tuple:
        return self.__find_the_column(self.tile_data, self.info_column_pattern)

    def __find_result_column(self) -> tuple:
        return self.__find_the_column(self.tile_data, self.result_column_pattern)

    def __find_type_column(self) -> tuple:
        return self.__find_the_column(self.tile_data, self.type_column_pattern)

    def __get_summary_data(self) -> dict[dict]:
        summary_data = {}

        try:
            for row in self.info_data:
                # row 是excel行对象，row[index] 获取单元格，row[index].value 则是单元格的值
                summary_data.setdefault(
                    row[self.type_column_index].value, {}
                ).setdefault(
                    row[self.info_column_index].value, []
                ).append(str(row[self.result_column_index].value))

            return summary_data
        except Exception as e:
            sys.exit(e)

    def analyze(self):
        print(self.check_date)
        data = self.__get_summary_data()

        for type_str, info_dict in data.items():
            self.report[type_str] = {}
            self.report[type_str]['总量'] = 0
            for status, deal_result in info_dict.items():
                self.report[type_str][status] = {}
                self.report[type_str][status] = len(deal_result)
                print(deal_result)
                if status == 0:
                    self.report[type_str]['已完成'] = Counter(deal_result)['完成']+Counter(deal_result)['10']
                    self.report[type_str]['剩余数量'] = len(deal_result) - self.report[type_str]['已完成']
                self.report[type_str]['总量'] += len(deal_result)
        pprint.pprint(self.report)
        return self.report

    def export_excel(self):
        pass

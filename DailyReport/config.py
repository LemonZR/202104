def init_config(check_date):
    config = {
        'mk_day': dict(
            check_date=check_date,
            file_path=r'D:\bigdata\集中化搬迁\开发区svn文件\集中化数据核对\核对清单\aaa_mk日模型核对情况-整体.xlsx',
            sheet_name='mk模型全量核对清单',
            type_column_pattern=r'^\s*类别\s*$',  # \s* 正则含义是任意个空白符号，包括换行； ^表示开头，$表是结尾
            # 比对结果列 0，1，2，3，4，5
            info_column_pattern=rf'^\s*{check_date}\s*核对结果\s*$',  # 匹配check_date 账期,如“20211201 核对结果”
            # 处理结果列 完成、进行中等
            result_column_pattern='这个在mk日不使用'  # self.result_column_index = self.info_column_index + 3
        ),
        'mk_month': dict(
            check_date=check_date,
            file_path=r'D:\bigdata\集中化搬迁\开发区svn文件\集中化数据核对\核对清单\bbb_mk月模型核对情况-整体.xlsx',
            sheet_name='mk月模型全量核对清单',
            type_column_pattern=r'^\s*类别\s*$',
            info_column_pattern=rf'^\s*m_{check_date}\s*$',  # 匹配check_date 账期
            result_column_pattern=rf'^\s*{check_date}\s*核查结果\s*$'  # 匹配check_date 账期
        ),
        'dis_day': dict(
            check_date=check_date,
            file_path=r'D:\bigdata\集中化搬迁\开发区svn文件\集中化数据核对\核对清单\ccc_dis日表整体核对情况.xlsx',
            sheet_name='dis表整体核对情况',
            type_column_pattern=r'^\s*dis表\s*标识\s*$',  # \s* 正则含义是任意个空白符号，包括换行； ^表示开头，$表是结尾
            info_column_pattern=rf'^\s*d_{check_date}\s*$',  # 匹配check_date 账期，如 d_20211201
            result_column_pattern=r'核对结果'

        ),
        'dis_month': dict(

        )
    }
    return config

# coding=utf-8


key_words = (
    'ALTER', 'BETWEEN', 'BIGINT', 'BOOLEAN', 'BY', 'CUBE', 'CURSOR', 'DATE', 'DECIMAL', 'DELETE', 'DESCRIBE', 'DOUBLE',
    'DROP', 'EXISTS', 'EXTERNAL', 'FLOAT', 'GROUPING', 'IMPORT', 'INSERT', 'INT', 'LOCAL', 'NONE', 'OF', 'OUT',
    'PARTITION',
    'PERCENT', 'PROCEDURE', 'RANGE', 'READS', 'REVOKE', 'ROLLUP', 'ROW', 'ROWS', 'SET', 'SMALLINT', 'TIMESTAMP',
    'TRIGGER',
    'TRUNCATE', 'UPDATE', 'UTC_TMESTAM', 'STATUS')


def run():
    f = open('./data', 'rb')
    for line in f.readlines():
        line = line.decode('utf8')
        if line:
            infos = line.split('\t')
            table_name = infos[0]
            column = infos[1]
            if column.upper() in key_words:
                print(table_name, column)


if __name__ == '__main__':
    run()

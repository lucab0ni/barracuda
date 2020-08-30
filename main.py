import csv
from pathlib import Path
import mysql.connector as mysql
from datetime import datetime


class TOP100:
    NAME = 'name'
    SYMBOL = 'symbol'
    WTD_ALPHA = 'wtd_alpha'
    CURR_RANK = 'curr_rank'
    PREV_RANK = 'prev_rank'
    LAST = 'last'
    CHANGE_VALUE = 'change_value'
    CHANGE_PERCENT = 'change_percent'
    HIGH_52W = 'high_52w'
    LOW_52W = 'low_52w'
    PERCENT_52W = 'percent_52w'
    TIME = 'time'

    ALL = (NAME, SYMBOL, WTD_ALPHA, CURR_RANK, PREV_RANK, LAST, CHANGE_VALUE, CHANGE_PERCENT, HIGH_52W, LOW_52W,
           PERCENT_52W, TIME)


HOST = '192.168.202.128'
DATABASE = 'barracuda'
USER = 'barracuda'
PW = 'barracuda'
TABLE_NAME = 'all_top_100'

CSV_FILE = Path('.data/top-100-stocks-to-buy-08-30-2020.csv').resolve().absolute()

VALUES_TO_QUOTE = [TOP100.SYMBOL, TOP100.NAME, TOP100.TIME]

MAP_DATATYPES = {TOP100.NAME: 'TEXT',
                             TOP100.SYMBOL: 'TEXT',
                             TOP100.WTD_ALPHA: 'FLOAT',
                             TOP100.CURR_RANK: 'INT',
                             TOP100.PREV_RANK: 'INT',
                             TOP100.LAST: 'FLOAT',
                             TOP100.CHANGE_VALUE: 'FLOAT',
                             TOP100.CHANGE_PERCENT: 'FLOAT',
                             TOP100.HIGH_52W: 'FLOAT',
                             TOP100.LOW_52W: 'FLOAT',
                             TOP100.PERCENT_52W: 'INT',
                             TOP100.TIME: 'DATE'}


def adapt_csv_key_to_db_column_name(dataset: dict) -> dict:
    dataset[TOP100.SYMBOL] = dataset.pop('Symbol')
    dataset[TOP100.NAME] = dataset.pop('Name')
    dataset[TOP100.WTD_ALPHA] = dataset.pop('Wtd Alpha')
    dataset[TOP100.CURR_RANK] = dataset.pop('Rank')
    dataset[TOP100.LAST] = dataset.pop('Last')
    dataset[TOP100.PREV_RANK] = dataset.pop('Prev Rank')
    dataset[TOP100.CHANGE_VALUE] = dataset.pop('Change')
    dataset[TOP100.CHANGE_PERCENT] = dataset.pop('%Chg')
    dataset[TOP100.HIGH_52W] = dataset.pop('52W High')
    dataset[TOP100.LOW_52W] = dataset.pop('52W Low')
    dataset[TOP100.PERCENT_52W] = dataset.pop('52W %Chg')
    dataset[TOP100.TIME] = dataset.pop('Time')

    return dataset


def read_csv() -> dict:
    csv_read = {}
    line_count = 0
    with open(str(CSV_FILE), 'r') as file:
        csv_file = csv.DictReader(file)

        for line in csv_file:
            csv_read.update({line_count: adapt_csv_key_to_db_column_name(line)})
            line_count += 1

    return csv_read


def print_all_symbols():
    current_file = read_csv()
    for rank in current_file.keys():
        print(f'{current_file.get(rank).get("Symbol")} - {current_file.get(rank).get("Name")}')


def format_date_to_sql(date_string) -> str:
    return datetime.strptime(date_string, "%m/%d/%y").strftime("%Y-%m-%d")


def format_value_for_query(column, value) -> str:
    value = value.replace(',', '')

    if column == TOP100.TIME:
        value = format_date_to_sql(value)

    if column == TOP100.PREV_RANK and value == '':
        return '0'

    if column in VALUES_TO_QUOTE:
        return f'"{value}"'

    if value.endswith('%'):
        return value[:-1]

    return value


def get_column_names_and_values(row) -> tuple:
    column_str = '('
    value_str = '('

    for column in row.keys():
        column_str += f'{column}, '
        new_value = row.get(column)
        value_str += f'{format_value_for_query(column, new_value)}, '

    column_str = column_str[:-2] + ')'
    value_str = value_str[:-2] + ')'

    return column_str, value_str


def print_all_data():
    current_file = read_csv()

    for row in current_file:
        print(current_file[row].values())


def connect_to_db(user, pw) -> mysql.MySQLConnection:
    return mysql.connect(host=HOST, database=DATABASE, user=user, password=pw)


def table_all_top_100_exists(conn: mysql.MySQLConnection) -> bool:
    cursor = conn.cursor()
    cursor.execute('SHOW TABLES;')
    tables = cursor.fetchall()

    for table in tables:
        if TABLE_NAME in table:
            return True

    return False


def generate_sql_table_column_defines() -> str:
    defines = 'id INT NOT NULL AUTO_INCREMENT, '

    for column in TOP100.ALL:
        defines = f'{defines}{column} {MAP_DATATYPES.get(column)} NOT NULL, '

    defines = f'{defines}PRIMARY KEY (id)'
    return defines


def create_all_top_100_table(conn: mysql.MySQLConnection):
    cursor = conn.cursor()
    sql = f'CREATE TABLE {TABLE_NAME} ({generate_sql_table_column_defines()});'
    cursor.execute(sql)
    conn.commit()


def dataset_already_in_database(conn, date) -> bool:
    cursor = conn.cursor(buffered=True)
    sql = f'SELECT * FROM {TABLE_NAME} WHERE {TOP100.TIME}="{date}"'
    cursor.execute(sql)

    return cursor.fetchone() is not None


def update_table_with_csv_data(conn: mysql.MySQLConnection):
    current_file = read_csv()
    date_of_dataset = format_date_to_sql(current_file.get(0)[TOP100.TIME])

    if dataset_already_in_database(conn, date_of_dataset):
        print('Dataset already in database.')
        return

    cursor = conn.cursor()
    for row in current_file:
        if "Downloaded from" in current_file.get(row)[TOP100.SYMBOL]:
            break

        column_names, values = get_column_names_and_values(current_file[row])
        sql = f'INSERT INTO {TABLE_NAME} {column_names} VALUES {values}'

        cursor.execute(sql)

    conn.commit()

    print(f'Dataset from "{date_of_dataset} was sucessfully added to database.')


def main():
    try:
        conn = connect_to_db(USER, PW)

        if not table_all_top_100_exists(conn):
            print(f'Creating table {TABLE_NAME} in database.')
            create_all_top_100_table(conn)

        update_table_with_csv_data(conn)

    except mysql.errors.InterfaceError as err:
        print(err)
    finally:
        if conn is not None and conn.is_connected():
            conn.close()


if __name__ == "__main__":
    main()

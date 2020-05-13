import mysql.connector
import time
import requests
import csv

API_KEY = '8ATXKFFXNC10W9J8'

config = {
    'user': 'root',
    'password': '',
    'host': '127.0.0.1',
    'database': 'stock_data'
}

def create_table(company):
    table_name = 'realtime_data_' + company
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    # drop_table_sql = "drop table if exists 'real_time_data'"
    drop_table_sql = "DROP TABLE IF EXISTS %s" % table_name
    cursor.execute(drop_table_sql)

    create_table_sql = """
        CREATE TABLE %s (
         id int(11) AUTO_INCREMENT NOT NULL,
         name VARCHAR(30) NOT NULL,
         time DATETIME NOT NULL,
         open FLOAT NOT NULL,
         high FLOAT NOT NULL,
         low FLOAT NOT NULL,
         close FLOAT NOT NULL,
         volume FLOAT NOT NULL,
         PRIMARY KEY (`id`))
    """ % table_name

    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()


def get_data(stock_name):
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=' + \
          stock_name + '&interval=1min&outputsize=full&apikey=' + API_KEY + '&datatype=csv'
    r = requests.get(url)
    return r.text


def write_csv(stock_name, data):
    cvs_name = stock_name + '_realtime.csv'
    with open(cvs_name, 'w') as f:
        f.writelines(data)
    return


def insert_name_to_data(stock_name):
    data = []
    with open(stock_name + '_realtime.csv', 'r') as f:
        next(f)
        reader = csv.reader(f)
        for index, row in enumerate(reader, 1):
            # From 9:30 to 16:00
            if index > 390:
                break
            row.insert(0, stock_name)
            data.append(row)
    return data


def insert_data_to_db(data, company):
    table_name = 'realtime_data_' + company
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    # insert_sql = """
    #                     INSERT INTO real_time_data
    #                     (name, time, open, high, low, close, volume)
    #                     VALUES
    #                     (%s, %s, %s, %s, %s, %s, %s)
    #                  """

    insert_sql = 'INSERT INTO %s' % table_name + \
                 '(name, time, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)'
    cursor.executemany(insert_sql, data)
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    count = 0
    company_list = ["GOOG", "WMT", "MSFT", "IBM", "AMZN", "CSCO", "ORCL", "EBAY", "JPM", "FB"]
    # company_list = ["GOOG", "WMT"]
    for company in company_list:
        create_table(company)
        if count % 5 == 1:
            time.sleep(60)
        print(company)
        data = get_data(company)
        write_csv(company, data)
        # print('data: ', data)
        data_with_name = insert_name_to_data(company)
        # print('data with name: ', data_with_name)
        insert_data_to_db(data_with_name, company)
        time.sleep(10)
        count += 1


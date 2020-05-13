import mysql.connector
import time
import requests
import csv

API_PRE = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&outputsize=full&datatype=csv"
API_KEY = "KJ1ASSL4GE5N2EX7"
API_PREFIX = API_PRE + "&apikey=" + API_KEY


config = {
    'user': 'root',
    'password': '',
    'host': '127.0.0.1',
    'database': 'stock_data'
}


def create_table():
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    # drop_table_sql = "drop table if exists 'real_time_data'"
    drop_table_sql = "DROP TABLE IF EXISTS `history_price`"
    cursor.execute(drop_table_sql)

    create_table_sql = """
        CREATE TABLE history_price (
         id int(11) AUTO_INCREMENT NOT NULL,
         name VARCHAR(30) NOT NULL,
         time DATETIME NOT NULL,
         open FLOAT NOT NULL,
         high FLOAT NOT NULL,
         low FLOAT NOT NULL,
         close FLOAT NOT NULL,
         volume FLOAT NOT NULL,
         PRIMARY KEY (`id`))
    """
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()


def get_data(stock_name):
    apiName = API_PREFIX + "&symbol=" + stock_name
    r = requests.get(apiName)
    return r.text


def write_csv(stock_name, data):
    cvs_name = stock_name + '.csv'
    with open(cvs_name, 'w') as f:
        f.writelines(data)
    return


def insert_name_to_data(stock_name):
    data = []
    with open(stock_name + '.csv', 'r') as f:
        next(f)
        reader = csv.reader(f)
        for index, row in enumerate(reader, 1):
            year = row[0].split('-')[0]
            if year == 2019 or year == 2020 or year == "2019" or year == "2020":
                row.insert(0, stock_name)
                data.append(row)
    return data


def insert_data_to_db(data):
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    insert_sql = """
                        INSERT INTO history_price
                        (name, time, open, high, low, close, volume)
                        VALUES
                        (%s, %s, %s, %s, %s, %s, %s)
                     """
    cursor.executemany(insert_sql, data)
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    count = 0
    company_list = ["GOOG", "WMT", "MSFT", "IBM", "AMZN", "CSCO", "ORCL", "EBAY", "JPM", "FB"]
    create_table()
    for company in company_list:
        if count % 5 == 1:
            time.sleep(60)
        print(company)
        data = get_data(company)
        write_csv(company, data)
        data_with_name = insert_name_to_data(company)
        insert_data_to_db(data_with_name)
        time.sleep(10)
        count += 1





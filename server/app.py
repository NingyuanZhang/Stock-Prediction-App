import os
from flask import Flask, render_template, request
from flask_cors import *
from flasgger import swag_from, Swagger
import mysql.connector
import json
import datetime
# import operations
# import operations_realtime
import indicators
# import task
from predict import ANN, SVM, Bayes


curPath = os.path.abspath(os.path.dirname(__file__))

base_dir = os.path.abspath('../public')
app = Flask(__name__, template_folder=base_dir, static_folder=base_dir, static_url_path="")
CORS(app, supports_credentials=True)
config = {
    'user': 'root',
    'password': '',
    'host': 'localhost',
    'database': 'stock_data',
}
swagger = Swagger(app)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/historical-stock-data/<company>', methods=['GET', 'PUT'])
def historicalData(company):
    if request.method == 'GET':
        tableName = "historical_data_" + company
        startDate = request.args.get('from')
        endDate = request.args.get('to')
        print('start date and end date: ',startDate, endDate)
        if startDate == '' or endDate =='':
            db = mysql.connector.connect(**config)
            cursor = db.cursor()
            sql = "SELECT id, DATE_FORMAT(time, '%Y-%m-%d')  AS time, open, high, low, close,volume FROM " + tableName
            cursor.execute(sql)
            records = cursor.fetchall()
            cursor.close()
            db.close()
            return json.dumps(records)
        else:
            start = datetime.datetime.strptime(startDate, '%Y-%m-%d')
            end = datetime.datetime.strptime(endDate, '%Y-%m-%d')
            indStart = start - datetime.timedelta(days=90)
            db = mysql.connector.connect(**config)
            cursor = db.cursor()

            # count number of rows for Start
            sql = "SELECT COUNT(*) as cnt " + \
                  "FROM %s " % (tableName) + \
                  "WHERE DATE(time) BETWEEN DATE('%s') AND DATE('%s') order by time asc" % (start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))
            cursor.execute(sql)
            daysDiff = cursor.fetchall()[0][0]

            sql = "SELECT DATE_FORMAT(time, '%Y-%m-%d')  AS time, close " + \
                  "FROM %s " % (tableName) + \
                  "WHERE DATE(time) BETWEEN DATE('%s') AND DATE('%s') order by time asc" % (indStart.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))
            cursor.execute(sql)
            records = cursor.fetchall()
            cursor.close()
            db.close()

            dates = [record[0] for record in records]
            prices = [record[1] for record in records]
            macd = indicators.macd(prices)
            rsi = indicators.rsiFunc(prices)
            movingAvgShort = indicators.moving_avg(prices, period=5)
            movingAvgLong = indicators.moving_avg(prices, period=50)

            dates = dates[-daysDiff:]
            prices = prices[-daysDiff:]
            macd = macd[-daysDiff:]
            rsi = rsi[-daysDiff:]
            movingAvgShort = movingAvgShort[-daysDiff:]
            movingAvgLong = movingAvgLong[-daysDiff:]

            stockData = {"dates": dates,
                         "prices": prices,
                         "macd": macd,
                         "rsi": rsi,
                         "movingAvgShort": movingAvgShort,
                         "movingAvgLong": movingAvgLong}

            return json.dumps(stockData)

    # if request.method == 'PUT':
    #     operations.getDataFromApiAndWriteToDisk(curPath + '/data', company)
    #     operations.writeToDB(curPath + '/data', company)
    #     return "sucess"



@app.route('/realtime-stock-data/<company>', methods=['GET', 'PUT'])
def realtimeData(company):
    if request.method == 'GET':
        tableName = "realtime_data_" + company
        print('time: ',(datetime.datetime.now().strftime("%Y-%m-%d")))
        if len(request.args) == 0:
            db = mysql.connector.connect(**config)
            cursor = db.cursor()
            sql = "SELECT DATE_FORMAT(time, '%H:%i')  AS time, close " + \
                  "FROM %s " % (tableName) + \
                  "WHERE DATE(time) = '%s' " % (datetime.datetime.now().strftime("%Y-%m-%d")) + \
                  "order by time asc"
            cursor.execute(sql)
            records = cursor.fetchall()
            cursor.close()
            db.close()
            times = [record[0] for record in records]
            prices = [record[1] for record in records]
            stockData = {"times": times,
                         "prices": prices}
            # print(stockData)
            return json.dumps(stockData)

    # if request.method == 'PUT':
    #     operations_realtime.getDataFromApiAndWriteToDisk(curPath + '/data', company)
    #     operations_realtime.writeToDB(curPath + '/data', company)
    #     return "sucess"


@app.route('/latest-price/', methods=['GET'])
#@swag_from('apidoc/latest_price.yml', methods=['GET'])
def get_latest_price():
    # task.timed_task()
    if request.method == 'GET':
        db = mysql.connector.connect(**config)
        cursor = db.cursor()
        sql = """
            select name, DATE_FORMAT(time, '%Y-%m-%d'), close, volume
            from latest_price 
            where (name, time) in 
            (select name, max(time) from latest_price group by name);
        """
        cursor.execute(sql)
        records = cursor.fetchall()
        cursor.close()
        db.close()
        items = []
        for i in range(len(records)):
            items.append(dict(name=records[i][0], time=records[i][1],
                              price=records[i][2], volume=records[i][3]))
        items = dict(data=items)
        return json.dumps(items)


@app.route('/high-price/<company>', methods=['GET'])
def get_highest_price(company):
    if request.method == 'GET':
        tableName = "historical_data_" + company
        db = mysql.connector.connect(**config)
        cursor = db.cursor()
        sql = "SELECT t.name, DATE_FORMAT(t.time, '%Y-%m-%d') AS time, t.close " \
              "FROM " + tableName + " t " \
                                    "JOIN " \
                                    "(SELECT Name, MAX(close) maxVal " \
                                    "FROM " + tableName + \
              " WHERE DATE_SUB(CURDATE(), INTERVAL 10 DAY) < DATE(time) " \
              "GROUP BY Name)" + \
              "t2 ON t.close = t2.maxVal AND t.name = t2.name"
        cursor.execute(sql)
        records = cursor.fetchall()
        cursor.close()
        db.close()
        items = [{'name': records[0][0], 'time': records[0][1], 'price': records[0][2]}]
        items = dict(data=items)
        return json.dumps(items)


@app.route('/avg-price/<company>', methods=['GET'])
def get_avg_price(company):
    if request.method == 'GET':
        tableName = "historical_data_" + company
        db = mysql.connector.connect(**config)
        cursor = db.cursor()
        sql = "SELECT name, avg(close) as avg_price " \
              "FROM " + tableName + " WHERE DATE_SUB(CURDATE(), INTERVAL 1 YEAR) < DATE(time) GROUP BY name"
        cursor.execute(sql)
        records = cursor.fetchall()
        cursor.close()
        db.close()
        items = [{'name': records[0][0], 'price': records[0][1]}]
        items = dict(data=items)
        return json.dumps(items)


@app.route('/low-price/<company>', methods=['GET'])
def get_lowest_price(company):
    if request.method == 'GET':
        tableName = "historical_data_" + company
        db = mysql.connector.connect(**config)
        cursor = db.cursor()
        sql = "SELECT t.name, DATE_FORMAT(t.time, '%Y-%m-%d') AS time, t.close " \
              "FROM " + tableName + " t " \
                                    "JOIN " \
                                    "(SELECT Name, MIN(close) minVal " \
                                    "FROM " + tableName + \
              " WHERE DATE_SUB(CURDATE(), INTERVAL 1 YEAR) < DATE(time) " \
              "GROUP BY Name)" + \
              "t2 ON t.close = t2.minVal AND t.name = t2.name"
        cursor.execute(sql)
        records = cursor.fetchall()
        cursor.close()
        db.close()
        items = [{'name': records[0][0], 'time': records[0][1], 'price': records[0][2]}]
        items = dict(data=items)
        return json.dumps(items)


@app.route('/list-company/<company>', methods=['GET'])
def get_company(company):
    if request.method == 'GET':
        db = mysql.connector.connect(**config)
        cursor = db.cursor()

        sql = "SELECT sc.id, sc.cmp_name, AVG(sd.close) " \
              "FROM history_price AS sd, stock_company AS sc " \
              "WHERE sc.name = sd.name " \
              "GROUP BY sc.id " \
              "HAVING AVG(sd.close) < (select MIN(close) from history_price " \
              "WHERE name= " + repr(company) + \
              " AND DATE_SUB(CURDATE(), INTERVAL 1 YEAR) < DATE(time))"
              

        cursor.execute(sql)
        records = cursor.fetchall()
        cursor.close()
        db.close()
        items = []
        for i in range(len(records)):
            items.append(dict(id=records[i][0], name=records[i][1], price=records[i][2]))
        items = dict(data=items)
        return json.dumps(items)


@app.route('/predict/<company>/<term>', methods=['GET'])
def predict(company, term):
    items = list()
    if request.method == 'GET':
        if term == 'short':
            print('\n\nI am here in short predict')
            bayes_price = Bayes.get_next_day_price(company)
            bayes_price = str(bayes_price[0]).replace('[', '').replace(']', '')
            items.append(dict(algorithm='Bayes', price=bayes_price))

            ann_price = ANN.get_next_day_price(company)
            print('the price', ann_price)
            ann_price = str(ann_price[0]).replace('[', '').replace(']', '')
            items.append(dict(algorithm='ANN', price=ann_price))

        else:
            svm_price = SVM.get_next_day_price(company)
            svm_price = str(svm_price[0]).replace('[', '').replace(']', '')
            items.append(dict(algorithm='SVM', price=svm_price))

        items = dict(data=items)
        return json.dumps(items)


if __name__ == '__main__':
    app.run(debug=True)

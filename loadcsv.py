import csv
import pymysql
from datetime import datetime

mydb = pymysql.connect(host='localhost', user='root', passwd='password', db='test')
cursor = mydb.cursor()
query = "DROP TABLE IF EXISTS CarSales;"
cursor.execute(query)
query = "CREATE TABLE CarSales(Year date, Brand varchar(32), Category varchar(32), Sales int, INDEX (Year,Brand,Category,Sales))"
cursor.execute(query)
with open('./data/CarSales.csv', 'r', encoding="utf8") as csvfile:
    csv_data = csv.reader(csvfile, delimiter=',')

    for i, row in enumerate(csv_data):
        if i == 0:
            continue
        cursor.execute('INSERT INTO CarSales(Year, Brand, Category, Sales)' 'VALUES(%s, %s, %s, %s)', (datetime.strptime(row[0],"%Y"), row[1], row[2], row[3]))

mydb.commit()
cursor.close()
print("Imported!")
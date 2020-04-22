from fact2sql import fact2sql
import pymysql

with open('./factjson/proportion.json', 'r') as file:
    jsondata = file.read().replace('\n', '')

sql = fact2sql(jsondata, 'CarSales')
print(sql)

mydb = pymysql.connect(host='localhost', user='root', passwd='password', db='test')
cursor = mydb.cursor()
cursor.execute(sql)
rows = cursor.fetchall()
print(rows)
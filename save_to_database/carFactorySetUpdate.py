# coding:utf-8
from pymongo import MongoClient
import pymysql
from publicMethods import loadCarTypeSet

def loadFactoryData():
    conn = pymysql.connect(host='47.99.116.136',user='root',passwd='3H1passwd',port=3306,db='car_test',charset='utf8')
    cursor=conn.cursor()
    sql_sentence='select factory_id,factory_name from factory'
    cursor.execute(sql_sentence)
    results = cursor.fetchall()
    carFactorySet={}
    for r in results:
        carFactorySet[r[1]]=r[0]
    return carFactorySet

def loadCarTypeSet():
    conn = pymysql.connect(host='47.99.116.136',user='root',passwd='3H1passwd',port=3306,db='car_test',charset='utf8')
    cursor=conn.cursor()
    sql_sentence='select value,label from car_type'
    cursor.execute(sql_sentence)
    results = cursor.fetchall()
    carTypeSet={}
    for r in results:  
        carTypeSet[r[1]]=r[0]
    return carTypeSet

def loadCarDataSet():
    conn = pymysql.connect(host='47.99.116.136',user='root',passwd='3H1passwd',port=3306,db='car_test',charset='utf8')
    cursor=conn.cursor()
    sql_sentence='select car_id,car_name from car'
    cursor.execute(sql_sentence)
    results = cursor.fetchall()
    carDataSet={}
    for r in results:
        carDataSet[r[1]]=r[0]
    return carDataSet


#连接MongoDB数据库
client=MongoClient('47.92.211.251',30000)
collection=client.car_test.model
print(collection.find().count())

#连接mysql数据库并导入数据
conn = pymysql.connect(host='47.99.116.136',user='root',passwd='3H1passwd',port=3306,db='car_test',charset='utf8')
cursor=conn.cursor()
sql_factory='insert into factory(factory_id,factory_name) values (%s,%s)'
sql_car='insert into car(car_id,factory_id,car_name,car_type) values (%s,%s,%s,%s)'

carFactorySet=loadFactoryData()
carTypeSet=loadCarTypeSet()
carDataSet=loadCarDataSet()
factory_id=max(list(carFactorySet.values()))+1
car_id=max(list(carDataSet.values()))+1
for data in collection.find({},{"carBrand":1,"modelName":1,"modelType":1}):
    carType=carTypeSet[data['modelType']]
    carName=data['modelName']
    factoryName=data['carBrand']
    if factoryName not in carFactorySet.keys():
        carFactorySet[factoryName]=factory_id
        cursor.execute(sql_factory,(factory_id,factoryName))
        factory_id+=1
    factory=carFactorySet[factoryName]
    if carName not in carDataSet.keys():
        carDataSet[carName]=car_id
        cursor.execute(sql_car,(car_id,factory,carName,carType))
        car_id+=1

cursor.close()                          # 关闭游标
conn.commit()                           #向数据库插入一条数据时必须要有这个方法，否则数据不会被真正的插入
conn.close() 

# coding:utf-8
from pymongo import MongoClient
import pymysql
from publicMethods import loadCarTypeSet

#连接MongoDB数据库
client=MongoClient('47.92.211.251',30000)
collection=client.car.model
print(collection.find().count())
carFactorySet=list(collection.find({},{"carBrand":1,"modelName":1,"modelType":1}))
#初始化一些id
carTypeSet=loadCarTypeSet()
carSet=[]
factorySet={}

car_id=1
factory_id=1
#处理数据并生成factory表和car表
for data in carFactorySet:
    carType=carTypeSet[data['modelType']]
    carName=data['modelName']
    factoryName=data['carBrand']

    if factoryName not in factorySet.keys():
        factorySet[factoryName]=factory_id
        factory_id+=1
    factory=factorySet[factoryName]
    
    carSet.append([car_id,factory,carName,carType])
    car_id+=1
#连接mysql数据库并导入数据
conn = pymysql.connect(host='47.99.116.136',user='root',passwd='3H1passwd',port=3306,db='car_test',charset='utf8')
cursor=conn.cursor()

sql_factory='insert into factory(factory_id,factory_name) values (%s,%s)'
sql_car='insert into car(car_id,factory_id,car_name,car_type) values (%s,%s,%s,%s)'

for (k,d) in factorySet.items():
    cursor.execute(sql_factory,(d,k))

for car in carSet:
    cursor.execute(sql_car,(car[0],car[1],car[2],car[3]))

cursor.close()                          # 关闭游标
conn.commit()                           #向数据库插入一条数据时必须要有这个方法，否则数据不会被真正的插入
conn.close() 

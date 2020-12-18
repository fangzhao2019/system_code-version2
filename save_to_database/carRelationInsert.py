# coding:utf-8
from pymongo import MongoClient
import pymysql
import numpy as np
from publicMethods import *

#载入汽车之家数据，并计算每辆车的提及数量以及构建共同提及矩阵
def loadCarCommentSet(carSet):
    client=MongoClient('47.92.211.251',30000)
    collection=client.new_carDataset.test
    totalCount=collection.find().count()
    print('一共包含数据%s条'%totalCount)
    
    #初始化数据并计算统计汽车提及和汽车共同提及
    carCommentSet=[]
    carCount={}
    for car in carSet:
        carCount[car]=0
    carOccurrenceMat=np.zeros((len(carSet),len(carSet)))
    #处理数据
    n=0
    for row in collection.find({},{"car":1,"formatMentionCar":1}):
        n+=1
        if n%1000==0:print(n)
        #if n==5000:break
        buyCar=row['car']
        if 'formatMentionCar' in row.keys():
            mentionCarSet=list(row['formatMentionCar'].keys())
            mentionCarSet.append(buyCar)
            mentionCarSet=list(set(mentionCarSet))
            #如果没有提及车型，就不将其加入该数据集中
            carComment={'buyCar':buyCar,'mentionCar':mentionCarSet}
            carCommentSet.append(carComment)
        else:
            mentionCarSet=[buyCar]
        #计算提及车型数量
        for mentionCar in mentionCarSet:
            if mentionCar in carCount.keys():
                carCount[mentionCar]+=1
        #计算车型共同提及数量
        for mentionCar1 in mentionCarSet:
            x=readCarID(mentionCar1,carSet)
            if x==-1:continue
            for mentionCar2 in mentionCarSet:
                if mentionCar2==mentionCar1:continue
                y=readCarID(mentionCar2,carSet)
                if y==-1:continue
                carOccurrenceMat[x,y]+=1
    return carCommentSet,carCount,carOccurrenceMat,totalCount

#给定两个车型，计算他们的共同潜在市场占比
def calPotentialMarket(car1,car2,carCommentSet):
    potenitialMarket={}
    for carComment in carCommentSet:
        if not car1 in carComment['mentionCar']:continue
        if not car2 in carComment['mentionCar']:continue
        buyCar=carComment['buyCar']
        if not buyCar in potenitialMarket.keys():
            potenitialMarket[buyCar]=0
        potenitialMarket[buyCar]+=1
    return potenitialMarket   

#存储单个车型的数据
def insert_into_car_market(carSet,carDataSet,carCount):
    print('正在导入单个车型的数据')
    conn = pymysql.connect(host='47.99.116.136',user='root',passwd='3H1passwd',port=3306,db='car_test',charset='utf8')
    cursor=conn.cursor()
    sql_one='insert into car_market(id,car_id,num) values (%s,%s,%s)'
    one_id=1
    for i in range(len(carSet)):
        car1=carSet[i]
        car_id1=carDataSet[car1]['car_id']
        num1=carCount[car1]
        #存储单个车型关注量，若为0则不存
        if num1==0:continue
        try:
            cursor.execute(sql_one,(one_id,car_id1,num1))
            one_id+=1
        except Exception as e:
            print(e)
    cursor.close()                          # 关闭游标
    conn.commit()                           #向数据库插入一条数据时必须要有这个方法，否则数据不会被真正的插入
    conn.close()

#存储共同关注量的数据
def insert_into_car_market_two_id(carSet,carDataSet,carCount,carOccurrenceMat):
    print('正在导入共同关注的数据')
    conn = pymysql.connect(host='47.99.116.136',user='root',passwd='3H1passwd',port=3306,db='car_test',charset='utf8')
    cursor=conn.cursor()
    sql_two='insert into car_market_two_id(id,car_id1,car_id2,num) values (%s,%s,%s,%s)'
    two_id=1
    for i in range(len(carSet)):
        if two_id%10000==0:print(two_id)
        car1=carSet[i]
        #print('正在导入%s的数据'%car1)
        car_id1=carDataSet[car1]['car_id']
        num1=carCount[car1]
        if num1==0:continue
        for j in range(len(carSet)):
            car2=carSet[j]
            car_id2=carDataSet[car2]['car_id']
            num2=int(carOccurrenceMat[i,j])
            #存储两个车型的共同关注量，若为0则不存
            if car1==car2:continue
            if num2==0:continue
            try:
                cursor.execute(sql_two,(two_id,car_id1,car_id2,num2))
                two_id+=1
            except Exception as e:
                print(e)        
    cursor.close()                          # 关闭游标
    conn.commit()                           #向数据库插入一条数据时必须要有这个方法，否则数据不会被真正的插入
    conn.close()

#存储潜在市场占比的数据
def insert_into_car_market_three_id(carSet,carDataSet,carCount,carOccurrenceMat,carCommentSet):
    print('正在导入潜在市场的数据')
    conn = pymysql.connect(host='47.99.116.136',user='root',passwd='3H1passwd',port=3306,db='car_test',charset='utf8')
    cursor=conn.cursor()
    sql_three='insert into car_market_three_id(id,car_id1,car_id2,car_id3,num) values (%s,%s,%s,%s,%s)'
    three_id=1
    for i in range(len(carSet)):
        if three_id%10000==0:
            print(three_id)
            conn.commit()
        car1=carSet[i]
        car_id1=carDataSet[car1]['car_id']
        num1=carCount[car1]
        if num1==0:continue
        for j in range(len(carSet)):
            car2=carSet[j]
            car_id2=carDataSet[car2]['car_id']
            num2=int(carOccurrenceMat[i,j])
            if car1==car2:continue
            if num2==0:continue
            potenitialMarket=calPotentialMarket(car1,car2,carCommentSet)
            for car3 in potenitialMarket.keys():
                if not car3 in carDataSet.keys():continue
                car_id3=carDataSet[car3]['car_id']
                num3=potenitialMarket[car3]
                try:
                    cursor.execute(sql_three,(three_id,car_id1,car_id2,car_id3,num3))
                    three_id+=1
                except Exception as e:
                    print(e)
    cursor.close()                          # 关闭游标
    conn.commit()                           #向数据库插入一条数据时必须要有这个方法，否则数据不会被真正的插入
    conn.close()

#存储竞争关系的数据——按照车类别进行区分
def insert_into_car_contend_value(carSet,carDataSet,carTypeSet,carCount,carOccurrenceMat,totalCount):
    print('正在导入竞争关系')
    conn = pymysql.connect(host='47.99.116.136',user='root',passwd='3H1passwd',port=3306,db='car_test',charset='utf8')
    cursor=conn.cursor()
    sql_relation='insert into car_contend_value(id,car_type_id,car_id,associated_id,value) values (%s,%s,%s,%s,%s)'
    _id=1
    for value in carTypeSet.values():
        carSubDataSet=splitCarDataSet(carDataSet,value)
        for car1 in carSubDataSet.keys():
            if carCount[car1]==0:continue
            for car2 in carSubDataSet.keys():
                if car1==car2:continue
                car_id1=carDataSet[car1]['car_id']
                car_id2=carDataSet[car2]['car_id']
                x=carSet.index(car1)
                y=carSet.index(car2)
                freq_x=carCount[car1]
                freq_y=carCount[car2]
                freq_xy=int(carOccurrenceMat[x,y])
                N=totalCount
                if freq_xy==0:continue
                carRelation=float(PMI_measure2(freq_x,freq_y,freq_xy,N))
                #print(_id,value,car_id1,car_id2,carRelation)
                try:
                    cursor.execute(sql_relation,(_id,value,car_id1,car_id2,carRelation))
                    if _id%1000==0:print(_id)
                    _id+=1
                except Exception as e:
                    print(e)
    cursor.close()                          # 关闭游标
    conn.commit()                           #向数据库插入一条数据时必须要有这个方法，否则数据不会被真正的插入
    conn.close()
    
carDataSet=loadCarDataSet()
carTypeSet=loadCarTypeSet()
carSet=list(carDataSet.keys())
carCommentSet,carCount,carOccurrenceMat,totalCount=loadCarCommentSet(carSet)
#insert_into_car_market(carSet,carDataSet,carCount)
#insert_into_car_market_two_id(carSet,carDataSet,carCount,carOccurrenceMat)
insert_into_car_market_three_id(carSet,carDataSet,carCount,carOccurrenceMat,carCommentSet)
#insert_into_car_contend_value(carSet,carDataSet,carTypeSet,carCount,carOccurrenceMat,totalCount)

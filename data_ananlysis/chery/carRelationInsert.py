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

def loadEffectiveCar(carSet,carDataSet,carOccurrenceMat):
    effectiveCar=['艾瑞泽5','艾瑞泽7','瑞虎7','瑞虎8','艾瑞泽GX']
    for car1 in carSet:
        if car1 in ['艾瑞泽5','艾瑞泽7','瑞虎7','瑞虎8','艾瑞泽GX']:
            x=carSet.index(car1)
            for y in range(len(carOccurrenceMat)):
                if carOccurrenceMat[x,y]>0:
                    car2=carSet[y]
                    effectiveCar.append(car2)
    return list(set(effectiveCar))

def loadEffectiveCar2(carSet,carDataSet,carOccurrenceMat):
    effectiveCar=['艾瑞泽5','艾瑞泽7','瑞虎7','瑞虎8','艾瑞泽GX']
    for i in range(100):
        aux=[]
        for car1 in effectiveCar:
            x=carSet.index(car1)
            for y in range(len(carOccurrenceMat[x])):
                if carOccurrenceMat[x,y]>0:
                    car2=carSet[y]
                    aux.append(car2)
        effectiveCar=list(set(set(effectiveCar)|set(aux)))
    return effectiveCar

#存储竞争关系的数据——按照车类别进行区分
def insert_into_car_contend_value(carSet,carDataSet,carTypeSet,carCount,carOccurrenceMat,totalCount):
    print('正在导入竞争关系')
    effectiveCar=loadEffectiveCar(carSet,carDataSet,carOccurrenceMat)
    print('一共包含有效车型%d个'%(len(effectiveCar)))
    conn = pymysql.connect(host='47.99.116.136',user='root',passwd='3H1passwd',port=3306,db='car_test',charset='utf8')
    cursor=conn.cursor()
    sql_relation='insert into car_contend_value(id,car_type_id,car_id,associated_id,value) values (%s,%s,%s,%s,%s)'
    _id=1
    for value in carTypeSet.values():
        carSubDataSet=splitCarDataSet(carDataSet,value)
        carKeySet=list(carSubDataSet.keys())
        for i in range(len(carKeySet)):
            car1=carKeySet[i]
            if carCount[car1]==0:continue
            for j in range(i+1,len(carKeySet)):
                car2=carKeySet[j]
                if carCount[car2]==0:continue
                if car1==car2:continue
                if not car1 in effectiveCar:
                    continue
                if not car2 in effectiveCar:
                    continue
                car_id1=carDataSet[car1]['car_id']
                car_id2=carDataSet[car2]['car_id']
                x=carSet.index(car1)
                y=carSet.index(car2)
                freq_x=carCount[car1]
                freq_y=carCount[car2]
                freq_xy=int(carOccurrenceMat[x,y])
                N=totalCount
                if freq_xy==0:continue
                carRelation=float(PMI_measure2(freq_x,freq_y,freq_xy,N)+2.5)
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
insert_into_car_contend_value(carSet,carDataSet,carTypeSet,carCount,carOccurrenceMat,totalCount)
#effectiveCar=loadEffectiveCar2(caeSet,carDataSet,carOccurrenceMat)

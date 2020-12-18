# coding:utf-8
import pymysql
import re

#获取所有车型数据以及对应的车类别
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
    sql_sentence='select car_id,car_name,car_type from car'
    cursor.execute(sql_sentence)
    results = cursor.fetchall()
    carDataSet={}
    for r in results:
        carData={}
        carData['car_id']=r[0]
        carData['type_id']=r[2]
        carDataSet[r[1]]=carData
    return carDataSet

#格式化数据来源
def sourceProcess(mongoUrl):
    if 'bitauto' in mongoUrl:
        return '易车网'
    if 'autohome' in mongoUrl:
        return '汽车之家'
    if 'pcauto' in mongoUrl:
        return '太平洋汽车网'
    return ''

#格式化购买地点
def placeProcess(place):
    special_city=['北京市','天津市','上海市','重庆市','香港市','澳门市']
    location=place.split(' ')
    province=location[0]
    for sc in special_city:
        if province==sc:
            city=sc
            return province,city
    city=location[1]
    return province,city

#根据车类别划分汽车数据子集
def splitCarDataSet(carDataSet,value):
    carSubDataSet={}
    for car in carDataSet.keys():
        if carDataSet[car]['type_id']==value:
            carSubDataSet[car]=carDataSet[car]
    return carSubDataSet

#获取某一辆车的序号
def readCarID(car,carSet):
    try:
        return carSet.index(car)
    except:
        return -1

#对数据集进行切片，dataSet为二维列表，axis为某一列，value为那一列下对应的值
def splitDataSet(dataSet, axis, value):
    retDataSet = []
    for featVec in dataSet:
        if featVec[axis] == value:
            reducedFeatVec = featVec[:axis]
            reducedFeatVec.extend(featVec[axis+1:])
            retDataSet.append(reducedFeatVec)
    return retDataSet#返回划分结果数据集


 

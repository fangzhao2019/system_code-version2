# coding:utf-8
from numpy import log
import pymysql
import re
#用于计算竞争关系的辅助矩阵，这里p值取0.05，即卡方值要达到3.84
#           car1    no car1
#   car2    a       b
#no car2    d       c
def chi_square_test1(a,b,c,d):
    N=a+b+c+d
    try:
        return N*(a*c-b*d)/float((a+b)*(b+c)*(c+d)*(a+d))
    except:
        return 0
def chi_square_test2(px,py,pxy,N):
    a=pxy
    b=py-pxy
    c=N-px-py+pxy
    d=px-pxy
    chi_square_value=chi_square_test1(a,b,c,d)
    return chi_square_value 
def PMI_measure1(a,b,d,N):
    return log(N*a/float((a+d)*(a+b)))
def PMI_measure2(px,py,pxy,N):
    a=pxy
    b=py-pxy
    d=px-pxy
    PMI_value=PMI_measure1(a,b,d,N)
    return PMI_value

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

def loadFineGrainedFeature():
    conn = pymysql.connect(host='47.99.116.136',user='root',passwd='3H1passwd',port=3306,db='car_test',charset='utf8')
    cursor=conn.cursor()
    sql_sentence='select fine_grained_feature_id,fine_grained_feature_name from fine_grained_featureset'
    cursor.execute(sql_sentence)
    results = cursor.fetchall()
    fineGrainedFeature={}
    for r in results:
        fineGrainedFeature[r[1]]=r[0]
    return fineGrainedFeature

def loadDataSource():
    conn = pymysql.connect(host='47.99.116.136',user='root',passwd='3H1passwd',port=3306,db='car_test',charset='utf8')
    cursor=conn.cursor()
    sql_sentence='select id,source_name from data_sources'
    cursor.execute(sql_sentence)
    results = cursor.fetchall()
    dataSource={}
    for r in results:
        dataSource[r[1]]=r[0]
    return dataSource

def loadArticleIdSet():
    conn = pymysql.connect(host='47.99.116.136',user='root',passwd='3H1passwd',port=3306,db='car_test',charset='utf8')
    cursor=conn.cursor()
    sql_sentence='select article_id from article'
    cursor.execute(sql_sentence)
    results = cursor.fetchall()
    articleIdSet=[]
    for r in results:
        articleIdSet.append(r[0])
    return articleIdSet

#根据车类别划分汽车数据子集
def splitCarDataSet(carDataSet,value):
    carSubDataSet={}
    for car in carDataSet.keys():
        if carDataSet[car]['type_id']==value:
            carSubDataSet[car]=carDataSet[car]
    return carSubDataSet

def dataSourceProcess(dataSource):
    if dataSource=='汽车之家':return 1
    if dataSource=='太平洋汽车网':return 2
    if dataSource=='易车网':return 3
    return ''

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


 

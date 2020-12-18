# coding:utf-8
import opinionMining
import pymysql
from pymongo import MongoClient
import jieba
jieba.load_userdict('E:/博二/奇瑞项目/项目代码/userdict.txt')
import re
import time
from publicMethods import *

def getKey(dic,value):
    for k,v in dic.items():
        if v==value:
            return k

def mentionCarProcess(comment,featureSet,carDataSet):
    clauseList=opinionMining.clauseSegmentation(comment)
    mentionCarSet=[]
    for clause in clauseList:
        clauseWordSet=[w for w in jieba.cut(clause)]
        for car in carDataSet.keys():
            if car in clauseWordSet:
                for feature in featureSet:
                    if feature in clauseWordSet:
                        result=[car,feature,clause]
                        mentionCarSet.append(result)
    formatMentionCarSet=opinionMining.formatDataSet(mentionCarSet)
    return formatMentionCarSet

featureSet,senwordSet,degwordSet,notwordSet=opinionMining.load_feature_senword_dataSet('E:/博二/奇瑞项目/项目代码/产品特征-情感词_数据集.xlsx')
carDataSet=loadCarDataSet()
carTypeSet=loadCarTypeSet()
reg=re.compile(' 20\d\d')

client=MongoClient('47.92.211.251',30000)
collection1=client.car_test.comment
collection2=client.new_carDataset.test
print('一共包含数据%s条'%collection1.find().count())

n=1
for row in collection1.find({},{"url":1,"status":1,"data":1}):
    if n%1000==0:print(n)
    #if n==20000:break
    #分别对每条原始数据进行处理，得到中间数据
    new_mongoData={}
    #数据来源
    new_mongoData['dataSource']=sourceProcess(row['url'])
    #地理位置
    try:
        province,city=placeProcess(row["data"]["购买地点"])
    except:
        province,city='',''
    new_mongoData['province']=province
    new_mongoData['city']=city
    #购买车型&车类别
    try:
        car_specific=row['data']['购买车型']
        car=re.split(reg,car_specific)[0]
        new_mongoData['car_specific']=car_specific
        new_mongoData['car']=car
        type_id=carDataSet[car]['type_id']
        car_type=getKey(carTypeSet,type_id)
        new_mongoData['car_type']=car_type
    except Exception as e:
        print(e)
        continue
    #购买时间
    try:
        new_mongoData['time']=row['data']['购买时间']
    except:
        new_mongoData['time']=''
    #购买价格
    try:
        new_mongoData['price']=row['data']['裸车价格']
    except:
        new_mongoData['price']=''
    #评分
    new_mongoData['score']=row['data']['评分']
    #评论
    new_commentSet={}
    commentTotal=''#整合全部评论
    commentSet=row['data']['评价']
    whole_featureSet=list(commentSet.keys())
    for i in range(len(whole_featureSet)):
        whole_feature=whole_featureSet[i]
        comment=commentSet[whole_feature]
        commentTotal+=comment
        formatOpinionSet=opinionMining.opinionMining(comment,featureSet,senwordSet,degwordSet,notwordSet)
        new_commentSet[whole_feature]=formatOpinionSet
    new_mongoData['formatOpinionSet']=new_commentSet
    #提及车型
    formatMentionCar=mentionCarProcess(commentTotal,featureSet,carDataSet)
    if len(list(formatMentionCar))>0:#如果没有值则不插入该字段
        new_mongoData['formatMentionCar']=formatMentionCar
    collection2.insert(new_mongoData)
    n+=1

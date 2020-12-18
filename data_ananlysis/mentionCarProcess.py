# coding:utf-8
import opinionMining
import pymysql
from pymongo import MongoClient
import jieba
jieba.load_userdict('E:/博二/奇瑞项目/项目代码/userdict.txt')
import time
import re

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

def process_excel(filename,featureSet,carDataSet,collection):
    print('正在导入%s的数据'%filename)
    f=open(filename, 'r',encoding='UTF-8')#加载处理原始数据并写入数据到新表
    reg=re.compile(' 20\d\d')
    for line in f.readlines()[1:]:
        new_mongoData={}
        data=line.replace(u'\n',u'').split(u',')
        #用户
        new_mongoData['user']=data[2]
        #购买车型
        try:
            car_specific=data[4]
            car=re.split(reg,car_specific)[0]
        except:
            car_specific,car='',''
        new_mongoData['car_specific']=car_specific
        new_mongoData['car']=car
        #购买时间
        try:
            new_mongoData['time']=data[7]
        except:
            new_mongoData['time']=''
        #提及车型
        comment=data[31]
        formatMentionCar=mentionCarProcess(comment,featureSet,carDataSet)
        if len(list(formatMentionCar))>0:#如果没有值则不插入该字段
            new_mongoData['formatMentionCar']=formatMentionCar
        collection.insert(new_mongoData)
        
t1=time.time()
featureSet,senwordSet,degwordSet,notwordSet=opinionMining.load_feature_senword_dataSet('E:/博二/奇瑞项目/项目代码/产品特征-情感词_数据集.xlsx')
carDataSet=loadCarDataSet()

client=MongoClient('47.92.211.251',30000)
collection=client.new_carDataset.mentionCar

import os
i=0
excel_names=os.listdir(u'汽车之家')
for i in range(len(excel_names)):
    if i%10==0:print(i+1)
    filename='汽车之家/%s'%excel_names[i]
    process_excel(filename,featureSet,carDataSet,collection)
    i+=1
    
t2=time.time()
print(u'一共花费时间%.2fs'%(t2-t1))













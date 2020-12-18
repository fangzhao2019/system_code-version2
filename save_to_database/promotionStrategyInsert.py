# coding:utf-8
from pymongo import MongoClient
import pymysql
from publicMethods import *
#import copy

def insert_into_promotion_strategy(carDataSet,carTypeSet,fineGrainedFeature):
    print('正在导入提升策略的数据')
    client=MongoClient('47.92.211.251',30000)
    collection=client.new_carDataset.test
    print('一共包含数据%d个'%collection.find().count())

    for car_type in carTypeSet.keys():
        searchResult=collection.find({'car_type':car_type},{'car':1,'car_type':1,'formatOpinionSet':1})
        print('**************关于%s的评论一共有%d条**************'%(car_type,searchResult.count()))
        #计算每个特征的重要性
        featureCount={}
        n=0
        for row in searchResult:
            n+=1
            if n%1000==0:print(n)
            if not 'formatOpinionSet' in row.keys():continue
            for key1 in row['formatOpinionSet'].keys():
                for key2 in row['formatOpinionSet'][key1].keys():
                    if not key2 in featureCount.keys():
                        featureCount[key2]=0
                    featureCount[key2]+=1
        #feature_number=copy.copy(featureCount)
        #归一化
        for key in featureCount.keys():
            featureCount[key]=featureCount[key]**(1/2)
        max_num=max(list(featureCount.values()))
        for key in featureCount.keys():
            featureCount[key]=featureCount[key]/float(max_num)
        print('特征重要性计算完毕')
        #return featureCount
        
        #在该车大类下，对于每一个车型，计算特征的满意度
        car_type_id=carTypeSet[car_type]
        carSubDataSet=splitCarDataSet(carDataSet,car_type_id)
        for car in carSubDataSet.keys():
            
            conn = pymysql.connect(host='47.99.116.136',user='root',passwd='3H1passwd',port=3306,db='car_test',charset='utf8')
            cursor=conn.cursor()
            sql_sentence='insert into promotion_strategy(car_id,fine_grained_feature_id,satisfaction_num,importance_num,feature_count) values(%s,%s,%s,%s,%s)'

            car_id=carSubDataSet[car]['car_id']
            searchSubResult=collection.find({'car_type':car_type,'car':car},{'car':1,'formatOpinionSet':1})
            print('正在处理%s的数据，包含%d条'%(car,searchSubResult.count()))
            featureSatisfaction={}
            m=0
            for row in searchSubResult:
                m+=1
                if m%100==0:print(m)
                if not 'formatOpinionSet' in row.keys():continue
                if not '缺点' in row['formatOpinionSet'].keys():continue
                if not '优点' in row['formatOpinionSet'].keys():continue
                positive_feature=list(row['formatOpinionSet']['优点'].keys())
                negative_feature=list(row['formatOpinionSet']['缺点'].keys())
                for feature in positive_feature:
                    if not feature in featureSatisfaction.keys():
                        featureSatisfaction[feature]=[0,0]
                    featureSatisfaction[feature][0]+=1
                for feature in negative_feature:
                    if not feature in featureSatisfaction.keys():
                        featureSatisfaction[feature]=[0,0]
                    featureSatisfaction[feature][1]+=1
            #计算满意度
            for feature in featureSatisfaction.keys():
                pos=featureSatisfaction[feature][0]
                neg=featureSatisfaction[feature][1]
                satisfaction=pos/float(pos+neg)
                #特征出现次数
                feature_count=pos+neg

                feature_id=fineGrainedFeature[feature]
                importance=featureCount[feature]
                try:
                    cursor.execute(sql_sentence,(car_id,feature_id,satisfaction,importance,feature_count))
                except Exception as e:
                    print(e)
            cursor.close()                          # 关闭游标
            conn.commit()                           #向数据库插入一条数据时必须要有这个方法，否则数据不会被真正的插入
            conn.close()

carTypeSet=loadCarTypeSet()
carDataSet=loadCarDataSet()
fineGrainedFeature=loadFineGrainedFeature()
featureCount=insert_into_promotion_strategy(carDataSet,carTypeSet,fineGrainedFeature)

    

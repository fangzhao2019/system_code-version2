# coding:utf-8
from pymongo import MongoClient
import pymysql
import numpy as np
from publicMethods import *

def insert_into_article(carDataSet):
    print('正在导入测评文章的数据')
    conn = pymysql.connect(host='47.99.116.136',user='root',passwd='3H1passwd',port=3306,db='car_test',charset='utf8')
    cursor=conn.cursor()
    sql_sentence='insert into article(id,data_sources_id,car_id,title,url,article_id,release_time) values (%s,%s,%s,%s,%s,%s,%s)'

    client=MongoClient('47.92.211.251',30000)
    collection=client.new_carDataset.article
    print('一共包含文章%d个'%collection.find().count())
    n=1
    for row in collection.find({},{'article_id':1,'dataSource':1,'url':1,'car':1,'articleType':1,'title':1,'time':1}).batch_size(10):
        if row['articleType']=='pingce':
            article_id=row['article_id']
            data_sources_id=dataSourceProcess(row['dataSource'])
            url=row['url']
            time=row['time']
            car=row['car']
            title=row['title']
            if car in carDataSet.keys():
                car_id=carDataSet[car]['car_id']
            else:
                continue
            try:
                cursor.execute(sql_sentence,(n,data_sources_id,car_id,title,url,article_id,time))
                n+=1
                if n%100==0:print(n)
            except Exception as e:
                print(e)
                
    cursor.close()                          # 关闭游标
    conn.commit()                           #向数据库插入一条数据时必须要有这个方法，否则数据不会被真正的插入
    conn.close()

def insert_into_evaluating_article(fineGrainedFeature):
    print('正在导入测评文章的摘要')
    conn = pymysql.connect(host='47.99.116.136',user='root',passwd='3H1passwd',port=3306,db='car_test',charset='utf8')
    cursor=conn.cursor()
    sql_sentence='insert into evaluating_article(id,article_id,fine_grained_feature_id,assessment_review) values (%s,%s,%s,%s)'
    articleIdSet=loadArticleIdSet()

    client=MongoClient('47.92.211.251',30000)
    collection=client.new_carDataset.article
    print('一共包含文章%d个'%collection.find().count())
    n=1
    for row in collection.find():
        article_id=row['article_id']
        if not article_id in articleIdSet:
            continue
        try:
            mentionFeatureSet=row['mentionFeatureSet']
        except:
            continue

        for feature in mentionFeatureSet.keys():
            if feature in fineGrainedFeature.keys():
                feature_id=fineGrainedFeature[feature]
                commentSet=mentionFeatureSet[feature]
                for comment in commentSet:
                    try:
                        cursor.execute(sql_sentence,(n,article_id,feature_id,comment))
                        n+=1
                        if n%100==0:print(n)
                    except Exception as e:
                        print(e)
                
    cursor.close()                          # 关闭游标
    conn.commit()                           #向数据库插入一条数据时必须要有这个方法，否则数据不会被真正的插入
    conn.close()

def insert_into_media_link(carDataSet):
    print('正在导入媒体文章的数据')
    conn = pymysql.connect(host='47.99.116.136',user='root',passwd='3H1passwd',port=3306,db='car_test',charset='utf8')
    cursor=conn.cursor()
    sql_sentence='insert into media_link(id,data_sources_id,car_id,link_title,url,release_time) values (%s,%s,%s,%s,%s,%s)'

    client=MongoClient('47.92.211.251',30000)
    collection=client.new_carDataset.article
    print('一共包含文章%d个'%collection.find().count())
    n=1
    for row in collection.find({},{'dataSource':1,'url':1,'car':1,'title':1,'time':1}):
        data_sources_id=dataSourceProcess(row['dataSource'])   
        url=row['url']
        time=row['time'].split()[0]
        car=row['car']
        link_title=row['title']
        if car in carDataSet.keys():
            car_id=carDataSet[car]['car_id']
        else:
            continue 
        try:
            cursor.execute(sql_sentence,(n,data_sources_id,car_id,link_title,url,time))
            n+=1
            if n%100==0:print(n)
        except Exception as e:
            print(e)
                
    cursor.close()                          # 关闭游标
    conn.commit()                           #向数据库插入一条数据时必须要有这个方法，否则数据不会被真正的插入
    conn.close()

def insert_into_media_article(dataSource,carDataSet):
    print('正在导入媒体情感分析的数据')
    conn = pymysql.connect(host='47.99.116.136',user='root',passwd='3H1passwd',port=3306,db='car_test',charset='utf8')
    cursor=conn.cursor()
    sql_sentence='insert into media_articles(id,car_id,positive_number,negative_number,data_sources_id,media_date) values (%s,%s,%s,%s,%s,%s)'

    client=MongoClient('47.92.211.251',30000)
    collection=client.new_carDataset.article
    print('一共包含文章%d个'%collection.find().count())
    n=1
    for source in dataSource.keys():
        data_source_id=dataSource[source]
        for car in carDataSet.keys():
            car_id=carDataSet[car]['car_id']
            #查询MongoDB，并统计数量，若为0，则不处理
            searchResult=collection.find({'dataSource':source,'car':car},{'polarity':1,'time':1})
            if searchResult.count()==0:continue
            resultSet=[]
            for row in searchResult:
                polarity=row['polarity']
                time=row['time']
                result=[time,polarity]
                resultSet.append(result)
            #按时间进行统计
            time_UniqueValue=list(set([exa[0] for exa in resultSet]))
            dataSet=[]
            for time in time_UniqueValue:
                subResultSet=splitDataSet(resultSet,0,time)
                positive_num=sum([1 for exa in subResultSet if exa[0]==1])
                negative_num=sum([1 for exa in subResultSet if exa[0]==-1])
               #导入数据库
                try:
                    cursor.execute(sql_sentence,(n,car_id,positive_num,negative_num,data_source_id,time))
                    n+=1
                    if n%100==0:print(n)
                except Exception as e:
                    print(e)
                
    cursor.close()                          # 关闭游标
    conn.commit()                           #向数据库插入一条数据时必须要有这个方法，否则数据不会被真正的插入
    conn.close()


carDataSet=loadCarDataSet()
dataSource=loadDataSource()
fineGrainedFeature=loadFineGrainedFeature()
#insert_into_article(carDataSet)
#insert_into_evaluating_article(fineGrainedFeature)
insert_into_media_link(carDataSet)
insert_into_media_article(dataSource,carDataSet)

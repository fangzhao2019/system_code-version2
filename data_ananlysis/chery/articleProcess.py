# coding:utf-8
from __future__ import division
from pymongo import MongoClient
import opinionMining
from publicMethods import *
import jieba
jieba.load_userdict('E:/博二/奇瑞项目/项目代码/userdict.txt')
from sklearn.naive_bayes import MultinomialNB
from sklearn.externals import joblib

#为输入文本构建分类属性向量，包含为1，否则为0
def setOfWords2Vec(vocabList, inputSet):
    returnVec = [0]*len(vocabList)
    for word in inputSet:
        if word in vocabList:
            returnVec[vocabList.index(word)] = 1
    return returnVec

def loadMyVocab(filename):
    f=open(filename)
    myVocab=f.read().strip().split(',')
    return myVocab

#将数据格式化为字典
def formatData(opinionSet):
    formatOpinionSet={}
    feaUniqueVals=set([exa[0] for exa in opinionSet])
    for value1 in feaUniqueVals:
        subDataSet=splitDataSet(opinionSet,0,value1)
        commentSet=[c[0] for c in subDataSet]
        formatOpinionSet[value1]=commentSet
    return formatOpinionSet

def featureExtraction(clauseList,featureSet):
    opinionSet=[]
    #对于每一条子句，分别提取特征词与观点词
    for i in range(len(clauseList)):
        clause=clauseList[i]
        if len(clause)>100:continue
        clauseWordSet=[w for w in jieba.cut(clause)]
        for j in range(len(featureSet)):
            #定位特征词
            feature=featureSet[j]
            if feature in clauseWordSet:
                result=[feature,clause]
                opinionSet.append(result)
    formatFeatureSet=formatData(opinionSet)
    return formatFeatureSet

def sentimentAnalysis(clauseList,myVocab,clf):
    senCount=[]
    for clause in clauseList:
        if len(clause)>100:
            continue
        clauseVec=[w for w in jieba.cut(clause)]
        testData=setOfWords2Vec(myVocab,clauseVec)
        predictor=clf.predict([testData])[0]
        senCount.append(predictor)
    if len(senCount)==0:
        return 0
    polarity=sum(senCount)/float(len(senCount))
    if polarity>0:
        return 1
    else:
        return -1

featureSet,senwordSet,degwordSet,notwordSet=opinionMining.load_feature_senword_dataSet('E:/博二/奇瑞项目/项目代码/产品特征-情感词_数据集.xlsx')
clf=joblib.load('../dataset/sentimentAnalysis.model')
myVocab=loadMyVocab('../dataset/myVocab.txt')

client=MongoClient('47.92.211.251',30000)
collection2=client.car_test.car_artical_chery
collection3=client.new_carDataset.article_chery

print('一共包含待处理文章数据%s条'%collection2.find().count())
n=0
for row in collection2.find({},{'dataSource':1,'car':1,'url':1,'articleType':1,'title':1,'time':1,'content':1}):
    if n%100==0:print(n)
    n+=1
    new_mongoData={}
    #id
    new_mongoData['article_id']=n
    #数据来源&url
    try:
        new_mongoData['url']=row['url']
        new_mongoData['dataSource']=row['dataSource']
    except:
        continue

    #车型
    car=row['car']
    if car=='eQ1':car='奇瑞eQ1'
    new_mongoData['car']=car
    #文章类型
    new_mongoData['type']=row['articleType']

    #标题
    title=row['title']
    if title==None:continue
    new_mongoData['title']=title
    #时间
    time=''.join([row['time'].split('月')[0].replace('年','-'),'-01'])
    new_mongoData['time']=time
    #文章内容
    content=row['content'].strip()
    if len(content)==0:continue
    new_mongoData['content']=content
    
    #提及特征及其对应的句子
    clauseList=opinionMining.clauseSegmentation(content)
    mentionFeatureSet=featureExtraction(clauseList,featureSet)
    if len(list(mentionFeatureSet.keys()))>0:
        new_mongoData['mentionFeatureSet']=mentionFeatureSet
    #文章极性
    new_mongoData['polarity']=sentimentAnalysis(clauseList,myVocab,clf)
    collection3.insert(new_mongoData)

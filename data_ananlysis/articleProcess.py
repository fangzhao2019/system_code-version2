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
clf=joblib.load('dataset/sentimentAnalysis.model')
myVocab=loadMyVocab('dataset/myVocab.txt')

client=MongoClient('47.92.211.251',30000)
collection1=client.car_test.model
collection2=client.car_test.car_article
collection3=client.new_carDataset.article

modelType={}
for row in collection1.find({},{'carHomeUrl':1,'modelName':1}):
    modelID=row['carHomeUrl'].replace('/','')
    modelName=row['modelName']
    modelType[modelID]=modelName
print('共有车型%d个'%len(list(modelType)))

print('一共包含待处理文章数据%s条'%collection2.find().count())
n=1
for row in collection2.find({},{'modelId':1,'url':1,'type':1,'title':1,'time':1,'content':1}):
    if n%100==0:print(n)
    n+=1
    #continue
    new_mongoData={}
    #id
    new_mongoData['article_id']=n
    #数据来源&url
    try:
        url=row['url']
        dataSource=sourceProcess(url)
    except:
        continue
    if dataSource=='':continue
    new_mongoData['url']=url
    new_mongoData['dataSource']=dataSource

    #车型
    try:
        new_mongoData['car']=modelType[row['modelId']]
    except:
        continue
    #文章类型
    try:
        new_mongoData['articleType']=row['type']
    except:
        continue
    #标题
    try:
        title=row['title']
    except:
        continue
    if title==None:continue
    new_mongoData['title']=title
    #时间
    try:
        tt=row['time'].split()[0].split('-')
        tt[2]='01'
        time='-'.join(tt)
    except:
        time=''
    new_mongoData['time']=time
    #文章内容
    try:
        content=row['content'].strip()
    except:
        continue
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
    

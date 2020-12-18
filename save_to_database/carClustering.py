# coding:utf-8
from publicMethods import *
import numpy as np
import copy

def loadCarCount():
    conn = pymysql.connect(host='47.99.116.136',user='root',passwd='3H1passwd',port=3306,db='car_test',charset='utf8')
    cursor=conn.cursor()
    sql_sentence='select car_id,num from car_market'
    cursor.execute(sql_sentence)
    results = cursor.fetchall()
    carCount={}
    for r in results:
        carCount[r[0]]=r[1]
    return carCount

def carFilter(carCount,carSubSet):
    dataSet=[]
    for car in carSubSet:
        if carCount[car]>10:
            dataSet.append(car)
    return(dataSet)

def loadCarRelation():
    conn = pymysql.connect(host='47.99.116.136',user='root',passwd='3H1passwd',port=3306,db='car_test',charset='utf8')
    cursor=conn.cursor()
    sql_sentence='select car_type_id,car_id,associated_id,value from car_contend_value'
    cursor.execute(sql_sentence)
    results = cursor.fetchall()
    carRelation=[]
    for r in results:
        carRel=[r[0],r[1],r[2],r[3]]
        carRelation.append(carRel)
    return carRelation

#获取两辆车的竞争关系
def readRelation(car1,car2,carSubRelation):
    for relation in carSubRelation:
        if relation[0]==car1:
            if relation[1]==car2:
                return relation[2]
    return 0

#创建竞争关系矩阵
def createRelationMat(carSubSet,carSubRelation):
    valueSet=[exa[2] for exa in carSubRelation]
    pmax=max(valueSet)
    pmin=min(valueSet)
    relationMat=np.zeros((len(carSubSet),len(carSubSet)))
    for i in range(len(carSubSet)):
        for j in range(len(carSubSet)):
            car1=carSubSet[i]
            car2=carSubSet[j]
            relation=readRelation(car1,car2,carSubRelation)
            if relation==0:relationMat[i,j]=0
            else:relationMat[i,j]=(relation-pmin)/float(pmax-pmin)
    return relationMat

#计算两个簇之间的距离,linkage表示距离度量方式
def distLinkage(cluster1,cluster2,carSet,PMImat,linkage):
    distance=[]
    for car1 in cluster1:
        for car2 in cluster2:
            m=carSet.index(car1)
            n=carSet.index(car2)
            distance.append(PMImat[m,n])
    if linkage=='singleLinkage':return max(distance)
    if linkage=='completeLinkage':return min(distance)
    if linkage=='averageLinkage':return sum(distance)/len(distance)

#层次聚类法，输入为二元列表，n_cluster表示聚类个数
def agglomerativeClustering(dataSet,featureSet,PMImat,n_cluster,linkage):
    clusterSet=copy.deepcopy(dataSet)
    n=len(clusterSet)#类别数量
    while n>n_cluster:
        #计算每两个类簇之间的距离,一个储存距离，一个储存相比较的簇的索引
        distanceIndex=[]
        distanceList=[]
        for i in range(len(clusterSet)):
            for j in range(i+1,len(clusterSet)):
                distance=distLinkage(clusterSet[i],clusterSet[j],featureSet,PMImat,linkage)
                distanceIndex.append([i,j])
                distanceList.append(distance)
        #找到距离最小的两个类簇(最小距离=最大相关度)
        minDistance=max(distanceList)#获取最大相关度
        minIndex=distanceList.index(minDistance)#获取最大相关度的位置
        x,y=distanceIndex[minIndex]#获取产生最大相关度的两个比较对象的索引
        #合并最近的两个簇，得到新的类簇数据集
        clusterSet[x].extend(clusterSet[y])
        del clusterSet[y]
        n=n-1 
    return clusterSet

def loadCarDataSet2():
    conn = pymysql.connect(host='47.99.116.136',user='root',passwd='3H1passwd',port=3306,db='car_test',charset='utf8')
    cursor=conn.cursor()
    sql_sentence='select car_id,car_name from car'
    cursor.execute(sql_sentence)
    results = cursor.fetchall()
    carDataSet={}
    for r in results:
        carDataSet[r[0]]=r[1]
    return carDataSet

def showClusterResult(clusterResult,carDataSet):
    i=0
    for cluster in clusterResult:
        ccc=[]
        for car in cluster:
            ccc.append(carDataSet[car])
        i+=1
        print('第%d个聚类簇为\t：%s'%(i,','.join(ccc)))

conn = pymysql.connect(host='47.99.116.136',user='root',passwd='3H1passwd',port=3306,db='car_test',charset='utf8')
cursor=conn.cursor()
sql_sentence='insert into car_contend(car_type_id,car_id,num,category) values (%s,%s,%s,%s)'

carDataSet=loadCarDataSet2()
carCount=loadCarCount()
#读取竞争关系表
carRelation=loadCarRelation()
#根据车类别提取出对应的车型——划分子数据集
type_unique_value=set([exa[0] for exa in carRelation])
for car_type_id in type_unique_value:
    carSubRelation=splitDataSet(carRelation, 0, car_type_id)
    carSubSet=list(set([exa[0] for exa in carSubRelation]))
    carSubSet=carFilter(carCount,carSubSet)
    print('%s的竞争关系一共有%d个,包含车型%d个'%(car_type_id,len(carSubRelation),len(carSubSet)))
    #构建竞争关系矩阵
    relationMat=createRelationMat(carSubSet,carSubRelation)
    #聚类
    if len(carSubSet)<=20:
        n_cluster=int(len(carSubSet)/5)+1
    else:
        n_cluster=int(len(carSubSet)/5)+1
    clusterInitialSet=[[car] for car in carSubSet]
    #clusterResult=agglomerativeClustering(clusterInitialSet,carSubSet,relationMat,n_cluster,'singleLinkage')
    #clusterResult=agglomerativeClustering(clusterInitialSet,carSubSet,relationMat,n_cluster,'completeLinkage')
    clusterResult=agglomerativeClustering(clusterInitialSet,carSubSet,relationMat,n_cluster,'averageLinkage')
    #print(clusterResult)
    #showClusterResult(clusterResult,carDataSet)
    #print('\n\n')
    #导入数据
    category=1####类别变量
    for cluster in clusterResult:
        for car_id in cluster:
            num=carCount[car_id]
            try:
                cursor.execute(sql_sentence,(car_type_id,car_id,int(num**(1/2)),category))
            except Exception as e:
                print(e)
        category+=1

cursor.close()                          # 关闭游标
conn.commit()                           #向数据库插入一条数据时必须要有这个方法，否则数据不会被真正的插入
conn.close()

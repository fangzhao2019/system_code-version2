import Mysql
import MongoDB

ERR_ID = -1


def setCarId(car_name):
    id = Mysql.getCarIdByName(car_name)
    if id is None:
        return ERR_ID
    else:
        return id[0]


def setCoarseFeatureId(feature_name):
    id = Mysql.getCoarseFeatureIdByName(feature_name)
    if id is None:
        Mysql.addCoarseFeature(feature_name)
        id = Mysql.getCoarseFeatureIdByName(feature_name)
    else:
        return id[0]


def setFineFeatureId(feature_name):
    id = Mysql.getFineFeatureIdByName(feature_name)
    if id is None:
        return ERR_ID
    else:
        return id[0]


def setPartFeatureId(car, coarse, fine):
    id = Mysql.getPartFeatureIdBy3Id(car, coarse, fine)
    if id is None:
        return ERR_ID
    else:
        return id[0]


def setFeaturePointId(id, name):
    id = Mysql.getFeaturePointIdByIdName(id, name)
    if id is None:
        return ERR_ID
    else:
        return id[0]


def setCommentId(car, coarse, fine, point_name):
    id = Mysql.getCommentId(car, coarse, fine, point_name)
    if id is None:
        return ERR_ID
    else:
        return id[0]


def geteRepeatIndex(list, coarse, fine):
    i = -1
    index = -1
    for record in list:
        i = i + 1
        if coarse == record['coarse_grained_feature'] and fine == record['fine_grained_feature']:
            index = i
    return index


# 初始化一条part_peature记录，feature_points和comments的集合
def partFeatureInit(car, coarse, fine):
    dic = {'car_id': car, 'fine_feature_count': 1, 'coarse_grained_feature': coarse,
           'fine_grained_feature': fine}
    return dic


def generatePartFeature(lists, car_id):  # 对小特征计数，将特征词和句按小特征合并
    result_list = []
    for Data in lists:
        if 'formatOpinionSet' in Data:
            format_opinion_set_dic = Data['formatOpinionSet']
            for coarse_feature in format_opinion_set_dic:
                if len(format_opinion_set_dic[coarse_feature]) != 0:
                    coarse_features_dic = format_opinion_set_dic[coarse_feature]
                    # 遍历小特征
                    for fine_feature in coarse_features_dic:
                        index = geteRepeatIndex(
                            result_list, coarse_feature, fine_feature)
                        if index >= 0:
                            result_list[index]['fine_feature_count'] += 1
                        else:
                            result_record_dic = {}
                            result_record_dic = partFeatureInit(
                                car_id, coarse_feature, fine_feature)
                            result_list.append(result_record_dic)
    return result_list


def setAllId(list):
    for record in list:
        record['coarse_grained_feature_id'] = setCoarseFeatureId(
            record.pop('coarse_grained_feature'))
        record['fine_grained_feature_id'] = setFineFeatureId(
            record.pop('fine_grained_feature'))
    return list


def getRepeatFeaturePointIndex(list, coarse, fine, feature_point):
    i = -1
    index = -1
    for record in list:
        i = i + 1
        if coarse == record['coarse_grained_feature'] and fine == record['fine_grained_feature'] and record['feature_point'] == feature_point:
            index = i
    return index


def featurePointInit(car_id, coarse, fine, feature_point):
    dic = {'car_id': car_id, 'coarse_grained_feature': coarse,
           'fine_grained_feature': fine, 'feature_point': feature_point}
    return dic


def generateFeaturePoint(lists, car_id):  # 对小特征计数，将特征词和句按小特征合并
    result_list = []
    for Data in lists:
        if 'formatOpinionSet' in Data:
            format_opinion_set_dic = Data['formatOpinionSet']
            for coarse_feature in format_opinion_set_dic:
                if len(format_opinion_set_dic[coarse_feature]) != 0:
                    coarse_features_dic = format_opinion_set_dic[coarse_feature]
                    # 遍历小特征
                    for fine_feature in coarse_features_dic:
                        # 遍历feature_point
                        for feature_point in coarse_features_dic[fine_feature]:
                            index = getRepeatFeaturePointIndex(
                                result_list, coarse_feature, fine_feature, feature_point)
                            if index <= 0:
                                result_record_dic = {}
                                result_record_dic = featurePointInit(
                                    car_id, coarse_feature, fine_feature, feature_point)
                                result_list.append(result_record_dic)
    return result_list


def getAllFeaturePointId(list):
    for record in list:
        record['coarse_feature_id'] = setCoarseFeatureId(
            record.pop('coarse_grained_feature'))
        record['fine_feature_id'] = setFineFeatureId(
            record.pop('fine_grained_feature'))
        record['part_feature_id'] = setPartFeatureId(
            record['car_id'], record.pop('coarse_feature_id'), record.pop('fine_feature_id'))
    return list


def commentInit(car_id, coarse_feature, fine_feature, feature_point, comment):
    dic = {'car_id': car_id, 'coarse_grained_feature': coarse_feature,
           'fine_grained_feature': fine_feature, 'feature_point': feature_point, 'comment': comment}
    return dic


def generateComment(lists, car_id):
    result_list = []
    for Data in lists:
        if 'formatOpinionSet' in Data:
            format_opinion_set_dic = Data['formatOpinionSet']
            for coarse_feature in format_opinion_set_dic:
                if len(format_opinion_set_dic[coarse_feature]) != 0:
                    coarse_features_dic = format_opinion_set_dic[coarse_feature]
                    # 遍历小特征
                    for fine_feature in coarse_features_dic:
                        # 遍历feature_point
                        comments_list = coarse_features_dic[fine_feature]
                        for feature_point in comments_list:
                            if len(comments_list[feature_point]) > 0:
                                result_record_dic = {}
                                result_record_dic = commentInit(
                                    car_id, coarse_feature, fine_feature, feature_point, comments_list[feature_point])
                                result_list.append(result_record_dic)
    return result_list


def getAllCommentId(list):
    for record in list:
        record['coarse_feature_id'] = setCoarseFeatureId(
            record.pop('coarse_grained_feature'))
        record['fine_feature_id'] = setFineFeatureId(
            record.pop('fine_grained_feature'))
        record['part_feature_id'] = setPartFeatureId(
            record.pop('car_id'), record.pop('coarse_feature_id'), record.pop('fine_feature_id'))
        record['feature_point_id'] = setFeaturePointId(
            record.pop('part_feature_id'), record.pop('feature_point'))
    return list


def partFeature(limit):  # 1 part_feature
    cars = Mysql.getAllCar2(limit)
    for car in cars:
        car_name = car[1]
        car_id = car[0]
        print(car_name)
        source = MongoDB.getCommentByCarName(car_name)
        part_feature_table = generatePartFeature(source, car_id)
        part_feature_table = setAllId(part_feature_table)
        Mysql.addPartFeature(part_feature_table)  # 将part_feature插入数据库
        print(car)



def featurePoint(limit):  # 2 feature_point
    cars = Mysql.getAllCar2(limit)
    for car in cars:
        car_name = car[1]
        car_id = car[0]
        print(car_name)
        source = MongoDB.getCommentByCarName(car_name)
        feature_point_table = generateFeaturePoint(source, car_id)
        feature_point_table = getAllFeaturePointId(feature_point_table)
        Mysql.addFeaturePoint(feature_point_table)  # 插入观点词


def comment(limit):  # 3 comment
    cars = Mysql.getAllCar2(limit)
    for car in cars:
        car_name = car[1]
        car_id = car[0]
        print("s",car)
        source = MongoDB.getCommentByCarName(car_name)
        comment_table = generateComment(source, car_id)
        comment_table = getAllCommentId(comment_table)
        Mysql.addComment(comment_table)  # 插入评论
        print("f")
    MongoDB.dbClose()
    Mysql.dbClose()

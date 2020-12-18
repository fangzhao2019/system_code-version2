import Mysql
import MongoDB


def ogenerateFeaturePoint(list):  # 这里的part_features_list应该传入get到id后的
    result_list = []
    for part_feature_dic in list:
        for feature_points in part_feature_dic['feature_points']:
            part_feature_id = setPartFeatureId(
                part_feature_dic['car_id'], part_feature_dic['coarse_grained_feature_id'],
                part_feature_dic['fine_grained_feature_id'])
            for feature_point_word, comment in feature_points.items():
                index = getRepeatFeaturePointIndex(
                    result_list, part_feature_id, feature_point_word)
                if index > 0:
                    result_list[index]['comment'].append(comment)
                else:
                    feature_point_dic = {}
                    feature_point_dic['feature_point_word'] = feature_point_word
                    feature_point_dic['part_feature_id'] = part_feature_id
                    feature_point_dic['comment'] = []
                    feature_point_dic['comment'].append(comment)
                    result_list.append(feature_point_dic)
    return result_list


def ogenerateComments(list):
    result_list = []
    for feature_point_dic in list:
        feature_point_id = setFeaturePointId(feature_point_dic['part_feature_id'],
                                             feature_point_dic['feature_point_word'])
        for com in feature_point_dic['comment']:
            comment = {}
            comment['feature_point_id'] = feature_point_id
            comment['comment'] = com
            result_list.append(comment)
    return result_list

# koubeuciyun


def test():
    cars = Mysql.getAllCar()
    for car in cars:
        car_name = car[1]
        car_id = car[0]
        print(car_name)
        source = MongoDB.getCommentByCarName(car_name)
        # 1 part_feature
        # part_feature_table = generatePartFeature(source, car_id)
        # part_feature_table = setAllId(part_feature_table)
        # 2 feature_point
        # feature_point_table = generateFeaturePoint(source, car_id)
        # feature_point_table = getAllFeaturePointId(feature_point_table)
        # 3 comment
        comment_table = generateComment(source, car_id)
        comment_table = getAllCommentId(comment_table)
        print(comment_table)
    MongoDB.dbClose()
    Mysql.dbClose()


def test():
    cars = Mysql.getAllCar()
    for car in cars:
        car_name = car[1]
        car_id = car[0]
        source = MongoDB.getRegionInfo(car_name)
        car_eva_table = countCarEva(source, car_id)
        car_eva_table = getCarEvaAllId(car_eva_table)
        Mysql.addCaeEva(car_eva_table)


for row in comment.find({"status": 0}).batch_size(50).limit(1000):
    data = parse(row['content'])
    # print(data)
    comment.update({'_id': row['_id']}, {'$set': {'status': 1, 'data': data}})

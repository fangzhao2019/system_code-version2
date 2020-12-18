import Mysql
import MongoDB

ERR_ID = -1


def setProvinceId(str):
    id = Mysql.getProvinceIdByName(str)
    if id is None:
        return ERR_ID
    else:
        return id[0]


def setCityId(str):
    id = Mysql.getCityIdByName(str)
    if id is None:
        return ERR_ID
    else:
        return id[0]


def setCarId(car_name):
    id = Mysql.getCarIdByName(car_name)
    if id is None:
        return ERR_ID
    else:
        return id[0]


def setFineFeatureId(feature_name):
    id = Mysql.getFineFeatureIdByName(feature_name)
    if id is None:
        return ERR_ID
    else:
        return id[0]


def getMapRepeatIndex(list, province, city):
    i = -1
    index = -1
    for record in list:
        i = i + 1
        if province == record['province'] and city == record['city']:
            index = i
    return index


def mapSalesInit(car_id, province, city):
    dict = {'car_id': car_id,
            'province': province,
            'city': city,
            'province_id': setProvinceId(province),
            'city_id': setCityId(city),
            'count': 1}
    return dict


def countSales(region_info_list, car):
    result_list = []
    car_id = setCarId(car)
    for region_info_dic in region_info_list:
        index = getMapRepeatIndex(
            result_list, region_info_dic['province'], region_info_dic['city'])
        if index >= 0:
            result_list[index]['count'] += 1
        else:
            result_record_dic = {}
            result_record_dic = mapSalesInit(
                car_id, region_info_dic['province'], region_info_dic['city'])
            result_list.append(result_record_dic)
    return result_list


def getRepeatMapOpinion(list, province, feature):
    i = -1
    index = -1
    for record in list:
        i = i + 1
        if province == record['province'] and feature == record['feature']:
            index = i
    return index


def mapOpoinInit(car, province, feature, polar):
    dic = {'car_id': car, 'province': province, 'feature': feature}
    if polar == 1:
        dic['good_num'] = 1
        dic['bad_num'] = 0
    else:
        dic['good_num'] = 0
        dic['bad_num'] = 1
    return dic


def countMapOpinion(source, car):
    result_list = []
    for sou_dic in source:
        comment = sou_dic['formatOpinionSet']
        province = sou_dic['province']
        if '优点' in comment and len(comment['优点']) > 0:
            for feature in comment['优点']:
                index = getRepeatMapOpinion(
                    result_list, sou_dic['province'], feature)
                if index >= 0:
                    result_list[index]['good_num'] += 1
                else:
                    result_record_dic = mapOpoinInit(
                        car, sou_dic['province'], feature, 1)
                    # result_record_dic['opinion'].append
                    result_list.append(result_record_dic)
        if '缺点' in comment and len(comment['缺点']) > 0:
            for feature in comment['缺点']:
                index = getRepeatMapOpinion(
                    result_list, sou_dic['province'], feature)
                if index >= 0:
                    result_list[index]['bad_num'] += 1
                else:
                    result_record_dic = mapOpoinInit(
                        car, sou_dic['province'], feature, 0)
                    result_list.append(result_record_dic)
    return result_list


def getMapOpinionAllId(list):
    for record in list:
        record['province_id'] = setProvinceId(record['province'])
        record['feature_id'] = setFineFeatureId(record['feature'])
    return list


def getRepeatMapOpinionEva(list, province, feature, op_word, polar):
    i = -1
    index = -1
    for record in list:
        i = i + 1
        if province == record['province'] and feature == record['feature'] and op_word == record['op_word'] and polar == \
                record['polar']:
            index = i
    return index


def caeEvaInit(car, province, feature, op_word, polar):
    dic = {'car_id': car,
           'province': province,
           'feature': feature,
           'op_word': op_word,
           'opinion': feature + op_word,
           'num': 1,
           'polar': polar}
    return dic


def countCarEva(source, car):
    result_list = []
    for sou_dic in source:
        comment = sou_dic['formatOpinionSet']
        province = sou_dic['province']
        if '优点' in comment and len(comment['优点']) > 0:
            for feature in comment['优点']:
                for op_word in comment['优点'][feature]:
                    index = getRepeatMapOpinionEva(
                        result_list, province, feature, op_word, 1)
                if index >= 0:
                    result_list[index]['num'] += 1
                else:
                    result_record_dic = caeEvaInit(
                        car, province, feature, op_word, 1)
                    result_list.append(result_record_dic)
        if '缺点' in comment and len(comment['缺点']) > 0:
            for feature in comment['缺点']:
                for op_word in comment['缺点'][feature]:
                    index = getRepeatMapOpinionEva(
                        result_list, province, feature, op_word, -1)
                if index >= 0:
                    result_list[index]['num'] += 1
                else:
                    result_record_dic = caeEvaInit(
                        car, province, feature, op_word, -1)
                    result_list.append(result_record_dic)
    return result_list


def getCarEvaAllId(list):
    for record in list:
        record['province_id'] = setProvinceId(record['province'])
        record['feature_id'] = setFineFeatureId(record['feature'])
    return list


def main(limit):
    cars = Mysql.getAllCar2(limit)
    for car in cars:
        print(car[1])
        car_name = car[1]
        car_id = car[0]
        # 1 save result_record_dic
        source = MongoDB.getRegionInfo(car_name)
        result_record_dic_table = countSales(source, car_name)
        Mysql.addMapSales(result_record_dic_table) #commit
        # 2 save result_record
        source = MongoDB.getRegionInfo(car_name)
        map_opoin_table = countMapOpinion(source, car_id)
        map_opoin_table = getMapOpinionAllId(map_opoin_table)
        Mysql.addMapOpinion(map_opoin_table)  #commit
        # 3 save car_evaluation
        source = MongoDB.getRegionInfo(car_name)
        car_eva_table = countCarEva(source, car_id)
        car_eva_table = getCarEvaAllId(car_eva_table)
        Mysql.addCarEva(car_eva_table)  #commit
        print(car[1], car[0])
    MongoDB.dbClose()
    Mysql.dbClose()

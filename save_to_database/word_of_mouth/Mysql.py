import pymysql

host = "47.99.116.136"
user = "root"
passwd = "3H1passwd"
schema= "car"
schema = "car_test"
err = "数据库错误"

db = pymysql.connect(host, user, passwd, schema)
cursor = db.cursor()


def dbClose():
    db.close()


def getAllCar():
    sql = "select car_id,car_name from car "
    cursor.execute(sql)
    data = cursor.fetchall()
    # db.close()
    return data

def getAllCar2(limit):
    sql = "select car_id,car_name from car "+limit
    cursor.execute(sql)
    data = cursor.fetchall()
    # db.close()
    return data


def getCarIdByName(str):
    sql = "select car_id from car where car_name = '%s'"
    # str = str.replace('(', '').replace(')', '')
    dt = (str,)
    cursor.execute(sql % dt)
    result = cursor.fetchone()
    return result


def getCoarseFeatureIdByName(str):
    sql = "select coarse_grained_feature_id from coarse_grained_featureset where coarse_grained_feature_name = '%s'"
    dt = (str,)
    cursor.execute(sql % dt)
    result = cursor.fetchone()
    # db.close()
    return result


def getFineFeatureIdByName(str):
    sql = "select fine_grained_feature_id from fine_grained_featureset where fine_grained_feature_name = '%s'"
    dt = (str,)
    cursor.execute(sql % dt)
    result = cursor.fetchone()
    # db.close()
    return result


def getPartFeatureIdBy3Id(car, coarse, fine):
    sql = """select part_feature_id from part_feature 
                where car_id = %s 
                    and coarse_grained_feature_id=%s
                    and fine_grained_feature_id=%s"""
    dt = (car, coarse, fine)
    cursor.execute(sql, dt)
    result = cursor.fetchone()
    # db.close()
    return result


def getFeaturePointIdByIdName(id, name):
    sql = "select feature_point_id from feature_point where part_feature_id = '%s' and feature_point_name='%s'"
    dt = (id, name)
    cursor.execute(sql % dt)
    result = cursor.fetchone()
    # db.close()
    return result


def getProvinceIdByName(str):
    sql = "select id from position_area where name = '%s' or short_name='%s' and id like '%s' "
    dt = (str, str, r'%0000')
    cursor.execute(sql % dt)
    result = cursor.fetchone()
    return result


def getCityIdByName(str):
    sql = "select id from position_area where name = '%s' or short_name='%s' and id not like '%s' "
    dt = (str, str, '%0000')
    cursor.execute(sql % dt)
    result = cursor.fetchone()
    return result

def getCommentId(car, coarse, fine,point_name):
    sql = """select feature_point_id from feature_point where part_feature_id = (select part_feature_id from part_feature 
                where car_id = %s and coarse_grained_feature_id = %s and fine_grained_feature_id = %s)
                and feature_point_name = %s"""
    dt = (car, coarse, fine,point_name)
    cursor.execute(sql % dt)
    result = cursor.fetchone()
    return result


def addCoarseFeature(str):
    sql = "INSERT INTO `coarse_grained_featureset` (`coarse_grained_feature_name`) VALUES ('%s');"
    dt = (str,)
    try:
        cursor.execute(sql % dt)
        db.commit()
    except:
        db.rollback()


def addPartFeature(list):
    for dic in list:
        sql = """INSERT INTO `part_feature` 
        (`car_id`, `coarse_grained_feature_id`, `fine_grained_feature_id`, `fine_feature_count`) 
        VALUES (%s,%s,%s,%s);"""
        cursor.execute(sql, [dic['car_id'], dic['coarse_grained_feature_id'],
                             dic['fine_grained_feature_id'], dic['fine_feature_count']])
    try:
        db.commit()
    except:
        db.rollback()


def addFeaturePoint(list):
    for dic in list:
        sql = """INSERT INTO `feature_point` 
                    (`part_feature_id`, `feature_point_name`) 
                        VALUES (%s,%s);"""
        cursor.execute(sql, [dic['part_feature_id'],
                             dic['feature_point']])
    try:
        db.commit()
    except:
        db.rollback()


def addComment(list):
    for dic in list:
        sql = """INSERT INTO `comment` 
        (`feature_point_id`, `comment_trim`) 
        VALUES (%s, %s);"""
        try:
            cursor.execute(sql, [dic['feature_point_id'], dic['comment']])
            db.commit()
        except:
            db.rollback()


def addMapSales(list):
    for dic in list:
        sql = """INSERT INTO `map_sales` 
        (`car_id`, `province_id`, `sales`, `city_id`) 
        VALUES (%s, %s, %s, %s);"""
        cursor.execute(
            sql, [dic['car_id'], dic['province_id'], dic['count'], dic['city_id']])
        try:
            db.commit()
        except:
            db.rollback()


def addMapOpinion(list):
    for dic in list:
        sql = """INSERT INTO `map_opinion` 
        (`province_id`, `car_id`, `good_num`, `bad_num`, `fine_grained_feature_id`) 
        VALUES (%s, %s, %s, %s, %s);"""
        cursor.execute(sql, [dic['province_id'], dic['car_id'],
                             dic['good_num'], dic['bad_num'], dic['feature_id']])
        try:
            db.commit()
            # print("提交")
        except:
            print("错误")
            db.rollback()


def addCarEva(list):
    for dic in list:
        sql = """INSERT INTO `car_evaluation` 
        (`car_id`, `province_id`, `fine_grained_feature_id`, `opinion`, `num`, `polarity`) 
        VALUES (%s, %s, %s, %s, %s, %s);"""
        cursor.execute(sql, [dic['car_id'], dic['province_id'],
                             dic['feature_id'], dic['opinion'], dic['num'], dic['polar']])
        try:
            db.commit()
            # print("提交")
        except:
            print("错误")
            db.rollback()


def addPrice(list):
    for dic in list:
        sql = """INSERT INTO `amount_price` 
        (`car_id`, `car_config`, `car_price`, `position_id`, `create_time`) 
        VALUES (%s, %s, %s, %s, %s);"""
        cursor.execute(sql, [dic['car_id'],
                             dic['config'], dic['price'], dic['province_id'], dic['time']])
        try:
            db.commit()
        except:
            print("错误")
            db.rollback()


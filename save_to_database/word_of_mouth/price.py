import Mysql
import MongoDB

ERR_ID = -1

def setProvinceId(str):
    id = Mysql.getProvinceIdByName(str)
    if id is None:
        return ERR_ID
    else:
        return id[0]


def getRepeatPriceIndex(list, car, config, province, time):
    i = -1
    index = -1
    for record in list:
        i = i + 1
        if car == record['car_id'] and config == record['config'] and province == record['province'] and time == record[
            'time']:
            index = i
    return index


def priceInit(car, config, province, time, price):
    dic = {'car_id': car,
           'config': config,
           'province': province,
           'time': time,
           'amount': 1,
           't_price': price
           }
    return dic


def amountPrice(source, car_id):
    result_list = []
    for dic in source:
        if len(dic['price']) > 1:
            index = getRepeatPriceIndex(
                result_list, car_id, dic['car_specific'], dic['province'], dic['time'])
            dic['price'] = float(dic['price'].replace("万元", ""))
            dic['time'] = dic['time'].replace("年", "").replace("月", "01").replace(" ", "0")
            if index >= 0:
                result_list[index]['t_price'] += dic['price']
                result_list[index]['t_amount'] += 1
            else:
                result_dic = {}
                result_dic = priceInit(car_id, dic['car_specific'],
                                       dic['province'], dic['time'], dic['price'])
                result_list.append(result_dic)
    return result_list


def getPriceAllId(list):
    for record in list:
        record['province_id'] = setProvinceId(record.pop('province'))
        record['price'] = record.pop('t_price') / record.pop('amount')
    return list


def main(limit):
    cars = Mysql.getAllCar2(limit)
    for car in cars:
        car_name = car[1]
        car_id = car[0]
        print(car_id)
        source = MongoDB.getPrice(car_name)
        amount_price_table = amountPrice(source, car_id)
        amount_price_table = getPriceAllId(amount_price_table)
        Mysql.addPrice(amount_price_table)
        print(car_name)
    MongoDB.dbClose()
    Mysql.dbClose()

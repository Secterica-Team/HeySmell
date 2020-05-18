import pymongo
import json
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler

# mongoDB server link
with open('../secret.json') as f:
    SECRET = json.load(f)
db_host_mongo_db_atlas = SECRET['mongo_db_link']

db_name = 'HeySmell'
collection_name_to_take_from = 'air_quality'
collection_name_to_put_in = 'air_quality_by_day'

connection = pymongo.MongoClient(host=db_host_mongo_db_atlas)
database = connection[db_name]
collection_from = database[collection_name_to_take_from]
collection_in = database[collection_name_to_put_in]
print("Connected to " + db_name + '\n')


def collect_data_by_day():
    from_date = datetime(2020, 5, 17)
    to_date = datetime(2020, 5, 18)
    data = []
    for post in collection_from.find({"dateTime": {"$gte": from_date, "$lt": to_date}}):
        if post['aqi'] == 40:
            data.append(post)
    amount_of_records = len(data)
    humidity = 0
    temperature = 0
    co = 0
    co2 = 0
    lpg = 0
    smoke = 0
    dust = 0
    aqi = 0
    sensor_id = collection_from.find_one()['id']
    date = str(datetime.date(collection_from.find_one()['dateTime']))
    for record in data:
        humidity += record.get('hum') / amount_of_records
        temperature += record.get('tmp') / amount_of_records
        co += record.get('co') / amount_of_records
        co2 += record.get('co2') / amount_of_records
        lpg += record.get('lpg') / amount_of_records
        smoke += record.get('smk') / amount_of_records
        dust += record.get('dus') / amount_of_records
        aqi += record.get('aqi') / amount_of_records
    updated_record = \
        {
            'id': sensor_id,
            'hum': round(humidity, 2),
            'tmp': round(temperature, 2),
            'co': round(co, 2),
            'co2': round(co2, 2),
            'lpg': round(lpg, 2),
            'smk': round(smoke, 2),
            'dus': round(dust, 2),
            'date': date,
            'aqi': round(aqi),
            '_class': "ua.lviv.iot.secterica.heysmell.model.AirQualityForDays"
        }
    print('done')
    collection_in.insert_one(updated_record)


if __name__ == '__main__':
    sched = BlockingScheduler()
    collect_data_by_day()
    sched.add_job(collect_data_by_day, 'interval', days=1)
    sched.start()

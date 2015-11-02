__author__ = 'aoberegg'
import MySQLdb
import json
from datetime import datetime
from create_database import execute_select
query_path = "../queries/"

def save_visibility_avg_over_categories(db):
    sql = "select ca.id, ca.name, hw.visibility " \
          "from CHECKIN c " \
          "inner join VENUE v on (v.id = c.venue_id) " \
          "inner join CATEGORY ca on (v.category_id = ca.id) " \
          "inner join DAILY_WEATHER dw on (dw.city_id = v.city_id) " \
          "inner join  HOURLY_WEATHER hw on (dw.id = hw.daily_weather_id and c.unix_utc_timestamp = hw.unix_utc_timestamp) " \
          "where hw.visibility is not null and v.controlled is not null "\
              "order by ca.id "
    result = execute_select(sql, db)
    list = []
    for row in result:
        dict = {}
        dict["id"] = row[0]
        dict["category"] = row[1]
        dict["visibility"] = row[2]
        list.append(dict)
    with open(query_path + "visibility_avg.json", mode='w') as file:
        file.write(json.dumps(list))
        file.close()

def save_precip_intensity_avg_over_categories(db):
    sql = "select ca.id, ca.name, hw.precip_intensity " \
          "from CHECKIN c " \
          "inner join VENUE v on (v.id = c.venue_id) " \
          "inner join CATEGORY ca on (v.category_id = ca.id) " \
          "inner join DAILY_WEATHER dw on (dw.city_id = v.city_id) " \
          "inner join  HOURLY_WEATHER hw on (dw.id = hw.daily_weather_id and c.unix_utc_timestamp = hw.unix_utc_timestamp) " \
          "where hw.precip_intensity is not null and v.controlled is not null " \
              "order by ca.id "
    result = execute_select(sql, db)
    list = []
    for row in result:
        dict = {}
        dict["id"] = row[0]
        dict["category"] = row[1]
        dict["precip"] = row[2]
        list.append(dict)
    with open(query_path + "precip_intensity_avg.json", mode='w') as file:
        file.write(json.dumps(list))
        file.close()

def save_humidity_avg_over_categories(db):
    sql = "select ca.id, ca.name, hw.humidity " \
          "from CHECKIN c " \
          "inner join VENUE v on (v.id = c.venue_id) " \
          "inner join CATEGORY ca on (v.category_id = ca.id) " \
          "inner join DAILY_WEATHER dw on (dw.city_id = v.city_id) " \
          "inner join  HOURLY_WEATHER hw on (dw.id = hw.daily_weather_id and c.unix_utc_timestamp = hw.unix_utc_timestamp) " \
          "where hw.humidity is not null and v.controlled is not null "\
              "order by ca.id "
    result = execute_select(sql, db)
    list = []
    for row in result:
        dict = {}
        dict["id"] = row[0]
        dict["category"] = row[1]
        dict["humidity"] = row[2]
        list.append(dict)
    with open(query_path + "humidity_avg.json", mode='w') as file:
        file.write(json.dumps(list))
        file.close()

def save_cloud_cover_avg_over_categories(db):
    sql = "select ca.id, ca.name, hw.cloud_cover " \
          "from CHECKIN c " \
          "inner join VENUE v on (v.id = c.venue_id) " \
          "inner join CATEGORY ca on (v.category_id = ca.id) " \
          "inner join DAILY_WEATHER dw on (dw.city_id = v.city_id) " \
          "inner join  HOURLY_WEATHER hw on (dw.id = hw.daily_weather_id and c.unix_utc_timestamp = hw.unix_utc_timestamp) " \
          "where hw.cloud_cover is not null and v.controlled is not null "\
              "order by ca.id "
    result = execute_select(sql, db)
    list = []
    for row in result:
        dict = {}
        dict["id"] = row[0]
        dict["category"] = row[1]
        dict["cloud_cover"] = row[2]
        list.append(dict)
    with open(query_path + "cloud_cover_avg.json", mode='w') as file:
        file.write(json.dumps(list))
        file.close()

def save_pressure_avg_over_categories(db):
    sql = "select ca.id, ca.name, hw.pressure " \
          "from CHECKIN c " \
          "inner join VENUE v on (v.id = c.venue_id) " \
          "inner join CATEGORY ca on (v.category_id = ca.id) " \
          "inner join DAILY_WEATHER dw on (dw.city_id = v.city_id) " \
          "inner join  HOURLY_WEATHER hw on (dw.id = hw.daily_weather_id and c.unix_utc_timestamp = hw.unix_utc_timestamp) " \
          "where hw.pressure is not null and v.controlled is not null "\
            "order by ca.id"
    result = execute_select(sql, db)
    list = []
    for row in result:
        list.append((row[0], row[1], row[2]))
    with open(query_path + "pressure_avg_nodict.json", mode='w') as file:
        file.write(json.dumps(list))
        file.close()

def save_temperature_over_categories(db):
    sql = "select ca.id, ca.name, hw.temperature " \
          "from CHECKIN c " \
          "inner join VENUE v on (v.id = c.venue_id) " \
          "inner join CATEGORY ca on (v.category_id = ca.id) " \
          "inner join DAILY_WEATHER dw on (dw.city_id = v.city_id) " \
          "inner join  HOURLY_WEATHER hw on (dw.id = hw.daily_weather_id and c.unix_utc_timestamp = hw.unix_utc_timestamp) " \
          "where hw.temperature is not null and v.controlled is not null " \
          "order by ca.id "

    result = execute_select(sql, db)
    list = []
    for row in result:
        dict = {}
        dict["id"] = row[0]
        dict["category"] = row[1]
        dict["temperature"] = row[2]
        list.append(dict)
    with open(query_path + "temperature_over_categories.json", mode='w') as file:
        file.write(json.dumps(list))
        file.close()

def save_avg_temperature_restaurants(db):

    sql = "select ca.id, ca.name, hw.temperature " \
          "from CHECKIN c " \
          "inner join VENUE v on (v.id = c.venue_id) " \
          "inner join CATEGORY ca on (v.category_id = ca.id) " \
          "inner join DAILY_WEATHER dw on (dw.city_id = v.city_id) " \
          "inner join  HOURLY_WEATHER hw on (dw.id = hw.daily_weather_id and c.unix_utc_timestamp = hw.unix_utc_timestamp) " \
          "where hw.temperature is not null and v.controlled is not null and ca.name like \'%Restaurant%\' " \
          "order by ca.id "

    result = execute_select(sql, db)
    list = []
    for row in result:
        dict = {}
        dict["id"] = row[0]
        dict["category"] = row[1]
        dict["temperature"] = row[2]
        list.append(dict)
    with open(query_path + "temperature_restaurants_avg.json", mode='w') as file:
        file.write(json.dumps(list))
        file.close()

def save_windspeed_avg_over_categories(db):
    sql = "select ca.id, ca.name, hw.wind_speed " \
          "from CHECKIN c " \
          "inner join VENUE v on (v.id = c.venue_id) " \
          "inner join CATEGORY ca on (v.category_id = ca.id) " \
          "inner join DAILY_WEATHER dw on (dw.city_id = v.city_id) " \
          "inner join  HOURLY_WEATHER hw on (dw.id = hw.daily_weather_id and c.unix_utc_timestamp = hw.unix_utc_timestamp) " \
          "where hw.wind_speed is not null and v.controlled is not null " \
          "order by ca.id "
    result = execute_select(sql, db)
    list = []
    for row in result:
        dict = {}
        dict["id"] = row[0]
        dict["category"] = row[1]
        dict["windspeed"] = row[2]
        list.append(dict)
    with open(query_path + "windspeed_avg.json", mode='w') as file:
        file.write(json.dumps(list))
        file.close()

def save_icon_over_categories(db):
    sql = "select ca.id, ca.name, hw.icon, count(hw.icon) " \
          "from CHECKIN c " \
          "inner join VENUE v on (v.id = c.venue_id) " \
          "inner join CATEGORY ca on (v.category_id = ca.id) " \
          "inner join DAILY_WEATHER dw on (dw.city_id = v.city_id) " \
          "inner join  HOURLY_WEATHER hw on (dw.id = hw.daily_weather_id and c.unix_utc_timestamp = hw.unix_utc_timestamp) " \
          "where hw.icon is not null and v.controlled is not null " \
          "group by ca.id, hw.icon"
    result = execute_select(sql, db)
    list = []
    for row in result:
        dict = {}
        dict["id"] = row[0]
        dict["category"] = row[1]
        dict["icon_name"] = row[2]
        dict["count"] = row[3]
        list.append(dict)
    with open(query_path + "icon_over_categories.json", mode='w') as file:
        file.write(json.dumps(list))
        file.close()

def save_summary_over_categories(db):
    sql = "select ca.id, ca.name, hw.summary, count(hw.summary) " \
          "from CHECKIN c " \
          "inner join VENUE v on (v.id = c.venue_id) " \
          "inner join CATEGORY ca on (v.category_id = ca.id) " \
          "inner join DAILY_WEATHER dw on (dw.city_id = v.city_id) " \
          "inner join  HOURLY_WEATHER hw on (dw.id = hw.daily_weather_id and c.unix_utc_timestamp = hw.unix_utc_timestamp) " \
          "where hw.summary is not null and v.controlled is not null " \
          "group by ca.id, hw.summary"
    result = execute_select(sql, db)
    list = []
    for row in result:
        dict = {}
        dict["id"] = row[0]
        dict["category"] = row[1]
        dict["summary"] = row[2]
        dict["count"] = row[3]
        list.append(dict)
    with open(query_path + "summary_over_categories.json", mode='w') as file:
        file.write(json.dumps(list))
        file.close()

def save_moonphase_over_categories(db):
    sql = "select ca.id, ca.name, dw.moonphase " \
      "from CHECKIN c " \
      "inner join VENUE v on (v.id = c.venue_id) " \
      "inner join CATEGORY ca on (v.category_id = ca.id) " \
      "inner join DAILY_WEATHER dw on (dw.city_id = v.city_id) " \
      "inner join  HOURLY_WEATHER hw on (dw.id = hw.daily_weather_id and c.unix_utc_timestamp = hw.unix_utc_timestamp) " \
      "where hw.wind_speed is not null and v.controlled is not null " \
      "order by ca.id "
    result = execute_select(sql, db)
    list =[]
    for row in result:
        dict = {}
        dict["id"] = row[0]
        dict["category"] = row[1]
        dict["moonphase"] = row[2]
        list.append(dict)
    with open(query_path + "moonphase_over_categories.json", mode = 'w') as file:
        file.write(json.dumps(list))
        file.close()



if __name__ == "__main__":
    datetime_begin = datetime.now()
    print(datetime.now().strftime("%H:%M:%S.%f"))
    db = MySQLdb.connect(host="localhost", # your host, usually localhost
                 user="aoberegger", # your username
                  passwd="foursquare", # your password
                  db="foursquare") # name of the data base

    visibility = False
    precip_intensity = False
    humidity = False
    cloud_cover = False
    pressure = True
    temperature = False
    temperature_restaurants = False
    windspeed = False
    icon = False
    summary = False
    moonphase = False

    db.set_character_set('utf8')
    print(datetime.now().strftime("%H:%M:%S.%f"))
    print("save_visibility_avg_over_categories(db)")
    if visibility:
        save_visibility_avg_over_categories(db)

    print(datetime.now().strftime("%H:%M:%S.%f"))
    print("save_precip_intensity_avg_over_categories(db)")
    if precip_intensity:
        save_precip_intensity_avg_over_categories(db)

    print(datetime.now().strftime("%H:%M:%S.%f"))
    print("save_humidity_avg_over_categories(db)")
    if humidity:
        save_humidity_avg_over_categories(db)

    print(datetime.now().strftime("%H:%M:%S.%f"))
    print("save_cloud_cover_avg_over_categories(db)")
    if cloud_cover:
        save_cloud_cover_avg_over_categories(db)

    print(datetime.now().strftime("%H:%M:%S.%f"))
    print("save_pressure_avg_over_categories(db)")
    if pressure:
        save_pressure_avg_over_categories(db)

    print(datetime.now().strftime("%H:%M:%S.%f"))
    print("save_temperature_avg_over_categories(db)")
    if temperature:
        save_temperature_over_categories(db)

    print(datetime.now().strftime("%H:%M:%S.%f"))
    print("save_avg_temperature_restaurants(db)")
    if temperature_restaurants:
        save_avg_temperature_restaurants(db)

    print(datetime.now().strftime("%H:%M:%S.%f"))
    print("save_windspeed_avg_over_categories(db)")
    if windspeed:
        save_windspeed_avg_over_categories(db)

    print(datetime.now().strftime("%H:%M:%S.%f"))
    print("save_icon_over_categories(db)")
    if icon:
        save_icon_over_categories(db)

    print(datetime.now().strftime("%H:%M:%S.%f"))
    print("save_summary_over_categories(db)")
    if summary:
        save_summary_over_categories(db)

    print(datetime.now().strftime("%H:%M:%S.%f"))
    print("save_moonphase_over_categories(db)")
    if moonphase:
        save_moonphase_over_categories(db)

    duration = datetime.now() - datetime_begin;
    print("Duration: " + str(duration))
    print("Finished at:")
    print(datetime.now().strftime("%H:%M:%S.%f"))
    db.close()

__author__ = 'aoberegg'

import MySQLdb
import json
import requests
from create_database import execute_select
from create_database import execute_insert_statement
from datetime import datetime

API_KEY = "74bf381ae8e78e8435166cce53d182c9"

checkins_us_json = "../networks/foursquare_checkins_Dingqi Yang/dataset_TIST2015_Checkins.us.json"

def insert_daily(daily_json, city_id, daily_id, db):
    data = daily_json["data"][0]
    sql = "INSERT INTO DAILY_WEATHER VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    if "precipType" not in data.keys():
        precipType = ""
    else:
        precipType = data["precipType"]
    if "windBearing" not in data.keys():
        windBearing = None
    else:
        windBearing = data["windBearing"]
    if "windSpeed" not in data.keys():
        windSpeed = None
    else:
        windSpeed = data["windSpeed"]
    if "cloudCover" not in data.keys():
        cloudCover = None
    else:
        cloudCover = data["windSpeed"]
    if "dewPoint" not in data.keys():
        dewPoint = None
    else:
        dewPoint = data["dewPoint"]
    if "humidity" not in data.keys():
        humidity = None
    else:
        humidity = data["humidity"]
    if "precipIntensity" not in data.keys():
        precipIntensity = None
    else:
        precipIntensity = data["precipIntensity"]
    if "visibility" not in data.keys():
        visibility = None
    else:
        visibility = data["visibility"]
    if "pressure" not in data.keys():
        pressure = None
    else:
        pressure = data["pressure"]
    params = (daily_id, city_id, data["time"], data["summary"], data["icon"], data["sunriseTime"], data["sunsetTime"],
              data["moonPhase"], precipIntensity, precipType , dewPoint, windSpeed,
              windBearing, cloudCover, humidity, pressure, visibility)
    execute_insert_statement(sql, db, params)

def insert_hourly(hourly_json, daily_id, db):
    data = hourly_json["data"]
    for hour in data:
        if "precipType" not in hour.keys():
            precipType = ""
        else:
            precipType = hour["precipType"]
        if "windBearing" not in hour.keys():
            windBearing = 0
        else:
            windBearing = hour["windBearing"]
        if "windSpeed" not in hour.keys():
            windSpeed = 0
        else:
            windSpeed = hour["windSpeed"]
        if "cloudCover" not in hour.keys():
            cloudCover = None
        else:
            cloudCover = hour["cloudCover"]
        if "dewPoint" not in hour.keys():
            dewPoint = None
        else:
            dewPoint = hour["dewPoint"]
        if "humidity" not in hour.keys():
            humidity = None
        else:
            humidity = hour["humidity"]
        if "precipIntensity" not in hour.keys():
            precipIntensity = None
        else:
            precipIntensity = hour["precipIntensity"]
        if "visibility" not in hour.keys():
            visibility = None
        else:
            visibility = hour["visibility"]
        if "pressure" not in hour.keys():
            pressure = None
        else:
            pressure = hour["pressure"]
        if "temperature" not in hour.keys():
            temperature = None
        else:
            temperature = hour["temperature"]

        sql = "INSERT INTO HOURLY_WEATHER VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        params = (daily_id, hour["time"], hour["summary"], hour["icon"], precipIntensity, precipType,
                  dewPoint, windSpeed, windBearing, cloudCover, humidity,
                  pressure, visibility, temperature)
        execute_insert_statement(sql, db, params)


if __name__ == "__main__":
    db = MySQLdb.connect(host="localhost", # your host, usually localhost
                 user="aoberegger", # your username
                  passwd="foursquare", # your password
                  db="foursquare") # name of the data base
    db.set_character_set('utf8')

    sql = "select distinct FROM_UNIXTIME(CHECKIN.unix_utc_timestamp+(CHECKIN.timezone_offset*60)," \
                               " '%Y-%m-%d') as day,  CITY.lat, CITY.lon,  CITY.id  " \
                               "from CHECKIN " \
                               "inner join VENUE on (CHECKIN.venue_id = VENUE.id) " \
                               "inner join CITY on (VENUE.city_id = CITY.id) " \
                               "where" \
                               " not exists (select 1 from DAILY_WEATHER " \
                               "                inner join HOURLY_WEATHER on(HOURLY_WEATHER.daily_weather_id = DAILY_WEATHER.id) " \
                               "                inner join CITY on (CITY.id = DAILY_WEATHER.city_id) " \
                               "                inner join CHECKIN on (CHECKIN.unix_utc_timestamp = HOURLY_WEATHER.unix_utc_timestamp" \
                               "            ) "
    #try:
    results = execute_select(sql, db, None)
    i = 1;
    response_json = {}

    leng = len(results)
    percent = -0.01
    print (leng)
    print(datetime.now().strftime("%H:%M:%S.%f"))
    for row in results:
        try:
            if i % 10 == 0:
                print (str(i) + " of " + str(leng))
                print(datetime.now().strftime("%H:%M:%S.%f"))

            request_url = "https://api.forecast.io/forecast/" + API_KEY + "/" + str(row[1]) + "," + str(row[2]) + "," + str(row[0]) +"T12:00:00?units=si"
            r = requests.get(request_url)
            response_json = r.json()
            insert_daily(response_json["daily"], row[3], i, db)
            insert_hourly(response_json["hourly"], i, db)
            i+= 1
        except Exception as e:
            print (response_json["daily"])
            print (response_json["hourly"])
            print(str(e))


    print ("finished")




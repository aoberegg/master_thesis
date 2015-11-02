__author__ = 'aoberegg'
import MySQLdb
import json
from datetime import datetime
import requests

cities_us_json = "../networks/foursquare_checkins_Dingqi Yang/dataset_TIST2015_cities.us.json"
category_us_json = "../networks/foursquare_checkins_Dingqi Yang/dataset_TIST2015_category.us.json"
checkins_us_json = "../networks/foursquare_checkins_Dingqi Yang/dataset_TIST2015_Checkins.us.json"
pois_us_json = "../networks/foursquare_checkins_Dingqi Yang/dataset_TIST2015_POIs.us.json"

API_KEY = "74bf381ae8e78e8435166cce53d182c9"

def create_database(db, venue = True, checkin = True, hourly_weather = True, daily_weather = True, city = True, category = True):
    cursor = db.cursor()

    # Drop table if it already exist using execute() method.

    #DELETE TABLES
    if checkin:
        cursor.execute("DROP TABLE IF EXISTS CHECKIN")
    if venue:
        cursor.execute("DROP TABLE IF EXISTS VENUE")
    if hourly_weather:
        cursor.execute("DROP TABLE IF EXISTS HOURLY_WEATHER")
    if daily_weather:
        cursor.execute("DROP TABLE IF EXISTS DAILY_WEATHER")
    if city:
        cursor.execute("DROP TABLE IF EXISTS CITY")
    if category:
        cursor.execute("DROP TABLE IF EXISTS CATEGORY")




    # Create table CITY
    if city:
        sql = "CREATE TABLE CITY (id int, country_code CHAR(2), lon FLOAT(9,6), lat FLOAT(9,6),	country_name VARCHAR(256)," \
              " city_type VARCHAR(256), city_name VARCHAR(256), PRIMARY KEY (id))"
        cursor.execute(sql)
    # Create table CATEGORY
    if category:
        sql = "CREATE TABLE CATEGORY (id int, name varchar(256), PRIMARY KEY (id))"
        cursor.execute(sql)
    # Create table VENUE
    if venue:
        sql = "CREATE TABLE VENUE (id CHAR(24), country_code CHAR(2),category_id INT, lat FLOAT(9,6), lon FLOAT(9,6), city_id int, controlled bool, " \
              "PRIMARY KEY (id), FOREIGN KEY (city_id) REFERENCES CITY(id) ON UPDATE CASCADE ON DELETE RESTRICT, " \
              "FOREIGN KEY (category_id) REFERENCES CATEGORY(id) ON UPDATE CASCADE ON DELETE RESTRICT)"
        cursor.execute(sql)
    # Create table CHECKIN
    if checkin:
        sql = "CREATE TABLE CHECKIN (user_id INT, timezone_offset INT, unix_utc_timestamp INT, venue_id CHAR(24), " \
              "PRIMARY KEY (user_id, unix_utc_timestamp, venue_id), FOREIGN KEY (venue_id) REFERENCES VENUE(id) ON UPDATE" \
              "  CASCADE ON DELETE CASCADE)"
        cursor.execute(sql)

    # Create table DAILY_WEATHER
    if daily_weather:
        sql = "CREATE TABLE DAILY_WEATHER(id INT, city_id INT, unix_utc_timestamp INT, summary varchar(256), icon varchar(50), " \
              "sunrise_time INT, sunset_time INT, moonphase FLOAT(3,2), precip_intensity FLOAT(6,4), precip_type varchar(10)," \
              "dew_point FLOAT(6,2), wind_speed FLOAT(6,2), wind_bearing INT, cloud_cover FLOAT(3,2), humidity FLOAT(3,2), pressure FLOAT(6,2)," \
              "visibility FLOAT(4,2), PRIMARY KEY (id), FOREIGN KEY (city_id) REFERENCES CITY(id) ON UPDATE CASCADE ON DELETE RESTRICT)"
        cursor.execute(sql)


    # Create table HOURLY_WEATHER
    if hourly_weather:
        sql = "CREATE TABLE HOURLY_WEATHER( daily_weather_id INT, utc_unix_timestamp INT, summary varchar(256), icon varchar(50)," \
              " precip_intensity FLOAT(6,4), precip_type varchar(10), dew_point FLOAT(6,2), wind_speed FLOAT(6,2), wind_bearing INT," \
              " cloud_cover FLOAT(3,2), humidity FLOAT(3,2), pressure FLOAT(6,2), visibility FLOAT(4,2), temperature FLOAT(5,2), " \
              "PRIMARY KEY(daily_weather_id, utc_unix_timestamp), " \
              " FOREIGN KEY (daily_weather_id) REFERENCES DAILY_WEATHER(id) ON UPDATE CASCADE ON DELETE RESTRICT) "
        cursor.execute(sql)

def execute_insert_statement(sql, db, params = None):
    cursor = db.cursor()
    try:
        if params is not None:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        db.commit()
    except Exception as e:
        print("Params:")
        print (params)
        print("SQL:")
        print (sql)
        print("Error:")
        print(str(e))
        db.rollback()

def execute_select(sql, db, params = None):
    try:
        cursor = db.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        return cursor.fetchall()
    except MySQLdb.Error as e:
        print(str(e))



def insert_cities(db):
    with open(cities_us_json, mode="r") as cities_file:
        cities_dict = json.loads(cities_file.read())
        cities_file.close()

    for city in cities_dict["cities"]:
        params = (int(city["id"]), str(city["country_code"]), float(city["long"]),
       float(city["lat"]), str(city["country_name"]), str(city["city_type"]),
       str(city["name"]))
        sql = "INSERT INTO CITY(id, country_code, lon, lat, country_name, city_type, city_name) VALUES " \
              "(%s, %s, %s, %s, %s, %s, %s)"
        execute_insert_statement(sql, db, params)
    del cities_dict

def insert_categories(db):
    with open(category_us_json) as category_file:
        category_dict = json.loads(category_file.read())
        category_file.close()

    for key,value in category_dict.iteritems():
        params = (int(key), str(value))
        sql = "INSERT INTO CATEGORY(id, name) VALUES (%s, %s)"
        execute_insert_statement(sql, db, params)
    del category_dict

def get_category_id(category, db):
    params = [category]
    sql = "select id from CATEGORY where name = %s" #\'"+category+"\'"
    results = execute_select(sql, db, params)
    for row in results:
        id = row[0]
        return id

def insert_venues(db):
    with open(pois_us_json) as venue_file:
        venue_dict = json.loads(venue_file.read())
        venue_file.close()
    i = 0
    leng = len(venue_dict["pois"])
    for venue in venue_dict["pois"]:
        if i % 10000 == 0:
            percent = (i / leng) * 100
            print("<----------------------------->")
            print("VENUES")
            print(datetime.now().strftime("%H:%M:%S.%f"))
            print(str(percent)+"%")
            print(leng)
            print(i)
            print(">-------------------------------<")
        category_id = get_category_id(str(venue["category"]), db)
        params = (str(venue["venue"]), str(venue["country"]), int(category_id), float(venue["lat"]), float(venue["long"]),
                  int(venue["city_id"]))
        sql = "INSERT INTO VENUE(id, country_code, category_id, lat, lon, city_id) VALUES (%s, %s, %s, %s, %s, %s)"
        execute_insert_statement(sql, db, params)
        i += 1
    del venue_dict

def insert_checkins(db):
    with open(checkins_us_json) as checkin_file:
        checkin_dict = json.loads(checkin_file.read())
    i = 0
    leng = len(checkin_dict["checkins"])
    for checkin in checkin_dict["checkins"]:
        if i % 10000 == 0:
            percent = (i / leng) * 100
            print("<----------------------------->")
            print("CHECKINS")
            print(datetime.now().strftime("%H:%M:%S.%f"))
            print(str(percent)+"%")
            print (leng)
            print(i)
            print(">-------------------------------<")
        params = (int(checkin["user"]), int(checkin["timezone_offset"]), int(checkin["utc_unix_timestamp"]), str(checkin["venue"]))
        sql = "INSERT INTO CHECKIN(user_id, timezone_offset, unix_utc_timestamp, venue_id) VALUES ( %s, %s, %s, %s )"
        execute_insert_statement(sql, db, params)
        i+=1
    del checkin_dict

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

def insert_and_crawl_hourly_and_daily_weather(db):
    sql_orig = "select distinct " \
      " FROM_UNIXTIME(CHECKIN.unix_utc_timestamp+(CHECKIN.timezone_offset*60), '%Y-%m-%d') as day, " \
      " CITY.lat, " \
      " CITY.lon, " \
      " CITY.id " \
      " from CHECKIN " \
      " inner join VENUE on (CHECKIN.venue_id = VENUE.id) " \
      " inner join CITY on (VENUE.city_id = CITY.id) "

    sql = "select distinct FROM_UNIXTIME(CHECKIN.unix_utc_timestamp+(CHECKIN.timezone_offset*60)," \
                               " '%Y-%m-%d') as day,  CITY.lat, CITY.lon,  CITY.id  " \
                               "from CHECKIN " \
                               "inner join VENUE on (CHECKIN.venue_id = VENUE.id) " \
                               "inner join CITY on (VENUE.city_id = CITY.id) " \
                               "where" \
                               " not exists (select 1 from DAILY_WEATHER " \
                               "                inner join HOURLY_WEATHER on(HOURLY_WEATHER.daily_weather_id = DAILY_WEATHER.id) " \
                               "                inner join CITY on (CITY.id = DAILY_WEATHER.city_id) " \
                               "                inner join CHECKIN on (CHECKIN.unix_utc_timestamp = HOURLY_WEATHER.unix_utc_timestamp)" \
                               "            ) "
    #try:
    results = execute_select(sql, db, None)
    max_daily_id = execute_select("select max(id) from DAILY_WEATHER", db, None)[0][0]
    i = max_daily_id+1;
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


if __name__ == "__main__":
    datetime_begin = datetime.now()
    print(datetime_begin.strftime("%H:%M:%S.%f"))
    db = MySQLdb.connect(host="localhost", # your host, usually localhost
                 user="aoberegger", # your username
                  passwd="foursquare", # your password
                  db="foursquare") # name of the data base

    db.set_character_set('utf8')
    venue = True
    checkin = False
    weather = False
    city = False
    category = False
    print("CREATE DATABASE")
    create_database(db, venue, checkin, weather, weather, city, category) #Comment in if you want to create Database from scratch
    print("INSERT CITIES")
    if city:
        insert_cities(db) #Comment in if you want to insert the cities
    print("INSERT CATEGORIES")
    if category:
        insert_categories(db) #Comment in if you want to insert the categories
    print("INSERT VENUES")
    if venue:
        insert_venues(db) #Comment in if you want to insert the venues
    print("INSERT CHECKINS")
    if checkin:
        insert_checkins(db) #Comment in if you want to insert the checkins
    print("INSERT WEATHER")
    if weather:
        insert_and_crawl_hourly_and_daily_weather(db) #Comment in if you want to insert and crawl the weather data

    duration = datetime.now() - datetime_begin;
    print("Duration: " + str(duration))
    print("Finished at:")
    print(datetime.now().strftime("%H:%M:%S.%f"))
    db.close()

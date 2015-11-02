# this script converts the raw .txt file containing the checkins (crawled from Dingqui Yang) to
#  a json file and sorts out all venues not being in the timezone of the US
__author__ = 'aoberegg'


#import MySQLdb
import json
from pprint import pprint
from geopy.distance import vincenty

checkins = "../networks/foursquare_checkins_Dingqi Yang/dataset_TIST2015_Checkins.txt"
checkins_json = "../networks/foursquare_checkins_Dingqi Yang/dataset_TIST2015_Checkins.json"
checkins_us_json = "../networks/foursquare_checkins_Dingqi Yang/dataset_TIST2015_Checkins.us.json"
checkins_us_json_small = "../networks/foursquare_checkins_Dingqi Yang/dataset_TIST2015_Checkins.us.small.json"
pois = "../networks/foursquare_checkins_Dingqi Yang/dataset_TIST2015_POIs.txt"
pois_json = "../networks/foursquare_checkins_Dingqi Yang/dataset_TIST2015_POIs.json"
pois_us_json = "../networks/foursquare_checkins_Dingqi Yang/dataset_TIST2015_POIs.us.json"
user_number_checkins = "../networks/foursquare_checkins_Dingqi Yang/dataset_TIST2015_user_number_checkins.us.json"
cities = "../networks/foursquare_checkins_Dingqi Yang/dataset_TIST2015_Cities.txt"
cities_json = "../networks/foursquare_checkins_Dingqi Yang/dataset_TIST2015_cities.json"
cities_us_json = "../networks/foursquare_checkins_Dingqi Yang/dataset_TIST2015_cities.us.json"
city_poi_us_json = "../networks/foursquare_checkins_Dingqi Yang/dataset_TIST2015_cities_poi.us.json"
category_us_json = "../networks/foursquare_checkins_Dingqi Yang/dataset_TIST2015_category.us.json"



from time import *
from datetime import datetime

def utctime_to_unix_full_hour(utc_time):
    time = datetime.strptime(utc_time,"%a %b %d %H:%M:%S %z %Y")
    time = time.replace(second = 0, minute = 0)
    unix_timestamp = int(time.timestamp())
    return unix_timestamp


def create_dict_of_array(array, name, attribute_list, json_file, comparing_dict = None,comparing_key = None ,comparing_value = None):
    print("start preparing dict")
    file = open(json_file, 'w')
    file.write("{\""+name+"\":[")
    i = 0
    first = True
    while array:
        entry = array.pop()
        entry = entry.split("\t")
        entry_dict = {}
        if i % 100000 == 0:
            lt = localtime()
            print(strftime("Datum und Zeit: %c", lt))
            print(i)
        if not comparing_dict or comparing_dict[entry[1]][comparing_key] == comparing_value:
            if not first:
                file.write(",\n")
            first = False
            entry_counter = 0
            for attribute in attribute_list:
                entry_dict[attribute] = entry[entry_counter].rstrip('\n')
                entry_counter += 1

            file.write(json.dumps(entry_dict))

        i += 1
    file.write("]}")
    file.close()
    print("end preparing dict")

def find_city_id_to_lat_long(city_array, lat, long):
    closest_id = 0
    min_dist = -1
    for city in city_array:
        dist = vincenty((lat,long), (city["lat"], city["long"])).miles
        if dist < 0:
            return city["id"]
        elif dist < min_dist or min_dist == -1:
            min_dist = dist
            closest_id = city["id"]
    if min_dist > 20:
        print ("min dist: " + str(min_dist))
    return closest_id

def dict_out_of_pois_txt():
    #the following write a dict file out of the pois txt file from dingqi yang:
    with open(pois) as poi_file:
        poi_array = poi_file.readlines()
    create_dict_of_array(poi_array, "pois", ["venue","lat","long","category","country"],pois_json)

def dict_out_of_checkins_txt_just_us():
    #the following writes a dict file out of the checkins txt file from dingqi yang (just those with country us)
    with open(checkins) as checkin_file:
        checkin_array = checkin_file.readlines()
    with open(pois_json) as poi_json_file:
        pois_dict = json.loads(poi_json_file.read())
        poi_json_file.close()
    pois_hashed_list = dict( (poi['venue'],poi) for poi in pois_dict["pois"])
    create_dict_of_array(checkin_array, "checkins", ["user", "venue", "utc_time", "timezone_offset"], checkins_json, pois_hashed_list, "country", "US")

def dict_out_of_cities_txt_just_us():
    # the following creates a json file for the cities.txt file and deletes cities that are not in the us
    with open(cities) as cities_file:
        city_array = cities_file.readlines()
        cities_file.close()
    create_dict_of_array(city_array, "cities", ["name","lat","long","country_code","country_name", "city_type"],cities_json)
    with open(cities_json) as cities_file:
        city_dict = json.loads(cities_file.read())
        cities_file.close()
    city_list_us = [city for city in city_dict["cities"] if city["country_code"] == 'US']
    i = 0
    for city in city_list_us:
        city["id"] = i
        i += 1
    city_dict["cities"] = city_list_us
    with open(cities_us_json, mode="w") as cities_file:
        cities_file.write(json.dumps(city_dict))
        cities_file.close()

def delete_non_us_from_poi_dict():
    #the following deletes the non us entries out of the poi dict file
    with open(pois_json) as poi_json_file:
        pois_dict = json.loads(poi_json_file.read())
        pois_us_json_file = open(pois_us_json, 'w')
        pois_us_json_file.write("{\"pois\":[")
        first = True
        for poi in pois_dict["pois"]:
            if poi["country"] == 'US':
                if not first:
                    pois_us_json_file.write(",\n")
                first = False
                pois_us_json_file.write(json.dumps(poi))
        pois_us_json_file.write("]}")

def add_unix_timestamp_to_checkins_dict():
    #the following reads the checkins_us_json_file and adds the unix timestamp to it
    filename = checkins_us_json
    with open(filename,mode = 'r') as checkins_us_json_file:
        checkins_us_dict = json.loads(checkins_us_json_file.read())
    checkins_us_json_file.close()
    i = 0
    for checkin in checkins_us_dict["checkins"]:
        if i % 100000 == 0:
            print(i)
        i+=1
        checkin["utc_unix_timestamp"] = utctime_to_unix_full_hour(checkin["utc_time"])
    with open(filename,mode = 'w') as checkins_us_json_file:
        checkins_us_json_file.write(json.dumps(checkins_us_dict))

def add_city_id_to_pois():
    # the following code tries to find a city to every poi id
    with open(cities_us_json) as cities_file:
        city_us_dict = json.loads(cities_file.read())
        cities_file.close()

    with open(pois_us_json) as pois_file:
        pois_us_dict = json.loads(pois_file.read())
        pois_file.close()
    leng = len(pois_us_dict["pois"])
    i = 0
    for poi in pois_us_dict["pois"]:
        if i % 10000 == 0:
            print("<----------------------------->")
            print(datetime.now().strftime("%H:%M:%S.%f"))
            print(str(i/leng*100)+"%")
            print(">-------------------------------<")
            print (i)
        poi["city_id"] = find_city_id_to_lat_long(city_us_dict["cities"], poi["lat"], poi["long"])
        i +=1

    with open(pois_us_json, mode="w") as pois_file:
        pois_file.write(json.dumps(pois_us_dict))
        pois_file.close()

def min_distance_between_all_cities():
    min_dist = -1
    with open(cities_us_json) as cities_file:
        city_us_dict = json.loads(cities_file.read())
        cities_file.close()
    for city in city_us_dict["cities"]:
        for city2 in city_us_dict["cities"]:
            if city["id"] != city2["id"]:
                test = city["lat"]
                dist = vincenty((city["lat"],city["long"]), (city2["lat"],city2["long"])).miles
                if dist < min_dist or min_dist == -1:
                    min_dist = dist
    return dist



if __name__ == "__main__":


    #dict_out_of_pois_txt()

    #dict_out_of_checkins_txt_just_us()

    #delete_non_us_from_poi_dict()

    #dict_out_of_cities_txt_just_us()

    #add_unix_timestamp_to_checkins_dict()

    #add_city_id_to_pois()

    #print(min_distance_between_all_cities())

    print("finished")

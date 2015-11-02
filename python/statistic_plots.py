__author__ = 'aoberegg'


import json
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
from itertools import cycle
import math
import os
import errno
import collections

query_path = "../queries/"
plot_path = "../plots/"
temperature_restaurants_avg = "temperature_restaurants_avg.json"
temperature_over_categories = "temperature_over_categories.json"
visibility_over_categories = "visibility_avg.json"
precip_intensity_over_categories = "precip_intensity_avg.json"
humiditiy_over_categories = "humidity_avg.json"
cloud_cover_over_categories = "cloud_cover_avg.json"
pressure_over_categories = "pressure_avg.json"
pressure_over_categories_small = "pressure_avg_small.json"
windspeed_over_categories = "windspeed_avg.json"
moonphase_over_categories = "moonphase_over_categories.json"

def plot_hist_over_category(category_names_avgs_sem_triple_list, plt_number, x_label, groups, printse):
    height_factor = 0.5
    ind = np.linspace(0,len(category_names_avgs_sem_triple_list)*height_factor, num = len(category_names_avgs_sem_triple_list))
    width = 0.25
    fig = plt.figure(figsize=(15.5, 10),dpi=800)
    plot = fig.add_subplot(111)
    plot.tick_params(axis='y', which='major', labelsize= 10 )
    plot.tick_params(axis='x', which='major', labelsize= 10 )
    length = len(category_names_avgs_sem_triple_list)
    l = 0
    it = cycle(["#CCD64B","#C951CA","#CF4831","#90D0D2","#33402A","#513864",
                "#C84179","#DA983D","#CA96C4","#53913D","#CEC898","#70D94C",
                "#CB847E","#796ACB","#74D79C","#60292F","#6C93C4","#627C76",
                "#865229","#838237"])
    color=[next(it) for i in range(length)]
    if printse:
        p1 = plt.barh(ind,  [x[1] for x in category_names_avgs_sem_triple_list], color=color,align='center', height= height_factor, xerr= [x[2] for x in category_names_avgs_sem_triple_list])
    else:
        p1 = plt.barh(ind,  [x[1] for x in category_names_avgs_sem_triple_list], color=color,align='center', height= height_factor)
    plt.yticks(ind, [x[0] for x in category_names_avgs_sem_triple_list])
    plt.xlabel(x_label)
    plt.ylabel("Categories")
    plt.subplots_adjust(bottom=0.15, left=0.14,right=0.95,top=0.95)
    plt.ylim([ind.min()- height_factor, ind.max() + height_factor])
    plt.xlim(min([x[1] for x in category_names_avgs_sem_triple_list])-height_factor, max([x[1] for x in category_names_avgs_sem_triple_list])+height_factor)
    try:
        os.makedirs(plot_path+x_label)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise
    print("da wirds gespeichert:")
    print(plot_path+x_label+"/"+str(plt_number)+"groups_"+str(groups))
    plt.savefig(plot_path+x_label+"/"+str(plt_number)+"groups_"+str(groups))

    plt.close()

def splitter(lst,n):
    return [ lst[i::n] for i in range(n) ]

def plot_seperated_hist_over_category(category_list, x_label, cnt, printse = False):
    i = 0
    splitter_list = list(splitter(category_list, cnt))
    for split in splitter_list:
        plot_hist_over_category(sorted(split, key=lambda category: category[1], reverse = True),i, x_label, cnt, printse)
        i+=1

def calculate_avg_and_sem_over_categories(over_categories_list, key):
    category_list = []
    print(datetime.now().strftime("%H:%M:%S.%f"))
    result = collections.defaultdict(list)

    for d in over_categories_list:
        result[d['id']].append(d)

    result_list = result.values()
    for result in result_list:
        if len(result) < 50:
            continue
        feature_list = [x[key] for x in result]
        avg = np.average(feature_list)
        semp = stats.sem(feature_list)
        triple = (result[0]["category"], avg, semp)
        category_list.append(triple)
    print(datetime.now().strftime("%H:%M:%S.%f"))
    return category_list

def plot_avg_se_over_categories(file_name, feature, feature_key, max_categories = 20, category_list = None, printse = False):

    if category_list is None:
        with open(query_path + file_name) as temp_file:
            over_categories_list = json.loads(temp_file.read())
            temp_file.close()
        category_list = calculate_avg_and_sem_over_categories(over_categories_list, feature_key)
    plot_seperated_hist_over_category(category_list, feature, int(math.ceil(float(float(len(category_list))/float(max_categories)))), printse)
    return category_list

if __name__ == "__main__":
    visibility = True
    precip_intensity = True
    humidity = True
    cloud_cover = True
    pressure = True
    temperature = True
    temperature_restaurants = True
    windspeed = True
    moonphase = True
    icon = True
    summary = True

    if visibility:
        print("Visibility")
        print(datetime.now().strftime("%H:%M:%S.%f"))
        category_list = plot_avg_se_over_categories(visibility_over_categories, "Visibility", "visibility")
        plot_avg_se_over_categories(visibility_over_categories, "Visibility", "visibility",600, category_list,printse= False)
    if precip_intensity:
        print("Precip intensity")
        print(datetime.now().strftime("%H:%M:%S.%f"))
        category_list = plot_avg_se_over_categories(precip_intensity_over_categories, "Precip intensity", "precip")
        plot_avg_se_over_categories(precip_intensity_over_categories, "Precip intensity", "precip",600, category_list,printse= False)
    if humidity:
        print("Humidity")
        print(datetime.now().strftime("%H:%M:%S.%f"))
        category_list = plot_avg_se_over_categories(humiditiy_over_categories, "Humidity", "humidity")
        plot_avg_se_over_categories(humiditiy_over_categories, "Humidity", "humidity",600, category_list,printse= False)
    if cloud_cover:
        print("Cloud cover")
        print(datetime.now().strftime("%H:%M:%S.%f"))
        category_list = plot_avg_se_over_categories(cloud_cover_over_categories, "Cloud cover", "cloud_cover")
        plot_avg_se_over_categories(cloud_cover_over_categories, "Cloud cover", "cloud_cover",600, category_list, printse=False)
    if pressure:
        print("Pressure")
        print(datetime.now().strftime("%H:%M:%S.%f"))
        category_list = plot_avg_se_over_categories(pressure_over_categories, "Pressure", "pressure")
        plot_avg_se_over_categories(pressure_over_categories, "Pressure", "pressure",600, category_list, printse=False)
    if windspeed:
        print("Windspeed")
        print(datetime.now().strftime("%H:%M:%S.%f"))
        category_list = plot_avg_se_over_categories(windspeed_over_categories, "Windspeed", "windspeed")
        plot_avg_se_over_categories(windspeed_over_categories, "Windspeed", "windspeed",600, category_list,printse= False)
    if temperature:
        print("Temperature")
        print(datetime.now().strftime("%H:%M:%S.%f"))
        category_list = plot_avg_se_over_categories(temperature_over_categories, "Temperature", "temperature")
        plot_avg_se_over_categories(temperature_over_categories, "Temperature", "temperature",600, category_list,printse= False)
    if temperature_restaurants:
        print("Temperature restaurants")
        print(datetime.now().strftime("%H:%M:%S.%f"))
        category_list = plot_avg_se_over_categories(temperature_restaurants_avg, "Temperature Restaurants", "temperature", 600)
        plot_avg_se_over_categories(temperature_restaurants_avg, "Temperature Restaurants", "temperature",category_list, printse= False)
    if moonphase:
        print("Moonphase")
        print(datetime.now().strftime("%H:%M:%S.%f"))
        category_list = plot_avg_se_over_categories(moonphase_over_categories, "Moonphase", "moonphase", 600)
        plot_avg_se_over_categories(moonphase_over_categories, "Moonphase", "moonphase", category_list = category_list, printse=False)

__author__ = 'aoberegg'
import MySQLdb
import json
import requests
import time
from datetime import datetime
import numpy
from matplotlib import pylab
from create_database import execute_select
from scipy import stats
import numpy as np
import collections



if __name__ == "__main__":
    dict_list =  [{'event': 0, 'voltage': 1, 'time': 0},
{'event': 0, 'voltage': 2, 'time': 1},
{'event': 1, 'voltage': 1, 'time': 2},
{'event': 1, 'voltage': 2, 'time': 3},
{'event': 2, 'voltage': 1, 'time': 4},
{'event': 2, 'voltage': 2, 'time': 5},
]


    result = collections.defaultdict(list)

    for d in dict_list:
        result[d['event']].append(d)

    result_list = result.values()
    print result_list
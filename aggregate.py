# -*- coding: utf-8 -*-
"""
Created on Thu Dec 13 00:42:28 2018
This file is for getting a sheet containing conflicts and their start date, end date, location, country and continent information.
@author: leo
"""

import numpy as np
import pandas as pd
import sys
import re
import random
import datetime
import calendar
from os import mkdir
main_path = "D:/learning/Arash/war_participants/"
try:
    mkdir(main_path)
except FileExistsError:
    pass


selected_qid_time_cat_3=pd.read_csv(main_path+'output/selected_qid_time_cat_3_original_still.csv')
qid_location_continent_country=pd.read_csv(main_path+'output/qid_location_continent_country.csv')

###########aggregate time and location##############
qid_time_location=pd.merge(selected_qid_time_cat_3,qid_location_continent_country,on='qid',how='left')
qid_time_location=qid_time_location[qid_time_location['qid']!=28913533]#no wikipedia at all about this military operation
qid_time_location=qid_time_location[qid_time_location['qid']!=220878]#greek myths
qid_time_location=qid_time_location[qid_time_location['qid']!=11499492]#this is a book with instance "war"
qid_time_location=qid_time_location[qid_time_location['qid']!=9385130]#War_in_mythology
qid_time_location=qid_time_location[qid_time_location['qid']!=1346923]#a method
qid_time_location=qid_time_location[qid_time_location['qid']!=2453944]# a myth
qid_time_location=qid_time_location[qid_time_location['qid']!=3114946]# a war in a movie
qid_time_location=qid_time_location[qid_time_location['qid']!=9385130]# war in Heaven
qid_time_location=qid_time_location[qid_time_location['qid']!=6073630]#a doctrine
qid_time_location=qid_time_location[qid_time_location['qid']!=7138638]#social compaign
qid_time_location=qid_time_location[qid_time_location['qid']!=10892253]#a political event
qid_time_location=qid_time_location[qid_time_location['qid']!=7889241]#United States Air Force in the United Kingdom
qid_time_location=qid_time_location[qid_time_location['qid']!=7820941]#not a war
qid_time_location=qid_time_location[qid_time_location['qid']!=10926148]#an army
qid_time_location=qid_time_location[qid_time_location['qid']!=10926146]#an army
qid_time_location=qid_time_location[qid_time_location['qid']!=10926149]#an army
qid_time_location=qid_time_location[qid_time_location['qid']!=16208438]# an assassination
qid_time_location=qid_time_location[qid_time_location['qid']!=15915571]#not a war
qid_time_location=qid_time_location[qid_time_location['qid']!=16987908]#video game
qid_time_location=qid_time_location[qid_time_location['qid']!=633862] #cold war
qid_time_location=qid_time_location[qid_time_location['qid']!=17143698] #Social media
qid_time_location=qid_time_location[qid_time_location['qid']!=8683] #Cold War
qid_time_location=qid_time_location[qid_time_location['qid']!=4048961] #Not a war
qid_time_location=qid_time_location[qid_time_location['qid']!=47496742]
qid_time_location=qid_time_location[qid_time_location['qid']!=3778658]
qid_time_location=qid_time_location[qid_time_location['qid']!=27643296]
qid_time_location=qid_time_location[qid_time_location['qid']!=829823]


qid_time_location.to_csv(main_path+"output/qid_time_location.csv",index=False)

#xxxxxxxxx=qid_time_location[qid_time_location['qid']==1192616]

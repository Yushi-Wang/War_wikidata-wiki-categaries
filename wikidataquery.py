# -*- coding: utf-8 -*-
"""
Created on Thu Dec 13 18:12:03 2018

This file is for obtaining the conflicts' location, time, participant and "partof" information by using wikiquery.
@author: leo
"""

from SPARQLWrapper import SPARQLWrapper
import multiprocessing as mp
import re
import csv
import datetime
import json
import pandas as pd
import calendar
from os import mkdir
import gc
import numpy as np
now = datetime.datetime.now()
date_part = calendar.month_name[now.month].lower() + str(now.year)
sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
main_path = "D:/learning/Arash/war_participants/material/wikiTSV_" + date_part + '/'
try:
    mkdir(main_path)
except FileExistsError:
    pass
qid_location_path = main_path + "qid_location.csv"
qid_time_path = main_path + "qid_time.csv"
qid_participant_path = main_path + "qid_participant.csv"
qid_partof_path = main_path + "qid_partof.csv"



def try_until_timeout(sparql_object, timeout_tries=100):
    iteration = 0
    try_again = True
    while try_again:
        try:
            query_result = sparql_object.queryAndConvert()
            try_again = False
            return query_result
        except json.decoder.JSONDecodeError:
            print("Timeout!, number " + str(iteration))
            iteration += 1
            if iteration > timeout_tries:
                print("Max timeout reached.")
                return
            
qid_time_list = list()
sparql.setQuery("""
  SELECT DISTINCT ?qid ?qidLabel ?start ?end ?point_in_time ?instance_of ?instance_ofLabel WHERE {
  ?qid (wdt:P31/wdt:P279*) wd:Q180684.
  OPTIONAL { ?qid wdt:P580 ?start. }
  OPTIONAL { ?qid wdt:P582 ?end. }
  OPTIONAL { ?qid wdt:P585 ?point_in_time. }
  OPTIONAL { ?qid wdt:P31 ?instance_of. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
 
}
""")
sparql.setReturnFormat('json')
sparql.setMethod("POST")
results = try_until_timeout(sparql)
for single_entry in results['results']['bindings']:
    qid= single_entry['qid']['value']
    qidLabel= single_entry['qidLabel']['value']
    if  'start' in single_entry:
        start= single_entry['start']['value']
    else:
        start=0
    if 'end' in single_entry:
        end= single_entry['end']['value']     
    else:
        end=0
    if 'point_in_time' in single_entry:
        point_in_time= single_entry['point_in_time']['value']
    else:
        point_in_time=0   
    instance_of= single_entry['instance_of']['value']
    instance_ofLabel= single_entry['instance_ofLabel']['value']
    qid_time_list.append([qid,qidLabel , start,end,point_in_time,instance_of,instance_ofLabel])
qid_time=pd.DataFrame(pd.DataFrame(qid_time_list))
qid_time.columns = ['qid','qidLabel' , 'start','end','point_in_time','instance_of','instance_ofLabel']   
qid_time=qid_time.replace(0,np.nan) 
qid_time.to_csv(qid_time_path, index=False)
del qid,qidLabel , start,end,point_in_time,instance_of,instance_ofLabel,qid_time_list
gc.collect()      

qid_location_list = list()
sparql.setQuery("""
    SELECT DISTINCT ?qid ?qidLabel ?location ?locationLabel ?instance_of ?instance_ofLabel WHERE {
  ?qid (wdt:P31/wdt:P279*) wd:Q180684.
  OPTIONAL { ?qid wdt:P276 ?location. }
  OPTIONAL { ?qid wdt:P31 ?instance_of. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}

""")
sparql.setReturnFormat('json')
sparql.setMethod("POST")
results = try_until_timeout(sparql)
for single_entry in results['results']['bindings']:
    qid= single_entry['qid']['value']
    qidLabel= single_entry['qidLabel']['value']
    if  'location' in single_entry:
        location= single_entry['location']['value']
    else:
        location=0   
    if  'locationLabel' in single_entry:
        locationLabel= single_entry['locationLabel']['value']
    else:
        locationLabel=0
    instance_of= single_entry['instance_of']['value']
    instance_ofLabel= single_entry['instance_ofLabel']['value']
    qid_location_list.append([qid,qidLabel,location,locationLabel,instance_of,instance_ofLabel])
qid_location=pd.DataFrame(pd.DataFrame(qid_location_list))
qid_location.columns = ['qid','qidLabel' , 'location','locationLabel','instance_of','instance_ofLabel'] 
qid_location=qid_location.replace(0,np.nan) 
qid_location.to_csv(qid_location_path, index=False)
del qid,qidLabel,location,locationLabel,instance_of,instance_ofLabel,qid_location_list
gc.collect()


qid_participant_list = list()
sparql.setQuery("""
   SELECT DISTINCT ?qid ?qidLabel ?participant ?participantLabel ?instance_of ?instance_ofLabel WHERE {
  ?qid (wdt:P31/wdt:P279*) wd:Q180684.
  OPTIONAL { ?qid wdt:P710 ?participant. }
  OPTIONAL { ?qid wdt:P31 ?instance_of. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
 
}

""")
sparql.setReturnFormat('json')
sparql.setMethod("POST")
results = try_until_timeout(sparql)
for single_entry in results['results']['bindings']:
    qid= single_entry['qid']['value']
    qidLabel= single_entry['qidLabel']['value']
    if  'participant' in single_entry:
        participant= single_entry['participant']['value']
    else:
        participant=0
    if  'participantLabel' in single_entry:
        participantLabel= single_entry['participantLabel']['value']
    else:
        participantLabel=0
    instance_of= single_entry['instance_of']['value']
    instance_ofLabel= single_entry['instance_ofLabel']['value']
    qid_participant_list.append([qid,qidLabel,participant,participantLabel,instance_of,instance_ofLabel])
qid_participant=pd.DataFrame(pd.DataFrame(qid_participant_list))
qid_participant.columns=['qid','qidLabel','participant','participantLabel','instance_of','instance_ofLabel']
qid_participant=qid_participant.replace(0,np.nan) 
qid_participant.to_csv(qid_participant_path, index=False)
del qid,qidLabel,participant,participantLabel,instance_of,instance_ofLabel,qid_participant_list
gc.collect()


qid_partof_list= list()
sparql.setQuery("""
   SELECT DISTINCT ?qid ?qidLabel ?part_of ?part_ofLabel ?instance_of ?instance_ofLabel  WHERE {
  ?qid (wdt:P31/wdt:P279*) wd:Q180684.
  OPTIONAL { ?qid wdt:P361 ?part_of. }
  OPTIONAL { ?qid wdt:P31 ?instance_of. }
   SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}


""")
sparql.setReturnFormat('json')
sparql.setMethod("POST")
results = try_until_timeout(sparql)
for single_entry in results['results']['bindings']:
    qid= single_entry['qid']['value']
    qidLabel= single_entry['qidLabel']['value']
    if  'part_of' in single_entry:
        part_of= single_entry['part_of']['value']
    else:
        part_of=0
    if  'part_ofLabel' in single_entry:
        part_ofLabel= single_entry['part_ofLabel']['value']
    else:
        part_ofLabel=0
    instance_of= single_entry['instance_of']['value']
    instance_ofLabel= single_entry['instance_ofLabel']['value']
    qid_partof_list.append([qid,qidLabel,part_of,part_ofLabel,instance_of,instance_ofLabel])
qid_partof=pd.DataFrame(pd.DataFrame(qid_partof_list))
qid_partof.columns=['qid','qidLabel','part_of','part_ofLabel','instance_of','instance_ofLabel']
qid_partof=qid_partof.replace(0,np.nan) 
qid_partof.to_csv(qid_partof_path, index=False)
del qid,qidLabel,part_of,part_ofLabel,instance_of,instance_ofLabel,qid_partof_list
gc.collect()
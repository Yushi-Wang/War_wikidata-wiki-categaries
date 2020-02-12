# -*- coding: utf-8 -*-
"""
Created on Wed Feb 12 18:52:58 2020

This is a new file for obtaining the conflicts' location, time, participant and "partof" information by using wikiquery.
@author: leo
"""


from SPARQLWrapper import SPARQLWrapper
import numpy as np
import multiprocessing as mp
import re
import csv
import datetime, time
import json
import pandas as pd
import calendar
import os
import gc
now = datetime.datetime.now()
date_part = calendar.month_name[now.month].lower() + str(now.year)
main_path = "D:/learning/Arash/war_participants/material/wikiTSV_" + date_part + '/'
qid_time_path = os.path.join(main_path,"qid_time.csv")
qid_location_path = os.path.join(main_path,"qid_location.csv")
qid_participant_path = os.path.join(main_path,"qid_participant.csv")
qid_partof_path = os.path.join(main_path,"qid_partof.csv")

sparql = SPARQLWrapper("https://query.wikidata.org/sparql")

get_lang_list = False
get_human_qid = False
get_subclasses = False
get_gender = False
get_parents = True
get_children = True
get_siblings_spouses = False
get_location = False
get_dates = False
get_religion = False
get_occupations = False
get_encyclopedias = False
get_cbdb_id = False
get_countries = False
get_yearPages = False
get_datePages = False
get_occCategories = False
get_catMtCountry = False
get_catCombCountry = False
get_qidCat_catContainsHum = False
get_participant = False
get_capital = False



def extract_qid(entry, name='qid'):
    try:
        return entry[name]['value'].replace('http://www.wikidata.org/entity/Q', '')
    except KeyError:
        return np.nan

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

def get_statement(query, timeout_tries=100, exclude_unknown=True):
    query = query.replace('\n', '')
    column_names = re.findall(R'SELECT(.*?)WHERE', query)[0]
    column_names = column_names.replace(' ', '')
    column_names = re.findall('\?([^\?]*)', column_names)
    print(query)
    sparql.addCustomHttpHeader('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36')
    sparql.setQuery(query)
    sparql.setReturnFormat('json')
    sparql.setMethod("POST")
    query_result = try_until_timeout(sparql, timeout_tries=timeout_tries)

    result_list = list()
    for column in column_names:
        tmp_list = list()
        for single_query in query_result['results']['bindings']:
            tmp_list.append(extract_qid(single_query, name=column))
        result_list.append(tmp_list)
    result_df = pd.DataFrame(result_list)
    result_df = result_df.transpose()
    result_df.columns = column_names
    if exclude_unknown:
        for column in column_names:
            result_df = result_df[~result_df[column].str.get(0).isin(['t'])]
    return result_df


instance= get_statement(query="""
    SELECT DISTINCT ?qid ?instance_of ?instance_ofLabel WHERE {
  ?qid (wdt:P31/wdt:P279*) wd:Q180684.
  OPTIONAL { ?qid wdt:P31 ?instance_of. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}

""")

qid_label= get_statement(query="""
    SELECT DISTINCT ?qid ?qidLabel WHERE {
  ?qid (wdt:P31/wdt:P279*) wd:Q180684.
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}

""")
qid_time = get_statement(query="""
  SELECT DISTINCT ?qid ?start ?end ?point_in_time ?instance_of  WHERE {
  ?qid (wdt:P31/wdt:P279*) wd:Q180684.
  OPTIONAL { ?qid wdt:P580 ?start. }
  OPTIONAL { ?qid wdt:P582 ?end. }
  OPTIONAL { ?qid wdt:P585 ?point_in_time. }
  OPTIONAL { ?qid wdt:P31 ?instance_of. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
 
}
""")





qid_location= get_statement(query="""
    SELECT DISTINCT ?qid ?location ?locationLabel ?instance_of WHERE {
  ?qid (wdt:P31/wdt:P279*) wd:Q180684.
  OPTIONAL { ?qid wdt:P276 ?location. }
  OPTIONAL { ?qid wdt:P31 ?instance_of. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}

""")
qid_participant = get_statement(query="""
   SELECT DISTINCT ?qid ?participant ?participantLabel ?instance_of  WHERE {
  ?qid (wdt:P31/wdt:P279*) wd:Q180684.
  OPTIONAL { ?qid wdt:P710 ?participant. }
  OPTIONAL { ?qid wdt:P31 ?instance_of. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
 
}

""")

qid_partof = get_statement(query="""
   SELECT DISTINCT ?qid ?part_of ?part_ofLabel ?instance_of   WHERE {
  ?qid (wdt:P31/wdt:P279*) wd:Q180684.
  OPTIONAL { ?qid wdt:P361 ?part_of. }
  OPTIONAL { ?qid wdt:P31 ?instance_of. }
   SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}


""")



"""loc_qid=qid_location.loc[:,['qid']].drop_duplicates()
loc_qid['loc']=1
time_qid=qid_time.loc[:,['qid']].drop_duplicates()
time_qid['index']=1
partic_qid=qid_participant.loc[:,['qid']].drop_duplicates()
partic_qid["parti"]=1
partof_qid=qid_partof.loc[:,['qid']].drop_duplicates()
partof_qid['partof']=1
instance_qid=qid_instance.loc[:,['qid']].drop_duplicates()
instance_qid['instance']=1


qid=pd.merge(loc_qid,time_qid,on='qid', how='outer')
qid=pd.merge(qid,partic_qid,on='qid', how='outer')
qid=pd.merge(qid,partof_qid,on='qid', how='outer')
qid=pd.merge(qid,instance_qid,on='qid', how='outer')


qid_miss=qid.loc[(qid['loc']!=1)|(qid['index']!=1)|(qid['parti']!=1)|(qid['partof']!=1)|(qid['instance']!=1)]
qid_location=pd.merge(qid_location,qid_instance,on='qid',how='left').drop_duplicates()


"""


#qid_instance=qid_participant.loc[:,['qid','qidLabel','instance_of','instance_ofLabel']].drop_duplicates()

instance=instance.loc[:,['instance_of','instance_ofLabel']].drop_duplicates()
#qid_label=qid_partof.loc[:,['qid','qidLabel']].drop_duplicates()

qid_location=pd.merge(qid_location,instance,on='instance_of',how='left').drop_duplicates()
qid_partof=pd.merge(qid_partof,instance,on='instance_of',how='left').drop_duplicates()
qid_time=pd.merge(qid_time,instance,on='instance_of',how='left').drop_duplicates()
qid_participant=pd.merge(qid_participant,instance,on='instance_of',how='left').drop_duplicates()

qid_location=pd.merge(qid_location,qid_label,on='qid',how='left').drop_duplicates()
qid_time=pd.merge(qid_time,qid_label,on='qid',how='left').drop_duplicates()
qid_partof=pd.merge(qid_partof,qid_label,on='qid',how='left').drop_duplicates()
qid_participant=pd.merge(qid_participant,qid_label,on='qid',how='left').drop_duplicates()

qid_location=qid_location.loc[:,['qid','qidLabel','location','locationLabel','instance_of','instance_ofLabel']]
qid_time=qid_time.loc[:,['qid','qidLabel','start','end', 'point_in_time','instance_of','instance_ofLabel']]
qid_partof=qid_partof.loc[:,['qid','qidLabel','part_of','part_ofLabel','instance_of','instance_ofLabel']]
qid_participant=qid_participant.loc[:,['qid','qidLabel','participant','participantLabel','instance_of','instance_ofLabel']]


qid_time.to_csv(qid_time_path, index=False)
qid_location.to_csv(qid_location_path, index=False)
qid_participant.to_csv(qid_participant_path, index=False)
qid_partof.to_csv(qid_partof_path, index=False)
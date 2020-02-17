# -*- coding: utf-8 -*-
"""
Created on Mon Feb 17 20:13:08 2020
This file is for creating sheets of conflicts with their categaries, then we can using 
the categaries to obtain more information of their time, locations and participants.
@author: leo
"""

import os
import numpy as np
import pandas as pd
import sys
import re
import random
import datetime
import calendar
from os import mkdir
now = datetime.datetime.now()
date_part = calendar.month_name[now.month].lower() + str(now.year)

main_path = "D:/learning/Arash/war_participants/"
try:
    mkdir(main_path)
except FileExistsError:
    pass

#wikidata_date="december2018"
wikidata_date=date_part
wikidata_path="D:/learning/Arash/war_participants/material/wikiTSV_" + wikidata_date + '/'

#wiki_after_sele_merg_path=main_path + "input/"
wiki_after_sele_merg_path=os.path.join(main_path,"input/")

#qid_time_path=wikidata_path + "qid_time.csv"
qid_time_path=os.path.join(wikidata_path,"qid_time.csv")
#qid_participant_path=wikidata_path + "qid_participant.csv"
qid_participant_path=os.path.join(wikidata_path,"qid_participant.csv")
#qid_location_path=wikidata_path + "qid_location.csv"
qid_location_path=os.path.join(wikidata_path, "qid_location.csv")
#selected_qid_time_path=wiki_after_sele_merg_path + 'selected_qid_time.csv'
selected_qid_time_path=os.path.join(wiki_after_sele_merg_path,'selected_qid_time.csv')
#selected_qid_location_path=wiki_after_sele_merg_path + 'selected_qid_location.csv'
selected_qid_location_path=os.path.join(wiki_after_sele_merg_path,'selected_qid_location.csv')
#selected_qid_participant_path=wiki_after_sele_merg_path + 'selected_qid_participant.csv'
selected_qid_participant_path=os.path.join(wiki_after_sele_merg_path,'selected_qid_participant.csv')
#merge_sele_qid_time_path=wiki_after_sele_merg_path + 'merge_sele_qid_time.csv'
merge_sele_qid_time_path=os.path.join(wiki_after_sele_merg_path,'merge_sele_qid_time.csv')
#merge_sele_qid_location_path=wiki_after_sele_merg_path + 'merge_sele_qid_location.csv'
merge_sele_qid_location_path=os.path.join(wiki_after_sele_merg_path,'merge_sele_qid_location.csv')
#mergecat_sele_qid_time_path=wiki_after_sele_merg_path + 'mergecat_sele_qid_time.csv'
mergecat_sele_qid_time_path=os.path.join(wiki_after_sele_merg_path,'mergecat_sele_qid_time.csv')
#mergecat_sele_qid_location_path=wiki_after_sele_merg_path + 'mergecat_sele_qid_location.csv'
mergecat_sele_qid_location_path=os.path.join(wiki_after_sele_merg_path,'mergecat_sele_qid_location.csv')

qid_time=pd.read_csv(qid_time_path)
qid_participant=pd.read_csv(qid_participant_path)
qid_location=pd.read_csv(qid_location_path)


#step1:selection
pos_list = ['war', 'conflict', 'rebellion', 'revolt', 'military','revolt', 'occupation', 'conquest', 'annexation']
pos_list = '|'.join(pos_list)

neg_list = ['trade war','military exercise','war crimes trial','fictional','legendary','Battles in the Chronicles of Narnia','social conflict','organizational conflict','power conflict']
neg_list = '|'.join(neg_list)

all_dfs=[qid_time, qid_location, qid_participant]
for i in range(len(all_dfs)):
    all_dfs[i]['instance_ofLabel']=all_dfs[i]['instance_ofLabel'].str.lower()
    all_dfs[i] = all_dfs[i][~all_dfs[i].instance_ofLabel.isna()]
    all_dfs[i] = all_dfs[i][all_dfs[i].instance_ofLabel.str.contains(pos_list)]
    all_dfs[i] = all_dfs[i][ ~ all_dfs[i]['instance_ofLabel'].str.contains(neg_list)]
    all_dfs[i].drop(['instance_of','instance_ofLabel'],axis=1,inplace=True)
    all_dfs[i].drop_duplicates(inplace=True)

qid_time=all_dfs[0]
qid_location=all_dfs[1]
qid_participant=all_dfs[2]


qid_time.to_csv(selected_qid_time_path, index=False)
qid_location.to_csv(selected_qid_location_path,index=False)
qid_participant.to_csv(selected_qid_participant_path,index=False)    
 #y=~x.str.contains('Q')
#y.describe()


# step2:merge the selected dataset to the qid_qid_cat.tsv: to get the categaries
qid_qidcat=pd.read_csv(main_path+'material/qid_qid-cat.tsv',delimiter='\t')
selected_qid_time=pd.read_csv(selected_qid_time_path) 
selected_qid_participant=pd.read_csv(selected_qid_participant_path)
selected_qid_location=pd.read_csv(selected_qid_location_path)
#x=qid_qidcat.loc[qid_qidcat['qid']==79791]#see how many catevaries that Q79791 has

merge_sele_qid_time=pd.merge(selected_qid_time,qid_qidcat,on='qid',how='left')
merge_sele_qid_time.to_csv(merge_sele_qid_time_path, index=False)

merge_sele_qid_location=pd.merge(selected_qid_location,qid_qidcat,on='qid',how='left')
merge_sele_qid_location.to_csv(merge_sele_qid_location_path, index=False)
#get the categaries
qid_catsubject=pd.read_csv(main_path+'material/qid_catSubject.tsv',delimiter='\t')
merge_sele_qid_time=pd.read_csv(merge_sele_qid_time_path)
merge_sele_qid_location=pd.read_csv(merge_sele_qid_location_path)


qid_catsubject.rename(columns={'qid':'qid_cat'},inplace=True)
merge_sele_qid_time['qid_cat']=merge_sele_qid_time['qid_cat'].fillna(0)# because there are some missing in qid_cat, so we fill all the missing with 0, then we can trange them into int
merge_sele_qid_time[['qid_cat']] = merge_sele_qid_time[['qid_cat']].astype(int)
mergecat_sele_qid_time=pd.merge(merge_sele_qid_time,qid_catsubject,on='qid_cat',how='left')#merging to get the categary names
mergecat_sele_qid_time['qid_cat']=mergecat_sele_qid_time['qid_cat'].apply(lambda x: np.NaN if x==0 else x)

mergecat_sele_qid_time.drop_duplicates(inplace=True)
mergecat_sele_qid_time.to_csv(mergecat_sele_qid_time_path, index=False)

merge_sele_qid_location['qid_cat']=merge_sele_qid_location['qid_cat'].fillna(0)# because there are some missing in qid_cat, so we fill all the missing with 0, then we can trange them into int
merge_sele_qid_location[['qid_cat']] = merge_sele_qid_location[['qid_cat']].astype(int)
mergecat_sele_qid_location=pd.merge(merge_sele_qid_location,qid_catsubject, on='qid_cat',how='left')#merging to get the categary names
mergecat_sele_qid_location['qid_cat']=mergecat_sele_qid_location['qid_cat'].apply(lambda x: np.NaN if x==0 else x)

mergecat_sele_qid_location.drop_duplicates(inplace=True)
mergecat_sele_qid_location.to_csv(mergecat_sele_qid_location_path, index=False)


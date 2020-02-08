# -*- coding: utf-8 -*-
"""
Created on Wed Jan  9 14:30:14 2019
This file is for creating maps of conflicts to their wiki categaries, then we can using the categaries to obtain more information about their time, locations and participants.
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

main_path = "D:/learning/Arash/war_participants_battles/"
try:
    mkdir(main_path)
except FileExistsError:
    pass

#wikidata_date="december2018"
wikidata_date=date_part
#wikidata_path="D:/learning/Arash/war_participants_battles/material/wikiTSV_" + wikidata_date + '/'
material_path="D:/learning/Arash/war_participants/"
wikidata_path=material_path+"material/wikiTSV_" + wikidata_date + '/' # thus can provide convenience to the construction of master file

wiki_after_sele_merg_path=main_path + "input/"

qid_time_path=wikidata_path + "qid_time.csv"
qid_participant_path=wikidata_path + "qid_participant.csv"
qid_location_path=wikidata_path + "qid_location.csv"
selected_qid_time_path=wiki_after_sele_merg_path + 'selected_qid_time_battles.csv'
selected_qid_location_path=wiki_after_sele_merg_path + 'selected_qid_location_battles.csv'
selected_qid_participant_path=wiki_after_sele_merg_path + 'selected_qid_participant_battles.csv'
merge_sele_qid_time_path=wiki_after_sele_merg_path + 'merge_sele_qid_time_battles.csv'
merge_sele_qid_location_path=wiki_after_sele_merg_path + 'merge_sele_qid_location_battles.csv'
mergecat_sele_qid_time_path=wiki_after_sele_merg_path + 'mergecat_sele_qid_time_battles.csv'
mergecat_sele_qid_location_path=wiki_after_sele_merg_path + 'mergecat_sele_qid_location_battles.csv'


qid_time=pd.read_csv(qid_time_path)
qid_participant=pd.read_csv(qid_participant_path)
qid_location=pd.read_csv(qid_location_path)


#step1:selection
#1.ind the items that have labels of propert "instance of" correlated to wor
qid_time['instance_ofLabel']=qid_time['instance_ofLabel'].str.lower()
qid_location['instance_ofLabel']=qid_location['instance_ofLabel'].str.lower()
qid_participant['instance_ofLabel']=qid_participant['instance_ofLabel'].str.lower()# convert each value of 'instance_ofLabel' to lowercase  

selected_qid_time=qid_time.loc[qid_time.instance_ofLabel.str.contains('war')
|qid_time.instance_ofLabel.str.contains('conflict')
|qid_time.instance_ofLabel.str.contains('battle')
|qid_time.instance_ofLabel.str.contains('rebellion')
|qid_time.instance_ofLabel.str.contains('revolt')
|qid_time.instance_ofLabel.str.contains('military')
|qid_time.instance_ofLabel.str.contains('revolt')
|qid_time.instance_ofLabel.str.contains('occupation')
|qid_time.instance_ofLabel.str.contains('conquest')
|qid_time.instance_ofLabel.str.contains('annexation')]
selected_qid_time=selected_qid_time[ ~ selected_qid_time['instance_ofLabel'].str.contains('trade war')] # remove the items which have value 'trade war' in 'instance_ofLabel'
selected_qid_time=selected_qid_time[ ~ selected_qid_time['instance_ofLabel'].str.contains('military exercise')]
selected_qid_time=selected_qid_time[ ~ selected_qid_time['instance_ofLabel'].str.contains('war crimes trial')]
selected_qid_time=selected_qid_time[ ~ selected_qid_time['instance_ofLabel'].str.contains('fictional')]
selected_qid_time=selected_qid_time[ ~ selected_qid_time['instance_ofLabel'].str.contains('legendary')]
selected_qid_time=selected_qid_time[ ~ selected_qid_time['instance_ofLabel'].str.contains('Battles in the Chronicles of Narnia')]
selected_qid_time=selected_qid_time[ ~ selected_qid_time['instance_ofLabel'].str.contains('social conflict')]
selected_qid_time=selected_qid_time[ ~ selected_qid_time['instance_ofLabel'].str.contains('organizational conflict')]
selected_qid_time=selected_qid_time[ ~ selected_qid_time['instance_ofLabel'].str.contains('power conflict')]



selected_qid_location=qid_location.loc[qid_location.instance_ofLabel.str.contains('war')
|qid_location.instance_ofLabel.str.contains('conflict')
|qid_location.instance_ofLabel.str.contains('battle')
|qid_location.instance_ofLabel.str.contains('rebellion')
|qid_location.instance_ofLabel.str.contains('revolt')
|qid_location.instance_ofLabel.str.contains('military')
|qid_location.instance_ofLabel.str.contains('revolt')
|qid_location.instance_ofLabel.str.contains('occupation')
|qid_location.instance_ofLabel.str.contains('conquest')
|qid_location.instance_ofLabel.str.contains('annexation')]
selected_qid_location=selected_qid_location[ ~ selected_qid_location['instance_ofLabel'].str.contains('trade war')] # remove the items which have value 'trade war' in 'instance_ofLabel'
selected_qid_location=selected_qid_location[ ~ selected_qid_location['instance_ofLabel'].str.contains('military exercise')]
selected_qid_location=selected_qid_location[ ~ selected_qid_location['instance_ofLabel'].str.contains('war crimes trial')]
selected_qid_location=selected_qid_location[ ~ selected_qid_location['instance_ofLabel'].str.contains('fictional')]
selected_qid_location=selected_qid_location[ ~ selected_qid_location['instance_ofLabel'].str.contains('legendary')]
selected_qid_location=selected_qid_location[ ~ selected_qid_location['instance_ofLabel'].str.contains('Battles in the Chronicles of Narnia')]
selected_qid_location=selected_qid_location[ ~ selected_qid_location['instance_ofLabel'].str.contains('social conflict')]
selected_qid_location=selected_qid_location[ ~ selected_qid_location['instance_ofLabel'].str.contains('organizational conflict')]
selected_qid_location=selected_qid_location[ ~ selected_qid_location['instance_ofLabel'].str.contains('power conflict')]






selected_qid_participant=qid_participant.loc[qid_participant.instance_ofLabel.str.contains('war')
|qid_participant.instance_ofLabel.str.contains('conflict')
|qid_participant.instance_ofLabel.str.contains('battle')
|qid_participant.instance_ofLabel.str.contains('rebellion')
|qid_participant.instance_ofLabel.str.contains('revolt')
|qid_participant.instance_ofLabel.str.contains('military')
|qid_participant.instance_ofLabel.str.contains('revolt')
|qid_participant.instance_ofLabel.str.contains('occupation')
|qid_participant.instance_ofLabel.str.contains('conquest')
|qid_participant.instance_ofLabel.str.contains('annexation')]
selected_qid_participant=selected_qid_participant[ ~ selected_qid_participant['instance_ofLabel'].str.contains('trade war')] # remove the items which have value 'trade war' in 'instance_ofLabel'
selected_qid_participant=selected_qid_participant[ ~ selected_qid_participant['instance_ofLabel'].str.contains('military exercise')]
selected_qid_participant=selected_qid_participant[ ~ selected_qid_participant['instance_ofLabel'].str.contains('war crimes trial')]
selected_qid_participant=selected_qid_participant[ ~ selected_qid_participant['instance_ofLabel'].str.contains('fictional')]
selected_qid_participant=selected_qid_participant[ ~ selected_qid_participant['instance_ofLabel'].str.contains('legendary')]
selected_qid_participant=selected_qid_participant[ ~ selected_qid_participant['instance_ofLabel'].str.contains('Battles in the Chronicles of Narnia')]
selected_qid_participant=selected_qid_participant[ ~ selected_qid_participant['instance_ofLabel'].str.contains('social conflict')]
selected_qid_participant=selected_qid_participant[ ~ selected_qid_participant['instance_ofLabel'].str.contains('organizational conflict')]
selected_qid_participant=selected_qid_participant[ ~ selected_qid_participant['instance_ofLabel'].str.contains('power conflict')]





#2、remove the instance
items=[selected_qid_time,selected_qid_location,selected_qid_participant]
for item in items:
    item.drop(['instance_of','instance_ofLabel'],axis=1,inplace=True)
    
#3、Removing Duplicates
for item in items:
    item.drop_duplicates(inplace=True)

#4、get the qid number from the original qid. This aims to be consistent with qid_subject.tsv and qid_qid-cat.tsv
for item in items:
    item['qid']=item.loc[:,'qid'].str[32:]



selected_qid_time.to_csv(selected_qid_time_path, index=False)
selected_qid_location.to_csv(selected_qid_location_path,index=False)
selected_qid_participant.to_csv(selected_qid_participant_path,index=False)    
 #y=~x.str.contains('Q')
#y.describe()


# step2:merge the selected dataset to the qid_qid_cat.tsv: to get the categaries
#qid_qidcat=pd.read_csv(main_path+'material/qid_qid-cat.tsv',delimiter='\t')
qid_qidcat=pd.read_csv(material_path+'material/qid_qid-cat.tsv',delimiter='\t')
selected_qid_time=pd.read_csv(selected_qid_time_path) 
selected_qid_participant=pd.read_csv(selected_qid_participant_path)
selected_qid_location=pd.read_csv(selected_qid_location_path)
#x=qid_qidcat.loc[qid_qidcat['qid']==79791]#see how many catevaries that Q79791 has

merge_sele_qid_time=pd.merge(selected_qid_time,qid_qidcat,on='qid',how='left')
merge_sele_qid_time.to_csv(merge_sele_qid_time_path, index=False)

merge_sele_qid_location=pd.merge(selected_qid_location,qid_qidcat,on='qid',how='left')
merge_sele_qid_location.to_csv(merge_sele_qid_location_path, index=False)
#get the categaries
#qid_catsubject=pd.read_csv(main_path+'material/qid_catSubject.tsv',delimiter='\t')
qid_catsubject=pd.read_csv(material_path+'material/qid_catSubject.tsv',delimiter='\t')
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


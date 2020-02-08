# -*- coding: utf-8 -*-
"""
Created on Tue Dec  4 09:31:56 2018
This file is for getting time information from the wiki categaries and wikidata
@author: leo
"""
import datetime
import calendar
from os import mkdir
import numpy as np
import pandas as pd
import sys
import re
import random
import datetime

main_path = "D:/learning/Arash/war_participants/"
try:
    mkdir(main_path)
except FileExistsError:
    pass

mergecat_sele_qid_time=pd.read_csv(main_path+'input/mergecat_sele_qid_time.csv')
####step 1:get the date from categaries
#1 : get the exaxt year
mergecat_sele_qid_time['withnumber']=mergecat_sele_qid_time['subject_cat'].str.extract(r'([\w\.-\\\(\)]*\d+[\w\.-\\\(\))]*)',expand=False)
mergecat_sele_qid_time['conflicts_in_year']=mergecat_sele_qid_time['subject_cat'].str.extract(r'((?<=Conflicts_in_)\d+$)',expand=False)#we should add $ here, because if there is categaries like" conflicts_in_134_BC", then here we get 134, but in year_BC, we get 134_BC
mergecat_sele_qid_time['year_in']=mergecat_sele_qid_time['subject_cat'].str.extract(r'(^\d+(?=_in_))',expand=False)
mergecat_sele_qid_time['year']=mergecat_sele_qid_time['subject_cat'].str.extract(r'(^\d+$)',expand=False)
mergecat_sele_qid_time['year_by']=mergecat_sele_qid_time['subject_cat'].str.extract(r'(^\d+(?=_by_))',expand=False)
mergecat_sele_qid_time['year_BC']=mergecat_sele_qid_time['subject_cat'].str.extract(r'(\d+_BC|(?<=前)\d+(?=年(?!代))|(?<=de/)\d+(?=_v._Chr.))',expand=False)
mergecat_sele_qid_time['year_present']=mergecat_sele_qid_time['subject_cat'].str.extract(r'(\d+(?=–present))',expand=False)#notice:here,it is –, not-
mergecat_sele_qid_time['present']=mergecat_sele_qid_time['subject_cat'].str.extract(r'((?<=\d{3}–)present)',expand=False)#notice:here,it is –, not-
mergecat_sele_qid_time['add_year']=mergecat_sele_qid_time['subject_cat'].str.extract(r'((?<=War_of_)\d+|(?<=War_in_)\d+|(?<=de/Ereignis_)\d+|(?<=de/Veranstaltung_)\d+|(?<=fr/Bataille_de_)\d+|(?<=it/Eventi_del_)\d+|(?<=zh/)\d+(?=年(?!代)))',expand=False)

# 2 : get the range of year(with s)
mergecat_sele_qid_time['conflicts_in_year_s']=mergecat_sele_qid_time['subject_cat'].str.extract(r'(\d+s(?=_conflicts))',expand=False)
mergecat_sele_qid_time['year_s_in']=mergecat_sele_qid_time['subject_cat'].str.extract(r'(^\d+s(?=_in_)|(?<=zh/)\d+(?=年代))',expand=False)
mergecat_sele_qid_time['year_s']=mergecat_sele_qid_time['subject_cat'].str.extract(r'(^\d+s$)',expand=False)
mergecat_sele_qid_time['year_s_BC']=mergecat_sele_qid_time['subject_cat'].str.extract(r'(\d+s_BC|(?<=前)\d+(?=年代))',expand=False)
# 3: get the century
mergecat_sele_qid_time['century']=mergecat_sele_qid_time['subject_cat'].str.extract(r'(\d+\w*[_-]century(?!_BC)|\d+(?=._Jahrhundert(?!_[vV].))|(?<=zh/)\d+(?=世纪))',expand=False)
mergecat_sele_qid_time['century_BC']=mergecat_sele_qid_time['subject_cat'].str.extract(r'(\d+\w*[_-]century_BC|(?<=前)\d+(?=世纪))',expand=False)
#4: get month
mergecat_sele_qid_time['with_month']=mergecat_sele_qid_time['subject_cat'].str.extract(r'(\w+_\d+(?=_events))',expand=False)#no problem, only months were extracted

#after extracting dates from categaries, how many items still have no imformation of date?(1430)
D=mergecat_sele_qid_time[mergecat_sele_qid_time.start.isnull() 
& mergecat_sele_qid_time.end.isnull()&mergecat_sele_qid_time.point_in_time.isnull()
]
x=D.loc[:,'qid']
x.drop_duplicates(inplace=True)
x.count()


s=D[D.conflicts_in_year.notnull() 
|D.year_in.notnull()
|D.year.notnull()
|D.year_by.notnull()
|D.year_BC.notnull()
|D.conflicts_in_year_s.notnull()
|D.year_s_in.notnull()
|D.year_s.notnull()
|D.year_s_BC.notnull()
|D.century.notnull()
|D.century_BC.notnull()
|D.with_month.notnull()
|D.year_present.notnull()
|D.present.notnull()
|D.add_year.notnull()]

s.drop_duplicates(inplace=True)
x=s.loc[:,'qid']
x.drop_duplicates(inplace=True)
3765-x.count()#3862


####step 2: get the dataframe of the date got from the categaries
mergecat_sele_qid_time.drop(['start','end','point_in_time','qid_cat','subject_cat','withnumber'],axis=1,inplace=True)#for the next merging operation, drop the old variables 
mergecat_sele_qid_time.drop_duplicates(inplace=True)

selected_qid_time=pd.read_csv(main_path+'input/selected_qid_time.csv') 
missing_time=selected_qid_time[selected_qid_time.start.isnull() 
& selected_qid_time.end.isnull()&selected_qid_time.point_in_time.isnull()]
date_missing_qid_time=pd.merge(missing_time,mergecat_sele_qid_time,on='qid',how='left')#add the date imformation to the items that have no values in start,end or point in time
#get the list of the qid of those that have no information of date
nodate_atall=date_missing_qid_time[~date_missing_qid_time['qid'].isin(x)]
nodate_qid=nodate_atall.loc[:,'qid']
nodate_qid.drop_duplicates(inplace=True)
nodate_qid.to_csv(main_path+'output/nodate_qid.csv', index=False)
###add the start and end
date_missing_qid_time=date_missing_qid_time.fillna(0)# because there are some missing i, so we fill all the missing with 0, then we can change them into int,then we can caculate them
#conflicts_in_year
date_missing_qid_time[['conflicts_in_year']] = date_missing_qid_time[['conflicts_in_year']].astype(int)
date_missing_qid_time['conflicts_in_year']=date_missing_qid_time['conflicts_in_year'].apply(lambda x: np.NaN if x==0 else x)# change 0 to nan, then 0 will not be involved in sorting
grouped_conflicts_in_year=date_missing_qid_time['conflicts_in_year'].groupby(date_missing_qid_time['qid'])
g=grouped_conflicts_in_year.min()
g=g.reset_index()# change the index to column
g.rename(columns={'conflicts_in_year': 'conflicts_in_year_start'}, inplace=True) 
date_missing_qid_time=pd.merge(date_missing_qid_time,g,on='qid',how='left')

g=grouped_conflicts_in_year.max()
g=g.reset_index()# change the index to column
g.rename(columns={'conflicts_in_year': 'conflicts_in_year_end'}, inplace=True) 
date_missing_qid_time=pd.merge(date_missing_qid_time,g,on='qid',how='left')

#year_in
date_missing_qid_time[['year_in']] = date_missing_qid_time[['year_in']].astype(int)
date_missing_qid_time['year_in']=date_missing_qid_time['year_in'].apply(lambda x: np.NaN if x==0 else x)# change 0 to nan, then 0 will not be involved in sorting
grouped_year_in=date_missing_qid_time['year_in'].groupby(date_missing_qid_time['qid'])
g=grouped_year_in.min()
g=g.reset_index()# change the index to column
g.rename(columns={'year_in': 'year_in_start'}, inplace=True) 
date_missing_qid_time=pd.merge(date_missing_qid_time,g,on='qid',how='left')

g=grouped_year_in.max()
g=g.reset_index()# change the index to column
g.rename(columns={'year_in': 'year_in_end'}, inplace=True) 
date_missing_qid_time=pd.merge(date_missing_qid_time,g,on='qid',how='left')

#year
date_missing_qid_time[['year']] = date_missing_qid_time[['year']].astype(int)
date_missing_qid_time['year']=date_missing_qid_time['year'].apply(lambda x: np.NaN if x==0 else x)# change 0 to nan, then 0 will not be involved in sorting
grouped_year=date_missing_qid_time['year'].groupby(date_missing_qid_time['qid'])
g=grouped_year.min()
g=g.reset_index()# change the index to column
g.rename(columns={'year': 'year_start'}, inplace=True) 
date_missing_qid_time=pd.merge(date_missing_qid_time,g,on='qid',how='left')

g=grouped_year.max()
g=g.reset_index()# change the index to column
g.rename(columns={'year': 'year_end'}, inplace=True) 
date_missing_qid_time=pd.merge(date_missing_qid_time,g,on='qid',how='left')

#year_by
date_missing_qid_time[['year_by']] = date_missing_qid_time[['year_by']].astype(int)
date_missing_qid_time['year_by']=date_missing_qid_time['year_by'].apply(lambda x: np.NaN if x==0 else x)# change 0 to nan, then 0 will not be involved in sorting
grouped_year_by=date_missing_qid_time['year_by'].groupby(date_missing_qid_time['qid'])
g=grouped_year_by.min()
g=g.reset_index()# change the index to column
g.rename(columns={'year_by': 'year_by_start'}, inplace=True) 
date_missing_qid_time=pd.merge(date_missing_qid_time,g,on='qid',how='left')

g=grouped_year_by.max()
g=g.reset_index()# change the index to column
g.rename(columns={'year_by': 'year_by_end'}, inplace=True) 
date_missing_qid_time=pd.merge(date_missing_qid_time,g,on='qid',how='left')

#add_year
date_missing_qid_time[['add_year']] = date_missing_qid_time[['add_year']].astype(int)
date_missing_qid_time['add_year']=date_missing_qid_time['add_year'].apply(lambda x: np.NaN if x==0 else x)# change 0 to nan, then 0 will not be involved in sorting
grouped_add_year=date_missing_qid_time['add_year'].groupby(date_missing_qid_time['qid'])
g=grouped_add_year.min()
g=g.reset_index()# change the index to column
g.rename(columns={'add_year': 'add_year_start'}, inplace=True) 
date_missing_qid_time=pd.merge(date_missing_qid_time,g,on='qid',how='left')

g=grouped_add_year.max()
g=g.reset_index()# change the index to column
g.rename(columns={'add_year': 'add_year_end'}, inplace=True) 
date_missing_qid_time=pd.merge(date_missing_qid_time,g,on='qid',how='left')

#year_BC
date_missing_qid_time['year_BC']=date_missing_qid_time['year_BC'].str.extract(r'(\d+)',expand=False)
date_missing_qid_time['year_BC']=date_missing_qid_time['year_BC'].fillna(0)# because after the re-extracting, the missings turn back to nan
date_missing_qid_time[['year_BC']] = date_missing_qid_time[['year_BC']].astype(int)
date_missing_qid_time['year_BC']=-date_missing_qid_time['year_BC']#take negative number
date_missing_qid_time['year_BC']=date_missing_qid_time['year_BC'].apply(lambda x: np.NaN if x==0 else x)# change 0 to nan, then 0 will not be involved in sorting
grouped_year_BC=date_missing_qid_time['year_BC'].groupby(date_missing_qid_time['qid'])
g=grouped_year_BC.min()
g=g.reset_index()# change the index to column
g.rename(columns={'year_BC': 'year_BC_start'}, inplace=True) 
date_missing_qid_time=pd.merge(date_missing_qid_time,g,on='qid',how='left')

g=grouped_year_BC.max()
g=g.reset_index()# change the index to column
g.rename(columns={'year_BC': 'year_BC_end'}, inplace=True) 
date_missing_qid_time=pd.merge(date_missing_qid_time,g,on='qid',how='left')

#conflicts_in_year_s
date_missing_qid_time['conflicts_in_year_s']=date_missing_qid_time['conflicts_in_year_s'].str.extract(r'(\d+)',expand=False)#remove "s"
date_missing_qid_time['conflicts_in_year_s']=date_missing_qid_time['conflicts_in_year_s'].fillna(0)# because after the re-extracting, the missings turn back to nan
date_missing_qid_time[['conflicts_in_year_s']] = date_missing_qid_time[['conflicts_in_year_s']].astype(int)
date_missing_qid_time['conflicts_in_year_s']=date_missing_qid_time['conflicts_in_year_s']+5
date_missing_qid_time['conflicts_in_year_s']=date_missing_qid_time['conflicts_in_year_s'].apply(lambda x: np.NaN if x==5 else x)# change 0 to nan, then 0 will not be involved in sorting
grouped_conflicts_in_year_s=date_missing_qid_time['conflicts_in_year_s'].groupby(date_missing_qid_time['qid'])
g=grouped_conflicts_in_year_s.min()
g=g.reset_index()# change the index to column
g.rename(columns={'conflicts_in_year_s': 'conflicts_in_year_s_start'}, inplace=True) 
date_missing_qid_time=pd.merge(date_missing_qid_time,g,on='qid',how='left')

g=grouped_conflicts_in_year_s.max()
g=g.reset_index()# change the index to column
g.rename(columns={'conflicts_in_year_s': 'conflicts_in_year_s_end'},inplace=True) 
date_missing_qid_time=pd.merge(date_missing_qid_time,g,on='qid',how='left')

#year_s_in
date_missing_qid_time['year_s_in']=date_missing_qid_time['year_s_in'].str.extract(r'(\d+)',expand=False)#remove "s"
date_missing_qid_time['year_s_in']=date_missing_qid_time['year_s_in'].fillna(0)# because after the re-extracting, the missings turn back to nan
date_missing_qid_time[['year_s_in']] = date_missing_qid_time[['year_s_in']].astype(int)
date_missing_qid_time['year_s_in']=date_missing_qid_time['year_s_in']+5
date_missing_qid_time['year_s_in']=date_missing_qid_time['year_s_in'].apply(lambda x: np.NaN if x==5 else x)# change 0 to nan, then 0 will not be involved in sorting
grouped_year_s_in=date_missing_qid_time['year_s_in'].groupby(date_missing_qid_time['qid'])
g=grouped_year_s_in.min()
g=g.reset_index()# change the index to column
g.rename(columns={'year_s_in': 'year_s_in_start'}, inplace=True) 
date_missing_qid_time=pd.merge(date_missing_qid_time,g,on='qid',how='left')

g=grouped_year_s_in.max()
g=g.reset_index()# change the index to column
g.rename(columns={'year_s_in': 'year_s_in_end'},inplace=True) 
date_missing_qid_time=pd.merge(date_missing_qid_time,g,on='qid',how='left')

#year_s
date_missing_qid_time['year_s']=date_missing_qid_time['year_s'].str.extract(r'(\d+)',expand=False)#remove "s"
date_missing_qid_time['year_s']=date_missing_qid_time['year_s'].fillna(0)# because after the re-extracting, the missings turn back to nan
date_missing_qid_time[['year_s']] = date_missing_qid_time[['year_s']].astype(int)
date_missing_qid_time['year_s']=date_missing_qid_time['year_s']+5
date_missing_qid_time['year_s']=date_missing_qid_time['year_s'].apply(lambda x: np.NaN if x==5 else x)# change 0 to nan, then 0 will not be involved in sorting
grouped_year_s=date_missing_qid_time['year_s'].groupby(date_missing_qid_time['qid'])
g=grouped_year_s.min()
g=g.reset_index()# change the index to column
g.rename(columns={'year_s': 'year_s_start'}, inplace=True) 
date_missing_qid_time=pd.merge(date_missing_qid_time,g,on='qid',how='left')

g=grouped_year_s.max()
g=g.reset_index()# change the index to column
g.rename(columns={'year_s': 'year_s_end'},inplace=True) 
date_missing_qid_time=pd.merge(date_missing_qid_time,g,on='qid',how='left')

#year_s_BC
date_missing_qid_time['year_s_BC']=date_missing_qid_time['year_s_BC'].str.extract(r'(\d+)',expand=False)#remove "s_BC"
date_missing_qid_time['year_s_BC']=date_missing_qid_time['year_s_BC'].fillna(0)# because after the re-extracting, the missings turn back to nan
date_missing_qid_time[['year_s_BC']] = date_missing_qid_time[['year_s_BC']].astype(int)
date_missing_qid_time['year_s_BC']=date_missing_qid_time['year_s_BC']+5
date_missing_qid_time['year_s_BC']=-date_missing_qid_time['year_s_BC']
date_missing_qid_time['year_s_BC']=date_missing_qid_time['year_s_BC'].apply(lambda x: np.NaN if x==-5 else x)# change 0 to nan, then 0 will not be involved in sorting
grouped_year_s_BC=date_missing_qid_time['year_s_BC'].groupby(date_missing_qid_time['qid'])
g=grouped_year_s_BC.min()
g=g.reset_index()# change the index to column
g.rename(columns={'year_s_BC': 'year_s_BC_start'}, inplace=True) 
date_missing_qid_time=pd.merge(date_missing_qid_time,g,on='qid',how='left')

g=grouped_year_s_BC.max()
g=g.reset_index()# change the index to column
g.rename(columns={'year_s_BC': 'year_s_BC_end'},inplace=True) 
date_missing_qid_time=pd.merge(date_missing_qid_time,g,on='qid',how='left')

#century
date_missing_qid_time['century']=date_missing_qid_time['century'].str.extract(r'(\d+)',expand=False)
date_missing_qid_time['century']=date_missing_qid_time['century'].fillna(0.01)# because after the re-extracting, the missings turn back to nan
date_missing_qid_time[['century']] = date_missing_qid_time[['century']].astype(float)
date_missing_qid_time['century']=(date_missing_qid_time['century']-1)*100+50
date_missing_qid_time['century']=date_missing_qid_time['century'].apply(lambda x: np.NaN if x==-49 else x)# change 0 to nan, then 0 will not be involved in sorting
grouped_century=date_missing_qid_time['century'].groupby(date_missing_qid_time['qid'])
g=grouped_century.min()
g=g.reset_index()# change the index to column
g.rename(columns={'century': 'century_start'}, inplace=True) 
date_missing_qid_time=pd.merge(date_missing_qid_time,g,on='qid',how='left')

g=grouped_century.max()
g=g.reset_index()# change the index to column
g.rename(columns={'century': 'century_end'},inplace=True) 
date_missing_qid_time=pd.merge(date_missing_qid_time,g,on='qid',how='left')

#century_BC
date_missing_qid_time['century_BC']=date_missing_qid_time['century_BC'].str.extract(r'(\d+)',expand=False)
date_missing_qid_time['century_BC']=date_missing_qid_time['century_BC'].fillna(0.01)# because after the re-extracting, the missings turn back to nan
date_missing_qid_time[['century_BC']] = date_missing_qid_time[['century_BC']].astype(float)
date_missing_qid_time['century_BC']=(date_missing_qid_time['century_BC']-1)*(-100)-50
date_missing_qid_time['century_BC']=date_missing_qid_time['century_BC'].apply(lambda x: np.NaN if x==49 else x)# change 0 to nan, then 0 will not be involved in sorting
grouped_century_BC=date_missing_qid_time['century_BC'].groupby(date_missing_qid_time['qid'])
g=grouped_century_BC.min()
g=g.reset_index()# change the index into column
g.rename(columns={'century_BC': 'century_BC_start'}, inplace=True) 
date_missing_qid_time=pd.merge(date_missing_qid_time,g,on='qid',how='left')

g=grouped_century_BC.max()
g=g.reset_index()# change the index into column
g.rename(columns={'century_BC': 'century_BC_end'},inplace=True) 
date_missing_qid_time=pd.merge(date_missing_qid_time,g,on='qid',how='left')

#with_month
date_missing_qid_time['with_month']=date_missing_qid_time['with_month'].str.replace('January','01')
date_missing_qid_time['with_month']=date_missing_qid_time['with_month'].str.replace('February','02')
date_missing_qid_time['with_month']=date_missing_qid_time['with_month'].str.replace('March','03')
date_missing_qid_time['with_month']=date_missing_qid_time['with_month'].str.replace('April','04')
date_missing_qid_time['with_month']=date_missing_qid_time['with_month'].str.replace('May','05')
date_missing_qid_time['with_month']=date_missing_qid_time['with_month'].str.replace('June','06')
date_missing_qid_time['with_month']=date_missing_qid_time['with_month'].str.replace('July','07')
date_missing_qid_time['with_month']=date_missing_qid_time['with_month'].str.replace('August','08')
date_missing_qid_time['with_month']=date_missing_qid_time['with_month'].str.replace('September','09')
date_missing_qid_time['with_month']=date_missing_qid_time['with_month'].str.replace('October','10')
date_missing_qid_time['with_month']=date_missing_qid_time['with_month'].str.replace('November','11')
date_missing_qid_time['with_month']=date_missing_qid_time['with_month'].str.replace('December','12')

date_missing_qid_time['with_month_m']=date_missing_qid_time['with_month'].str.extract(r'(\d{2})',expand=False)
date_missing_qid_time['with_month_y']=date_missing_qid_time['with_month'].str.extract(r'((?<=_)\d+)',expand=False)
date_missing_qid_time['with_month_m']=date_missing_qid_time['with_month_m'].astype(float)
date_missing_qid_time['with_month_m']=date_missing_qid_time['with_month_m']/100
date_missing_qid_time['with_month_y']=date_missing_qid_time['with_month_y'].astype(float)
date_missing_qid_time['with_month_my']=date_missing_qid_time['with_month_y']+date_missing_qid_time['with_month_m']


grouped_with_month=date_missing_qid_time['with_month_my'].groupby(date_missing_qid_time['qid'])
g=grouped_with_month.min()
g=g.reset_index()# change the index into column
g.rename(columns={'with_month_my': 'with_month_start'}, inplace=True) 
date_missing_qid_time=pd.merge(date_missing_qid_time,g,on='qid',how='left')

g=grouped_with_month.max()
g=g.reset_index()# change the index to column
g.rename(columns={'with_month_my': 'with_month_end'},inplace=True) 
date_missing_qid_time=pd.merge(date_missing_qid_time,g,on='qid',how='left')

#present, the problem here is: if we don't do any thing to 'year_present' and 'present', if an item has values in these two variables, then this item will show twice in the final dataset, one with values in these two and one without values in these two.
p=date_missing_qid_time.loc[:,['qid','year_present','present']]
p=p.replace(0,np.nan)
pr=p[p.year_present.notnull()|p.present.notnull()]
date_missing_qid_time.drop(['year_present','present'],axis=1,inplace=True)#drop these two variables because we have to use pr to match the date_e_missing_qid_time
date_missing_qid_time=pd.merge(date_missing_qid_time,pr,on='qid',how='left')


date_missing_qid_time.drop(['start','end','point_in_time','conflicts_in_year','year_in','year','year_by','year_BC','add_year',
                            'conflicts_in_year_s','year_s_in','year_s','year_s_BC','century','century_BC','with_month','with_month_m','with_month_y','with_month_my'],axis=1,inplace=True)#for the next merging operation, drop the old variables 
date_missing_qid_time.drop_duplicates(inplace=True)
selected_qid_time_cat=pd.merge(selected_qid_time,date_missing_qid_time,on='qid',how='left')
#####step 3: calculate the start and end
###1.get start_time of each accuracy
selected_qid_time_cat=selected_qid_time_cat.fillna(9999)# because if we use a!=None, there are always bugs, so we change the null into a number. At first, we calculate the start using min function , so using 9999 to make sure that null will not be involved
def function(a,b):#year. Notice, here we keep the charactoristic that those items with end but no start. So in the end we had better check these items
    if a!=9999:
        return a
    elif b!=9999:
        return b
selected_qid_time_cat['start_time_origin']=selected_qid_time_cat.apply(lambda x: function(x.start,x.point_in_time), axis=1)

def function1(d,e,f,g,h,i,j):#year
    if d!=9999 or e!=9999 or f!=9999 or g!=9999 or h!=9999 or i!=9999:
        return min(d,e,f,g,h,i)
    elif j!=9999:
        return j     
selected_qid_time_cat['start_time_year']=selected_qid_time_cat.apply(lambda x: function1(x.conflicts_in_year_start,x.year_in_start, x.year_start,x.year_by_start,x.year_BC_start,x.add_year_start,x.year_present), axis=1)    

def function2(a,b,c,d):#decade
    if a!=9999 or b!=9999 or c!=9999 or d!=9999:
        return min(a,b,c,d)
selected_qid_time_cat['start_time_decade']=selected_qid_time_cat.apply(lambda x: function2(x.conflicts_in_year_s_start,x.year_s_in_start,x.year_s_start,x.year_s_BC_start), axis=1)

def function3(a,b):#century
    if a!=9999 or b!=9999:
        return min(a,b)
selected_qid_time_cat['start_time_century']=selected_qid_time_cat.apply(lambda x: function3(x.century_start,x.century_BC_start), axis=1)

def function4(a):#month
    if a!=9999 :
        return a
selected_qid_time_cat['start_time_month']=selected_qid_time_cat.apply(lambda x: function4(x.with_month_start), axis=1)

###2.get end_time of each accuracy
selected_qid_time_cat=selected_qid_time_cat.replace(9999,np.nan)
selected_qid_time_cat=selected_qid_time_cat.fillna(-9999)#Now, we calculate the end using max function , so using -9999 to make sure that null will not be involved
def function55(a,b):#year. Notice, here we keep the charactoristic that those items with start but no end. So in the end we had better check these items
    if a!=-9999:
        return a
    elif b!=-9999:
        return b
selected_qid_time_cat['end_time_origin']=selected_qid_time_cat.apply(lambda x: function55(x.end,x.point_in_time), axis=1) 
def function5(d,e,f,g,h,i,j):  
    if d!=-9999 or e!=-9999 or f!=-9999 or g!=-9999 or h!=-9999 or i!=-9999:
        return max(d,e,f,g,h,i)
    elif j!=-9999:
        return j     
selected_qid_time_cat['end_time_year']=selected_qid_time_cat.apply(lambda x: function5(x.conflicts_in_year_end,x.year_in_end, x.year_end,x.year_by_end,x.year_BC_end,x.add_year_end,x.present), axis=1)    

def function6(a,b,c,d):#decade
    if a!=-9999 or b!=-9999 or c!=-9999 or d!=-9999:
        return max(a,b,c,d)
selected_qid_time_cat['end_time_decade']=selected_qid_time_cat.apply(lambda x: function6(x.conflicts_in_year_s_end,x.year_s_in_end,x.year_s_end,x.year_s_BC_end), axis=1)

def function7(a,b):#century
    if a!=-9999 or b!=-9999:
        return max(a,b)
selected_qid_time_cat['end_time_century']=selected_qid_time_cat.apply(lambda x: function7(x.century_end,x.century_BC_end), axis=1)

def function8(a):#month
    if a!=-9999 :
        return a
selected_qid_time_cat['end_time_month']=selected_qid_time_cat.apply(lambda x: function8(x.with_month_end), axis=1)
selected_qid_time_cat=selected_qid_time_cat.replace(-9999,np.nan)

selected_qid_time_cat_2=selected_qid_time_cat.loc[:,['qid','qidLabel','start_time_origin','start_time_year','start_time_decade','start_time_century','start_time_month','end_time_origin','end_time_year','end_time_decade','end_time_century','end_time_month']]

###3. get start_time
selected_qid_time_cat_2['start_origin']=selected_qid_time_cat_2['start_time_origin'].str.extract(r'([-]?\d+(?=-))',expand=False)
selected_qid_time_cat_2[['start_origin']] = selected_qid_time_cat_2[['start_origin']].astype(float)

selected_qid_time_cat_2['end_origin']=selected_qid_time_cat_2['end_time_origin'].str.extract(r'([-]?\d+(?=-))',expand=False)
selected_qid_time_cat_2[['end_origin']] = selected_qid_time_cat_2[['end_origin']].astype(float)# get the year data from the original start , end to check which items were happened before 1400

selected_qid_time_cat_2=selected_qid_time_cat_2.fillna(9999)# because if we use a!=None, there are always bugs, so we change the null into a number. At first, we calculate the start using min function , so using 9999 to make sure that null will not be involved
def function9(a,aa,b,c,d,e):
    if a!=9999:
        return a
    elif aa!=9999:#here we keep the charactoristic that those items with end but no start. So in the end we had better check these items
        return a
    elif b!=9999:
        return b
    elif c!=9999:
        return c
    elif d!=9999:
        return d
    elif e!=9999:
        return e
selected_qid_time_cat_2['start_time']=selected_qid_time_cat_2.apply(lambda x: function9(x.start_origin,x.end_origin,x.start_time_month,x.start_time_year,x.start_time_decade,x.start_time_century), axis=1)

def function99(a,aa,b,c,d,e):##add accuracy
    if a!=9999:
        return 'year'
    elif aa!=9999:
        return a
    elif b!=9999:
        return 'month'
    elif c!=9999:
        return 'year'
    elif d!=9999:
        return 'decade'
    elif e!=9999:
        return 'century'
selected_qid_time_cat_2['accuracy_start']=selected_qid_time_cat_2.apply(lambda x: function99(x.start_origin,x.end_origin,x.start_time_month,x.start_time_year,x.start_time_decade,x.start_time_century), axis=1)

selected_qid_time_cat_2=selected_qid_time_cat_2.replace(9999,np.nan)
selected_qid_time_cat_2=selected_qid_time_cat_2.fillna(-9999)#Now, we calculate the end using max function , so using -9999 to make sure that null will not be involved
def function10(a,aa,b,c,d,e):
    if a!=-9999:
        return a
    elif aa!=-9999: #here we keep the charactoristic that those items with start but without end. So in the end we had better check these items
        return a
    elif b!=-9999:
        return b
    elif c!=-9999:
        return c
    elif d!=-9999:
        return d
    elif e!=-9999:
        return e
selected_qid_time_cat_2['end_time']=selected_qid_time_cat_2.apply(lambda x: function10(x.end_origin,x.start_origin,x.end_time_month,x.end_time_year,x.end_time_decade,x.end_time_century), axis=1)
def function101(a,aa,b,c,d,e):#add accuracy
    if a!=-9999:
        return 'year'
    elif aa!=-9999:
        return a
    elif b!=-9999:
        return 'month'
    elif c!=-9999:
        return 'year'
    elif d!=-9999:
        return 'decade'
    elif e!=-9999:
        return 'century'
selected_qid_time_cat_2['accuracy_end']=selected_qid_time_cat_2.apply(lambda x: function101(x.end_origin,x.start_origin,x.end_time_month,x.end_time_year,x.end_time_decade,x.end_time_century), axis=1)
selected_qid_time_cat_2=selected_qid_time_cat_2.replace(-9999,np.nan)
selected_qid_time_cat_2['end_time']=selected_qid_time_cat_2['end_time'].replace('present',np.nan)
selected_qid_time_cat_3=selected_qid_time_cat_2.loc[:,['qid','qidLabel','start_time_origin','end_time_origin','start_time','accuracy_start','end_time','accuracy_end']]

selected_qid_time_cat_3.to_csv(main_path+'output/selected_qid_time_cat_3_original_still.csv', index=False)
#selected_qid_time_cat_3=pd.read_csv(main_path+'output/selected_qid_time_cat_3_original_still.csv')
#over=selected_qid_time_cat_3[selected_qid_time_cat_3['start_time']>=2018]

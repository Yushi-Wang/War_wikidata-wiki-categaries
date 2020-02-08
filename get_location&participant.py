# -*- coding: utf-8 -*-
"""
Created on Mon Dec 10 15:25:59 2018
This file is for getting the location (also participant) information from the wiki categaries and wikidata.
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
mergecat_sele_qid_location=pd.read_csv(main_path+'input/mergecat_sele_qid_location.csv')
# summarize the number of categories that each war page has
z=mergecat_sele_qid_location.loc[:,'qid_cat']
sum_cat=mergecat_sele_qid_location.loc[:,['qid','qid_cat']]
sum_cat.drop_duplicates(inplace=True)
g_sum_cat=sum_cat['qid_cat'].groupby(sum_cat['qid'])
gg=g_sum_cat.count()
gg_frame=gg.reset_index()
gg_frame.to_csv(main_path+'tem/summary_cat.csv', index=False)
min(gg)

count=pd.value_counts(gg_frame['qid_cat'])
count=count.reset_index()
count.to_csv(main_path+'tem/count_frequency.csv', index=False)




###########step1:get the information from the categories###################
qid_MtCountry=pd.read_csv(main_path+'material/qid_MtCountry.tsv',delimiter='\t')
qid_Subject=pd.read_csv(main_path+'material/qid_Subject.tsv',delimiter='\t')
qid_Subject.rename(columns={'qid':'qid_country'},inplace=True)
qid_MtCountry=pd.merge(qid_MtCountry,qid_Subject,on='qid_country',how='left')


mergecat_sele_qid_location['location_or_participants1']=mergecat_sele_qid_location['subject_cat'].str.extract(r'((?<=History_of_).*$|(?<=history_of_).*$|(?<=Military_of_).*|(?<=Wars_of_)[\w–]+|(?=<History_of_the_).*(?=_\([\d–]+\))|(?<=Wars_involving_).*|(?<=Battles_involving_).*|(?<=\d_in_)[\w–]+|(?<=\ds_in_).*|(?<=century_in_).*|(?<=century_BC_in_).*)',expand=False)#we should add $ here, because if there is categaries like" conflicts_in_134_BC", then here we get 134, but in year_BC, we get 134_BC
mergecat_sele_qid_location['location_or_participants_ch']=mergecat_sele_qid_location['subject_cat'].str.extract(r'(zh/刘宋政治\
|zh/春秋战国战役\
|zh/河北历次战争与战役\
|zh/河南历次战争与战役\
|zh/湖北历次战争与战役\
|zh/湖南历次战争与战役\
|zh/黑龙江历次战争与战役\
|zh/吉林历次战争与战役\
|zh/辽宁历次战争与战役\
|zh/山东历次战争与战役\
|zh/北京历次战争与战役\
|zh/天津历次战争与战役\
|zh/山西历次战争与战役\
|zh/陕西历次战争与战役\
|zh/甘肃历次战争与战役\
|zh/青海历次战争与战役\
|zh/福建历次战争与战役\
|zh/浙江历次战争与战役\
|zh/台湾历次战争与战役\
|zh/江西历次战争与战役\
|zh/江苏历次战争与战役\
|zh/安徽历次战争与战役\
|zh/广东历次战争与战役\
|zh/海南历次战争与战役\
|zh/四川历次战争与战役\
|zh/贵州历次战争与战役\
|zh/云南历次战争与战役\
|zh/上海历次战争与战役\
|zh/重庆历次战争与战役\
|zh/内蒙古历次战争与战役\
|zh/新疆历次战争与战役\
|zh/宁夏历次战争与战役\
|zh/广西历次战争与战役\
|zh/西藏历次战争与战役\
|zh/中华民国北洋政府时期内战战役\
|中国战役\
|中国战争\
|中国军事\
|zh/\d+年代中国\
|民国时期战役\
|zh/南燕国\
|zh/北周战役\
|zh/北齐战役\
|中原大战战役\
|zh/厦门军事史\
|zh/郑州军事史\
|zh/五胡十六国战役\
|zh/宋辽战争\
|zh/齐国战役\
|zh/吴国战争\
|zh/吴国战役\
|zh/東周\
|国共内战\
|zh/刘宋战争\
|zh/北魏战争\
|zh/楚国战争\
|zh/秦国战役\
|zh/楚国战役\
|zh/春秋时代战役\
|zh/明太祖北伐战役\
|zh/晋朝战役\
|zh/晋国战役\
|zh/宋國\
|zh/赵国战争\
|zh/齐国战争\
|zh/曹魏军事\
|zh/东吴军事\
|zh/五胡十六国战争\
|zh/护国战争战役\
|zh/齐国战役\
|zh/台灣清治時期民變事件\
|zh/辽朝战争\
|zh/后晋战争\
|zh/宋夏战争\
|zh/西夏战役\
|zh/南唐战争\
|zh/后周战争\
|zh/明朝民变\
|zh/刘宋战争\
|zh/后周战役\
|zh/赵国战争\
|zh/北宋战役\
|zh/南北朝战役\
|zh/赵国战役\
|zh/魏国战役\
|zh/北周战争\
|zh/北齐战争\
|zh/秦朝战役\
|zh/北周军事\
|国共冲突\
|zh/北宋战争\
|zh/匈奴战役\
|zh/辽金战役\
|zh/南唐战役\
|zh/北宋战役\
|zh/韩国战役\
|zh/韩国战争\
|zh/宋蒙战争\
|zh/辽朝战役\
|zh/北汉战役\
|护国战争\
|zh/北伐战役\
|zh/秦末民变\
|zh/东魏战役\
|zh/西魏战役\
|zh/三國歷史事件\
|Song_dynasty\
|Battles_of_the_Chinese_Civil_War\
|Jin–Song_Wars\
|zh/商朝战争\
|zh/安史之乱\
|zh/安史之乱战役\
|zh/南宋战役\
|zh/西汉战役\
|zh/宋金战役\
|zh/东晋战役\
|zh/北魏战役)',expand=False)
mergecat_sele_qid_location['location_or_participants_ch']=mergecat_sele_qid_location['location_or_participants_ch'].fillna(0)
def function_ch(a):
    if a!=0:
        return 'China'
    else:
        return a
mergecat_sele_qid_location['location_or_participants_ch1']=mergecat_sele_qid_location.apply(lambda x: function_ch(x.location_or_participants_ch), axis=1)
mergecat_sele_qid_location=mergecat_sele_qid_location.replace(0,np.nan)
#the_Second_Sino-Japanese_War抗日战争
mergecat_sele_qid_location['location_or_participants_chJa']=mergecat_sele_qid_location['subject_cat'].str.extract(r'(抗日战争|Battles_of_the_Second_Sino-Japanese_War)', expand=False)
china_japan_j=mergecat_sele_qid_location[mergecat_sele_qid_location.location_or_participants_chJa.notnull()]
china_japan_j['C_J']='Japan'

mergecat_sele_qid_location['location_or_participants_chJa']=mergecat_sele_qid_location['location_or_participants_chJa'].fillna(0)
mergecat_sele_qid_location['C_J']=mergecat_sele_qid_location.apply(lambda x: function_ch(x.location_or_participants_chJa), axis=1)
mergecat_sele_qid_location=mergecat_sele_qid_location.replace(0,np.nan)
mergecat_sele_qid_location=pd.concat([mergecat_sele_qid_location,china_japan_j])
mergecat_sele_qid_location=mergecat_sele_qid_location.reset_index(drop=True)
#china_japan=mergecat_sele_qid_location[mergecat_sele_qid_location.location_or_participants_chJa.notnull()]
#qid_china_japan=china_japan.loc[:,'qid']
#qid_china_japan.drop_duplicates(inplace=True)
#qid_china_japan=list(qid_china_japan)
#CvsJ=mergecat_sele_qid_location[mergecat_sele_qid_location.qid.isin(qid_china_japan)]

#高句丽战争
mergecat_sele_qid_location['location_or_participants_chGao']=mergecat_sele_qid_location['subject_cat'].str.extract(r'(zh/高句丽战争|Transition_from_Sui_to_Tang)', expand=False)
china_Gao_g=mergecat_sele_qid_location[mergecat_sele_qid_location.location_or_participants_chGao.notnull()]
china_Gao_g=china_Gao_g[china_Gao_g['qid']!=15908045]
china_Gao_g['Gao']="China"
mergecat_sele_qid_location['location_or_participants_chGao']=mergecat_sele_qid_location['location_or_participants_chGao'].fillna(0)
def function_Gao(a):
    if a!=0:
        return 'Korea'
    else:
        return a
mergecat_sele_qid_location['Gao']=mergecat_sele_qid_location.apply(lambda x: function_Gao(x.location_or_participants_chGao), axis=1)
mergecat_sele_qid_location=mergecat_sele_qid_location.replace(0,np.nan)
mergecat_sele_qid_location=pd.concat([mergecat_sele_qid_location,china_Gao_g])
mergecat_sele_qid_location=mergecat_sele_qid_location.reset_index(drop=True)


mergecat_sele_qid_location['location_or_participants1']=mergecat_sele_qid_location['location_or_participants1'].fillna(0)
mergecat_sele_qid_location['location_or_participants_ch1']=mergecat_sele_qid_location['location_or_participants_ch1'].fillna(0)
mergecat_sele_qid_location['C_J']=mergecat_sele_qid_location['C_J'].fillna(0)
mergecat_sele_qid_location['Gao']=mergecat_sele_qid_location['Gao'].fillna(0)
def function_ch_country(a,b,c,d):
    if a!=0:
        return a
    elif b!=0:
        return b
    elif c!=0:
        return c
    elif d!=0:
        return d
    else:
        return d
mergecat_sele_qid_location['location_or_participants2']=mergecat_sele_qid_location.apply(lambda x: function_ch_country(x.location_or_participants1,x.location_or_participants_ch1,x.C_J,x.Gao), axis=1)
mergecat_sele_qid_location=mergecat_sele_qid_location.replace(0,np.nan)
#other special conflicts
mergecat_sele_qid_location['others']=mergecat_sele_qid_location['subject_cat'].str.extract(r'([^\d]+(?=_in_the_American_Civil_War)\
|House_of_Guttenberg\
|(?<=Sieges_involving_).+\
|Mamluks\
|Merovingian_dynasty\
|Franks\
|(?<=Battles_of_World_War_II_involving_).+\
|(?<=Battles_of_World_War_I_involving_).+\
|(?<=Military_operations_of_World_War_II_involving_).+\
|(?<=World_War_II_)British(?=_Commando_raids)\
|(?<=World_War_II_in_)\D+\
|\D+(?=_in_World_War_II)\
|(?<=Naval_battles_involving_).+\
|County_of_Holland\
|House_of_Loon\
|(?<=Battles_and_operations_of_World_War_II_involving_).+\
|(?<=Naval_battles_of_World_War_I_involving_).+\
|(?<=Naval_battles_of_World_War_II_involving_).+\
|(?<=Contemporary_)German(?=_history)\
|(?<=Civil_wars_involving_the_states_and_peoples_of_).+\
|^[A-Za-z]+_peoples$\
|.+(?=_in_the_War_of_1812)\
|(?<=Battles_of_the_Iraq_War_involving_).+\
|^Iraq(?=i_insurgency_\(2003–11\)$)\
|(?<=Battles_of_the_)Iraq(?=_War_in_\d{4})\
|Toucouleur_Empire\
|Swedish(?=_battle_stubs)\
|Danish(?=_battle_stubs)\
|Western_Roman_Empire\
|Goths\
|(?<=Politics_of_)Italy\
|(?<=World_War_II_operations_and_battles_of_the_)Italian(?=_Campaign)\
|German_Empire(?=_in_World_War_I)\
|Livonian(?=_people)\
|Emirate_of_Córdoba\
|(?<=Internal_territorial_disputes_of_the_)United_States\
|(?<=Medieval_)Palestine$\
|Qays\
|(?<=Confederate_victories_of_the_)America(?=n_Civil_War)\
|(?<=Military_operations_of_the_American_Civil_War_in_).+\
|(?<=1838_in_Ottoman_)Syria$\
|(?<=Wars_involving_Ottoman_)Egypt$\
|(?<=Aerial_operations_and_battles_involving_).+\
|(?<=Battle_of_)Jutland$\
|(?<=Rebellions_in_)\D+\
|(?<=Conflicts_in_)Guangdong\
|(?<=Protests_in_)\D+\
|(?<=Battles_of_the_)Libyan(?=_Civil_War_\(2011\))\
|^Brazil(?=ian_rebels)\
|(?<=Former_subdivisions_of_)Brazil\
|Mapuche(?=_history)\
|(?<=Military_operations_of_the_)Syria(?=n_Civil_War_in_2012)\
|(?<=Military_operations_of_the_)Iraq(?=_War_in_2007)\
|(?<=Piracy_in_)Somalia\
|(?<=Anti-piracy_battles_involving_).+\
|(?<=Battles_of_the_War_in_)Afghanistan(?=_\(2001–present\))\
|Hawazin\
|(?<=Battles_of_the_)Venezuela(?=n_War_of_Independence)\
|Seljuk_Empire\
|Seljuq_dynasty\
|(?<=Battles_of_the_American_Civil_War_in_).+\
|(?<=Rebellions_against_).+\
|(?<=Battles_of_the_)America(?=n_Civil_War)\
|(?<=Invasions_by_).+\
|(?<=Invasions_of_).+\
|(?<=Battles_of_the_)Sengoku_period\
|(?<=Military_operations_of_the_Iraq_War_involving_).+\
|Chickasaw\
|United_Kingdom(?=_in_World_War_I)\
|(?<=2008–2009_)Sri_Lanka(?=n_Army_Northern_offensive)\
|(?<=Sieges_of_)Herat\
|(?<=Ancient_)[A-Za-z-]+\
|Mesoamerica\
|Illyria(?=n_warfare)\
|(?<=Military_operations_of_the_)Iraq(?=i_Civil_War_in_2014)\
|^Maya(?=_people)\
|(?<=History_of_the_canton_of_)Bern\
|(?<=Medieval_)[A-Za-z-]+\
|^Duchy_of_.+\
|(?<=Military_operations_of_the_)Syria(?=n_Civil_War_involving_the_Syrian_government)\
|(?<=Military_operations_of_the_Syrian_Civil_War_involving_).+\
|Kutrigurs\
|(?<=September_2017_events_in_)Syria\
|Empire_of_Nicaea\
|Latin_Empire\
|(?<=Islamic_State_of_Iraq_and_the_Levant_and_the_)Philippines\
|(?<=Attacks_in_)Pakistan(?=_in_2014)\
|Islamic_State_of_Iraq_and_the_Levant\
|(?<=7th-century_)Arabs\
|(?<=Terrorist_incidents_in_)Pakistan(?=_in_2013))', expand=False)
def function_others(a,b):
    if a!=0:
        return a
    else:
        return b
mergecat_sele_qid_location['location_or_participants2']=mergecat_sele_qid_location['location_or_participants2'].fillna(0)
mergecat_sele_qid_location['others']=mergecat_sele_qid_location['others'].fillna(0)
mergecat_sele_qid_location['location_or_participants']=mergecat_sele_qid_location.apply(lambda x: function_others(x.location_or_participants2,x.others), axis=1)
mergecat_sele_qid_location=mergecat_sele_qid_location.replace(0,np.nan)


#xxxx=mergecat_sele_qid_location[mergecat_sele_qid_location.others.notnull()]




mergecat_sele_qid_location['l_or_p_no_the']=mergecat_sele_qid_location['location_or_participants'].str.extract(r'^(?:the_)?(.*)',expand=False)#remove "the_"
mergecat_sele_qid_location['l_or_p_no_the_nosp']=mergecat_sele_qid_location['l_or_p_no_the'].str.extract(r'^(?:states_and_peoples_of_)?(.*)',expand=False) #remove "states_and_peoples_of_"
mergecat_sele_qid_location['l_or_p_no_the_nosp']=mergecat_sele_qid_location['l_or_p_no_the_nosp'].apply(lambda x: np.NaN if x=='Religion' else x)#drop out "Religion", because there is a cetigory "French wars of Religion"
mergecat_sele_qid_location['l_or_p_noancient']=mergecat_sele_qid_location['l_or_p_no_the_nosp'].str.extract(r'^(?:ancient_)?(.*)',expand=False)#remove "antient_"
mergecat_sele_qid_location['l_or_p']=mergecat_sele_qid_location['l_or_p_noancient'].str.extract(r'(^[A-Z].*)',expand=False)#drop out the strings that start with a lowercase letter, because usually a place or a regime should start with a capital letter
mergecat_sele_qid_location['l_or_p_1']=mergecat_sele_qid_location['l_or_p'].str.extract(r'(\D+)(?:_during_World_War_.*)',expand=False)
#remove _during_World_War_I or _during_World_War_II
mergecat_sele_qid_location['l_or_p_1']=mergecat_sele_qid_location['l_or_p_1'].fillna(0)
mergecat_sele_qid_location['l_or_p']=mergecat_sele_qid_location['l_or_p'].fillna(0)
def function(a,b):
    if b!=0:
        return b
    else:
        return a
mergecat_sele_qid_location['l_or_p']=mergecat_sele_qid_location.apply(lambda x: function(x.l_or_p,x.l_or_p_1), axis=1)
mergecat_sele_qid_location=mergecat_sele_qid_location.replace(0,np.nan)
#remove by_period
mergecat_sele_qid_location['l_or_p_2']=mergecat_sele_qid_location['l_or_p'].str.extract(r'(.*)(?:_by_period)$',expand=False)
mergecat_sele_qid_location['l_or_p_2']=mergecat_sele_qid_location['l_or_p_2'].fillna(0)
mergecat_sele_qid_location['l_or_p']=mergecat_sele_qid_location['l_or_p'].fillna(0)
def function(a,b):
    if b!=0:
        return b
    else:
        return a
mergecat_sele_qid_location['l_or_p']=mergecat_sele_qid_location.apply(lambda x: function(x.l_or_p,x.l_or_p_2), axis=1)
mergecat_sele_qid_location=mergecat_sele_qid_location.replace(0,np.nan)


#remove (****-****), like United_States_(1688-1978)
mergecat_sele_qid_location['l_or_p_n']=mergecat_sele_qid_location['l_or_p'].str.extract(r'(\D*)(?=_\(.*\))',expand=False)#remove (****-****), like United_States_(1688-1978)
mergecat_sele_qid_location['l_or_p_n']=mergecat_sele_qid_location['l_or_p_n'].fillna(0)
mergecat_sele_qid_location['l_or_p']=mergecat_sele_qid_location['l_or_p'].fillna(0)#remove (****-****), like United_States_(1688-1978)
def function(a,b):##remove (****-****), like United_States_(1688-1978)
    if b!=0:
        return b
    else:
        return a
mergecat_sele_qid_location['L_or_P']=mergecat_sele_qid_location.apply(lambda x: function(x.l_or_p,x.l_or_p_n), axis=1)#remove (****-****), like United_States_(1688-1978)
mergecat_sele_qid_location=mergecat_sele_qid_location.replace(0,np.nan)#remove (****-****), like United_States_(1688-1978)
##clean the L_or_P
mergecat_sele_qid_location['LP']=mergecat_sele_qid_location['L_or_P'].str.extract(r'(Sea)',expand=False)
def function_nosea(a,b):
    if a=='Sea':
        return 0
    else:
        return b
mergecat_sele_qid_location['L_or_P']=mergecat_sele_qid_location.apply(lambda x: function_nosea(x.LP,x.L_or_P), axis=1)#remove (****-****), like United_States_(1688-1978)
mergecat_sele_qid_location=mergecat_sele_qid_location.replace(0,np.nan)

    
    





mergecat_sele_qid_location.drop(['location_or_participants','l_or_p_no_the','l_or_p_no_the_nosp','l_or_p_noancient','l_or_p','l_or_p_n','l_or_p_1','l_or_p_2'],axis=1, inplace=True)
mergecat_sele_qid_location['qid_cat']=mergecat_sele_qid_location['qid_cat'].fillna(0)
mergecat_sele_qid_location[['qid_cat']] =mergecat_sele_qid_location[['qid_cat']].astype(int)
mergecat_sele_qid_location=mergecat_sele_qid_location.replace(0,np.nan)
###################step2:merge with country and get country information#######################
qid_MtCountry=qid_MtCountry.loc[:,['qid_cat','qid_country','subject']]
qid_MtCountry.drop_duplicates(inplace=True)
mergecat_sele_qid_location_WP=pd.merge(mergecat_sele_qid_location,qid_MtCountry,on='qid_cat',how='left')

A1_05_03_mapCountry=pd.read_stata(main_path+'material/A1_05_03_mapCountry.dta')
A1_05_03_mapCountry.rename(columns={'location':'L_or_P'},inplace=True)
mergecat_sele_qid_location_WP=pd.merge(mergecat_sele_qid_location_WP,A1_05_03_mapCountry,on='L_or_P',how='left')

#get country information from "location"
mergecat_sele_qid_location_WP['locationLabel']=mergecat_sele_qid_location_WP['locationLabel'].str.replace(" ","_")
A1_05_03_mapCountry.rename(columns={'L_or_P':'locationLabel'},inplace=True)
A1_05_03_mapCountry.rename(columns={'country':'country_locationLabel_bselect'},inplace=True)
mergecat_sele_qid_location_WP=pd.merge(mergecat_sele_qid_location_WP,A1_05_03_mapCountry,on='locationLabel',how='left')
locationLabel=mergecat_sele_qid_location_WP.loc[:,['locationLabel','country_locationLabel_bselect']]
locationLabel.drop_duplicates(inplace=True)
gl_label=locationLabel['country_locationLabel_bselect'].groupby(locationLabel['locationLabel'])
gl_l=gl_label.count()
gl_l=gl_l.reset_index()
selected_locationLabel=gl_l[gl_l['country_locationLabel_bselect']==1]
selected_locationLabel=selected_locationLabel.loc[:,'locationLabel']
selected_locationLabel=list(selected_locationLabel)
def function1(a,b):
    if a in selected_locationLabel:
        return b
    else:
        return np.nan
mergecat_sele_qid_location_WP['country_locationLabel']=mergecat_sele_qid_location_WP.apply(lambda x: function1(x.locationLabel,x.country_locationLabel_bselect), axis=1)
#ww=mergecat_sele_qid_location_WP[mergecat_sele_qid_location_WP.qid_country.notnull()&mergecat_sele_qid_location_WP.subject.isnull()]#every qid_country has been matched to a country name
#test how many items have no information about country,participants or location
#location_WP=mergecat_sele_qid_location_WP[mergecat_sele_qid_location_WP.qid_country.notnull()|mergecat_sele_qid_location_WP.L_or_P.notnull()|mergecat_sele_qid_location_WP.location.notnull()]#have information about participant,country or location
#qid_wp=location_WP.loc[:,'qid']
#qid_wp.drop_duplicates(inplace=True)

#qid_wp=list(qid_wp)

#withoutP_L_C=mergecat_sele_qid_location_WP[~mergecat_sele_qid_location_WP.qid.isin(qid_wp)]
#x=withoutP_L_C.loc[:,'qid']
#x.drop_duplicates(inplace=True)#get the serie of qid that have no information about country,participants nor location

#test how many items that have no information of exact country/participants information
#location_cp=mergecat_sele_qid_location_WP[mergecat_sele_qid_location_WP.qid_country.notnull()|mergecat_sele_qid_location_WP.country.notnull()]#have information about participant,country or location
#qid_exact_cp=location_cp.loc[:,'qid']
#qid_exact_cp.drop_duplicates(inplace=True)

#qid_exact_cp=list(qid_exact_cp)

#without_cp=mergecat_sele_qid_location_WP[~mergecat_sele_qid_location_WP.qid.isin(qid_exact_cp)]
#y=without_cp.loc[:,'qid']
#y.drop_duplicates(inplace=True)#get the serie of qid that have no information about the exact country


#get the list if L_or_P that can not be matched by A1_05_03_mapCountry
#list_not_match_country=mergecat_sele_qid_location_WP.loc[:,['L_or_P','country']]
#list_not_match_country=list_not_match_country[list_not_match_country.country.isnull()&list_not_match_country.L_or_P.notnull()]
#list_not_match_country.drop_duplicates(inplace=True)
#list_not_match_country.to_csv(main_path+'output/list_not_match_country.csv', index=False)

#aggregate the information about country
mergecat_sele_qid_location_WP['qid_country']=mergecat_sele_qid_location_WP['qid_country'].fillna(0)
mergecat_sele_qid_location_WP['subject']=mergecat_sele_qid_location_WP['subject'].fillna(0)
mergecat_sele_qid_location_WP['country']=mergecat_sele_qid_location_WP['country'].fillna(0)
mergecat_sele_qid_location_WP['country_locationLabel']=mergecat_sele_qid_location_WP['country_locationLabel'].fillna(0)
def function2(a,b,c,d):
    if a!=0:
        return a
    elif b!=0:
        return b
    elif c!=0:
        return c
    elif d!=0:
        return d
    else:
        return a
mergecat_sele_qid_location_WP['country_ag1']=mergecat_sele_qid_location_WP.apply(lambda x: function2(x.subject,x.qid_country,x.country,x.country_locationLabel), axis=1)#although subject,qid_country,country are got from categories, but because of this, these three may contain the information of participants. If we put contry_locationLabel prior to these three, then we may loss the infprmation of participants.
mergecat_sele_qid_location_WP=mergecat_sele_qid_location_WP.replace(0,np.nan)
# further cleaning for country information
A1_05_03_mapCountry.rename(columns={'locationLabel':'country_ag1'},inplace=True)
A1_05_03_mapCountry.rename(columns={'country_locationLabel_bselect':'country_ag2'},inplace=True)
mergecat_sele_qid_location_WP=pd.merge(mergecat_sele_qid_location_WP,A1_05_03_mapCountry,on='country_ag1',how='left')
ww=mergecat_sele_qid_location_WP[mergecat_sele_qid_location_WP.country_ag1.notnull()&mergecat_sele_qid_location_WP.country_ag2.isnull()]
ww=ww.loc[:,'country_ag1']
ww.drop_duplicates(inplace=True)
ww.to_csv(main_path+'tem/countrylabel_NM_country.csv')
mergecat_sele_qid_location_WP['country_ag1']=mergecat_sele_qid_location_WP['country_ag1'].fillna(0)
mergecat_sele_qid_location_WP['country_ag2']=mergecat_sele_qid_location_WP['country_ag2'].fillna(0)
def function22(a,b):
    if a!=0:
        return a
    elif b!=0:
        return b  
    else:
        return a
mergecat_sele_qid_location_WP['country_ag']=mergecat_sele_qid_location_WP.apply(lambda x: function22(x.country_ag2,x.country_ag1), axis=1)#although subject,qid_country,country are got from categories, but because of this, these three may contain the information of participants. If we put contry_locationLabel prior to these three, then we may loss the infprmation of participants.
mergecat_sele_qid_location_WP=mergecat_sele_qid_location_WP.replace(0,np.nan)
mergecat_sele_qid_location_WP['country_ag']=mergecat_sele_qid_location_WP['country_ag'].str.replace('Music_of_the_United_States','United_States')

#www=mergecat_sele_qid_location_WP[mergecat_sele_qid_location_WP.qid.isin(listqid)]
###################step3:merge with continent and get continent information#######################
A1_05_03_mapContinent=pd.read_stata(main_path+'material/A1_05_03_mapContinent.dta')
A1_05_03_mapContinent.rename(columns={'location':'L_or_P'},inplace=True)
A1_05_03_mapContinent.rename(columns={'continent':'continent1'},inplace=True)
A1_05_03_mapContinent.drop_duplicates(inplace=True)
mergecat_sele_qid_location_WP_WC=pd.merge(mergecat_sele_qid_location_WP,A1_05_03_mapContinent,on='L_or_P',how='left')
A1_05_03_mapContinent.rename(columns={'L_or_P':'country_ag'},inplace=True)
A1_05_03_mapContinent.rename(columns={'continent1':'continent2'},inplace=True)
mergecat_sele_qid_location_WP_WC=pd.merge(mergecat_sele_qid_location_WP_WC,A1_05_03_mapContinent,on='country_ag',how='left')
mergecat_sele_qid_location_WP_WC['country_ag']=mergecat_sele_qid_location_WP_WC['country_ag'].fillna(0)
mergecat_sele_qid_location_WP_WC['continent1']=mergecat_sele_qid_location_WP_WC['continent1'].fillna(0)
mergecat_sele_qid_location_WP_WC['continent2']=mergecat_sele_qid_location_WP_WC['continent2'].fillna(0)
def function3(a,b,c):
    if a!=0:
        return a
    elif b!=0:
        return b
    elif c!=0:
        return 1
    else:
        return a
mergecat_sele_qid_location_WP_WC['continent_ag']=mergecat_sele_qid_location_WP_WC.apply(lambda x: function3(x.continent1,x.continent2,x.country_ag), axis=1)
mergecat_sele_qid_location_WP_WC=mergecat_sele_qid_location_WP_WC.replace(0,np.nan)
###################step3:get location information#######################
mergecat_sele_qid_location_WP_WC['country_ag']=mergecat_sele_qid_location_WP_WC['country_ag'].fillna(0)
mergecat_sele_qid_location_WP_WC['continent_ag']=mergecat_sele_qid_location_WP_WC['continent_ag'].fillna(0)
def function4(a,b,c):
    if a!=0:
        return a
    elif b!=0:
        return b
    elif c!=0:
        return c

    else:
        return a
mergecat_sele_qid_location_WP_WC['location_ag']=mergecat_sele_qid_location_WP_WC.apply(lambda x: function4(x.country_ag,x.continent_ag,x.locationLabel), axis=1)
mergecat_sele_qid_location_WP_WC=mergecat_sele_qid_location_WP_WC.replace(0,np.nan)

qid_location_continent_country=mergecat_sele_qid_location_WP_WC.loc[:,['qid','country_ag','continent_ag','location_ag']]         
qid_location_continent_country.drop_duplicates(inplace=True)

g_qid_lcc=qid_location_continent_country['location_ag'].groupby(qid_location_continent_country['qid'])
gg_qid_lcc=g_qid_lcc.count()
gg_qid_lcc=gg_qid_lcc.reset_index()
no_location_qid=gg_qid_lcc[(gg_qid_lcc['location_ag']==0)]
no_location_qid=no_location_qid.loc[:,'qid']
no_location_qid=list(no_location_qid)


qid_location_continent_country['country_ag']=qid_location_continent_country['country_ag'].fillna(0)
qid_location_continent_country['continent_ag']=qid_location_continent_country['continent_ag'].fillna(0)
qid_location_continent_country['location_ag']=qid_location_continent_country['location_ag'].fillna(0)
def function5(a,b,c,d):
    if a not in no_location_qid and b==0 and c==0 and d==0:
        return 1
    else:
        return 0
qid_location_continent_country['mark']=qid_location_continent_country.apply(lambda x: function5(x.qid,x.country_ag,x.continent_ag,x.location_ag),axis=1)
qid_location_continent_country=qid_location_continent_country.replace(0,np.nan)
qid_location_continent_country=qid_location_continent_country[qid_location_continent_country.mark.isnull()]###remove the rows in which three variables are all missing but in fact the qid do have information of location.
qid_location_continent_country=qid_location_continent_country.loc[:,['qid','country_ag','continent_ag','location_ag']]
qid_location_continent_country.to_csv(main_path+'output/qid_location_continent_country.csv', index=False)

#extract the list of items that have no country information
g_nocountry=qid_location_continent_country['country_ag'].groupby(qid_location_continent_country['qid'])
gcount_nocountry=g_nocountry.count()
gcount_nocountry=gcount_nocountry.reset_index()
qid_withoutcountry=gcount_nocountry[gcount_nocountry['country_ag']==0]
qid_withoutcountry=qid_withoutcountry.loc[:,'qid']
qid_withoutcountry=list(qid_withoutcountry)
without_country=mergecat_sele_qid_location_WP[mergecat_sele_qid_location_WP['qid'].isin(qid_withoutcountry)]

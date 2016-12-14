# -*- coding: utf-8 -*-
"""
Created on Wed Oct 12 15:33:11 2016

@author: misras2
"""

import MySQLdb as mdb
import pandas as pd
import pandas.io.sql as sql
import numpy as np
import os

os.getcwd()
os.chdir('C:\\Users\\misras2\\Desktop\\CTD-PerfDB Visualization\\Analytics - Drive Wear % and Script')

conn = mdb.connect(host="XX.XXX.XXX.XXX",    # your host, usually localhost
                     user="XXXX",         # your username
                     passwd="XXXXXXXX",  # your password
                     db="XXXXXXXXX");
#df_tdsi = sql.read_sql('SELECT * FROM tb_drive_stats_info WHERE drive_id = 607', conn)
'''df_tdsi = sql.read_sql('SELECT * \
FROM performancedb.tb_drive_stats_info tdsi \
WHERE tdsi.id_drive_stats NOT IN (SELECT tdsi2.id_drive_stats \
								FROM performancedb.tb_drive_stats_info tdsi2 \
									LEFT JOIN performancedb.tb_drive_metadata_info tdmi ON tdmi.id_drive = tdsi2.drive_id \
									LEFT JOIN performancedb.tb_array_info tai ON tai.id_array = tdsi2.array_id \
                                    LEFT JOIN performancedb.tb_mel_config_basic tmcb ON tmcb.serial_number = tai.serial_number \
                                    LEFT JOIN performancedb.tb_testcase_execution_info tcei ON tcei.testset_id = tmcb.testset_execution_id \
                                WHERE tcei.script_id = 1971 \
                                    AND (tdsi2.creation_date BETWEEN tcei.start_time AND tcei.end_time)) \
AND tdsi.drive_id = 607', conn)'''

df_tdsi = sql.read_sql('SELECT * \
FROM performancedb.tb_drive_stats_info tdsi \
WHERE tdsi.id_drive_stats NOT IN (SELECT tdsi2.id_drive_stats \
								FROM performancedb.tb_drive_stats_info tdsi2, performancedb.tb_testcase_execution_info tcei \
                                WHERE tcei.script_id = 1971 \
                                    AND (tdsi2.creation_date BETWEEN tcei.start_time AND tcei.end_time)) \
AND tdsi.drive_id = 607', conn)




df_tdsi.head(200)
df_tdsi.iloc[:,[0,8]]
df_tdsi.loc[:,['drive_id','creation_date','wear_percentage']]

df_tdsi_filtered = df_tdsi.loc[:,['drive_id','creation_date','wear_percentage']]
#df_tdsi_filtered = df_tdsi.loc[23750:23765,['drive_id','creation_date','wear_percentage']]
df_tdsi_filtered

########Test#######
'''import math;

def nan(x):
    y=math.isnan(x)
    return y

math.isnan(df.shift(1,axis=0).iloc[0,1])
d = {'a':[1,1,1,2,2],'b':[1,1,1,2,2]}
df = pd.DataFrame(data=d,index=list(range(0,5)))

(d)
if (nan(df.shift(1,axis=0).iloc[0,0]) & nan(df.shift(1,axis=0).iloc[0,1])): 
    df.loc[0,'test']=0


df

nan(df.a.shift(1))

math.isnan(df_tdsi_filtered.shift(1).loc[:,'wear_percentage'])'''
###################


def pct_change(df_tdsi_filtered):
    if all(df_tdsi_filtered['wear_percentage'] != 0.00):
        df_tdsi_filtered['Percentage_Increase'] = 100 * (1 - df_tdsi_filtered.shift(1).wear_percentage / df_tdsi_filtered.wear_percentage)
    else:
        df_tdsi_filtered['Percentage_Increase'] = 100 * (df_tdsi_filtered.shift(1).wear_percentage - df_tdsi_filtered.wear_percentage)            
    return df_tdsi_filtered

'''def pct_change(df_tdsi_filtered):
    df_tdsi_filtered['Percentage_Increase'] = 100 * (1 - df_tdsi_filtered.shift(1).wear_percentage / df_tdsi_filtered.wear_percentage)
    return df_tdsi_filtered'''
    
    
df_tdsi_filtered_pct = df_tdsi_filtered.groupby('drive_id').apply(pct_change)
df_tdsi_filtered_pct['Percentage_Increase'] = np.round(df_tdsi_filtered_pct['Percentage_Increase'],2)
df_tdsi_filtered_pct = df_tdsi_filtered_pct.fillna(0)
df_tdsi_filtered_pct.dtypes
df_tdsi_filtered_pct


'''def weighted_moving_average_pct_change(df_tdsi_filtered_pct):
    if (df_tdsi_filtered_pct.shift(1).Percentage_Increase.isnull() | df_tdsi_filtered_pct.shift(2).Percentage_Increase.isnull() | df_tdsi_filtered_pct.shift(3).Percentage_Increase.isnull()):
                df_tdsi_filtered_pct['Weighted_MA'] = 0
    else:
        df_tdsi_filtered_pct['Weighted_MA'] = (((3*df_tdsi_filtered_pct.shift(3).Percentage_Increase)+(2*df_tdsi_filtered_pct.shift(2).Percentage_Increase)+(1*df_tdsi_filtered_pct.shift(1).Percentage_Increase)) / 5)
    return df_tdsi_filtered_pct'''
    
def weighted_moving_average_pct_change(df_tdsi_filtered_pct):
    df_tdsi_filtered_pct['Weighted_MA'] = (((3*df_tdsi_filtered_pct.shift(3).Percentage_Increase)+(2*df_tdsi_filtered_pct.shift(2).Percentage_Increase)+(1*df_tdsi_filtered_pct.shift(1).Percentage_Increase)) / 5)
    return df_tdsi_filtered_pct

df_tdsi_filtered_pct_WMA = df_tdsi_filtered_pct.groupby('drive_id').apply(weighted_moving_average_pct_change)
df_tdsi_filtered_pct_WMA['Weighted_MA'] = np.round(df_tdsi_filtered_pct_WMA['Weighted_MA'],2)
df_tdsi_filtered_pct_WMA = df_tdsi_filtered_pct_WMA.fillna(0)
df_tdsi_filtered_pct_WMA

def diff_in_WMA_and_pct_change(df_tdsi_filtered_pct_WMA):
    df_tdsi_filtered_pct_WMA['Diff_in_WeightedMA_and_%Inc'] = df_tdsi_filtered_pct_WMA['Percentage_Increase'] - df_tdsi_filtered_pct_WMA['Weighted_MA'] 
    return df_tdsi_filtered_pct_WMA

df_tdsi_filtered_pct_WMA_diff = df_tdsi_filtered_pct_WMA.apply(diff_in_WMA_and_pct_change, axis=1)
df_tdsi_filtered_pct_WMA_diff

df_tdsi_filtered_pct_WMA_diff = df_tdsi_filtered_pct_WMA_diff.sort_values(by=['Diff_in_WeightedMA_and_%Inc'], ascending=False)

def steepness_check(df_tdsi_filtered_pct_WMA_diff):
    if df_tdsi_filtered_pct_WMA_diff['Percentage_Increase'] > df_tdsi_filtered_pct_WMA_diff['Weighted_MA']:
        df_tdsi_filtered_pct_WMA_diff['Steepness_Check'] = 1
    else:
        df_tdsi_filtered_pct_WMA_diff['Steepness_Check'] = 0
    return df_tdsi_filtered_pct_WMA_diff

df_tdsi_filtered_pct_WMA_diff_chk = df_tdsi_filtered_pct_WMA_diff.apply(steepness_check, axis=1)
df_tdsi_filtered_pct_WMA_diff_chk

#df_tdsi_filtered_pct_WMA_diff_chk.to_csv('Output.csv')
#df_tdsi_filtered_pct_WMA_diff_chk.to_csv('Output1.csv')
df_tdsi_filtered_pct_WMA_diff_chk.to_csv('Output2.csv')

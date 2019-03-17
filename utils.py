#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 26 12:23:55 2019

@author: jxf

"""
import pandas as pd
import numpy as np
import datetime
#has incomplete data. 999 points are NaN
def read_file(fname):
    with open(fname, "r") as f:
        try:
            line1= f.readline().split()
        except Exception as e:
            raise Exception("problem reading first line of file %s"%fname) from e
        if line1[0] not in ("YY","#YY","YYYY"):
            raise Exception("bad header line 1 for file %s: '%s'"%(fname,line1))
        try:
            line2= f.readline().split()
        except Exception as e:
            raise Exception("problem reading second line of file %s"%fname) from e
        try:
            int(line2[0])
            has_second_header= False
            header= 0
        except ValueError:
            if line2[0]in ("#yy","#yr"):
                has_second_header=True
                header=[0,1]
            else:
                raise Exception("unexpected second header in file %s"%fname)
 
    #this gives it a second header
    df= pd.read_csv(fname, sep='\s+', header= header)#allows you to read the file
    if has_second_header:
        df.columns = [h1 for (h1, h2) in df.columns]
    def mkyear(v):
        v = int(v)
        if v<100:
            return 1900 + v
        else:
            return v
    if 'mm' in df.columns:
        df['timestamp']=df.apply(lambda s:datetime.datetime(mkyear(s[0]), int(s[1]), int(s[2]), int(s[3]), int(s[4])),
                                 axis=1)
    else:
        df['timestamp']=df.apply(lambda s:datetime.datetime(mkyear(s[0]), int(s[1]), int(s[2]), int(s[3]), 0),
                                 axis=1)        
    df['ATMP'] = df['ATMP'].apply(lambda v:np.NaN if v>100 else v) # 999 is used to indicate no data available
    df['WTMP'] = df['WTMP'].apply(lambda v:np.NaN if v>100 else v) # 999 is used to indicate no data available  
    print("%s has %d entries" % (fname, len(df)))
    return df

def build_median_df(df, base_col, year,
                    index=['01-Jan', '02-Feb', '03-Mar', '04-Apr', '05-May', '06-Jun',
                           '07-Jul', '08-Aug', '09-Sep', '10-Oct', '11-Nov', '12-Dec']):
    if 'YY' in df.columns:
        df = df[(df['YY']==year) | (df['YY']==(year-1900))].copy()
    elif '#YY' in df.columns:
        df = df[df['#YY']==year].copy()
    elif 'YYYY' in df.columns:
        df = df[df['YYYY']==year].copy()
    else:
        assert 0, "Did not find a year column in %s for %s" % (df.columns, year)
    grouped=df.groupby(pd.Grouper(key = "timestamp", freq="M")).agg({base_col:['median']})
    grouped.columns=['%s %s median'%(year, base_col)]
    grouped['month'] = index
    grouped.set_index('month', drop=True, inplace=True)
    return grouped


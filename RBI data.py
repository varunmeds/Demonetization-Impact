# -*- coding: utf-8 -*-
"""
Created on Wed Jan 24 09:16:25 2018

@author: Suman
"""
import datetime as dt
import matplotlib.pyplot as plt
from matplotlib import style
import pandas as pd
import pandas_datareader.data as web
import os
import bs4 as bs
import pickle
import requests
from pandas_datareader._utils import RemoteDataError
import numpy as np
from sklearn import svm, cross_validation, neighbors
from sklearn.ensemble import VotingClassifier, RandomForestClassifier
import scipy.stats as stats
from bs4 import BeautifulSoup
# Python 3.x
from urllib.request import urlopen, urlretrieve

directoryname = "C:\\Users\Desktop\...." # set the address in your laptop where you want things saved
os.chdir(directoryname) 

def save_and_download():
    files=[]
    for x in range(80,117,1):
        print(x)
        URL = 'https://www.rbi.org.in/scripts/NEFTUserView.aspx?Id='+str(x)
        OUTPUT_DIR = ''  # path to output folder, '.' or '' uses current folder
        #if not os.path.exists('RBI_Data'):
         #   os.makedirs('RBI_Data') 
        #print(URL)
        u = urlopen(URL)
        try:
            html = u.read().decode('utf-8')
        finally:
            u.close()
        #print(x+3)
        soup = BeautifulSoup(html, "html.parser")
        #print('soup is done')
        for link in soup.select('a[href^="http://rbidocs.rbi.org.in/rdocs/NEFT/DOCs"]'):
            href = link.get('href')
            print('hello')
            if any(href.endswith(l) for l in ['.csv','.XLS','.xlsx']):
                continue
            filename = os.path.join(OUTPUT_DIR, href.rsplit('/', 1)[-1])
            print('this shit is crazy')
            files.append(filename)
        
            # We need a https:// URL for this site
            href = href.replace('http://','https://')
        
            print("Downloading %s to %s..." % (href, filename) )
            urlretrieve(href, filename)
            print("Done.")
            
            with open("datasetnames.pickle","wb") as f:
                pickle.dump(files,f)
            
    return files
    
save_and_download()


----------------------------------------------------------------------------------------------------------------------------------------

'''WORK IN PROGRESS'''

-----------------------------------------------------------------------------------------------------------------------------------------
def compile_data():
    with open("datasetnames.pickle","rb") as f:
        allfiles = pickle.load(f)

    main_df = pd.DataFrame()
    
    for count,files in enumerate(allfiles):
        #df = pd.ExcelFile('RTGS112017F8FF4971057645B79FF0AB54B8117BA1.XLS')
        #df = pd.ExcelFile('RBI_Data/{}.XLS'.format(files),header=[0,1])
        df = pd.ExcelFile('{}'.format(files))
        df1 = {sheet: df.parse(sheet) for sheet in df.sheet_names}
        for sheet in df.sheet_names:
            if 'ECS' in df.sheet_names:
                df_ECS=df1['ECS']
                df_ECS=df_ECS.loc[:, ~df_ECS.columns.str.contains('^Unnamed: 0')]
                df_ECS=df_ECS.rename(columns = {'ECS - All Centres\' Transaction Details For the month of November 2017':'S.No', 'Unnamed: 2':'Centre Name','Unnamed: 3':'Bank Name','Unnamed: 4':'ECS Credit No of users','Unnamed: 5':'ECS Credit Volume','Unnamed: 6':'ECS Credit Value','Unnamed: 7':'ECS Debit No of users','Unnamed: 8':'ECS Debit Volume','Unnamed: 9':'ECS Debit Value'})
                df_ECS=df_ECS.iloc[2:-2]
                for column in df_ECS.columns[3:9]:
                    df_ECS[column]=df_ECS[column].astype(str).astype(float)
                df_ECS['Number of Users'] = df_ECS.apply(lambda x: x['ECS Credit No of users']+x['ECS Debit No of users'],axis =1)
                df_ECS['Value']=df_ECS.apply(lambda x: x['ECS Credit Value']+x['ECS Debit Value'],axis =1)
                df_ECS['Volume']=df_ECS.apply(lambda x: x['ECS Credit Volume']+x['ECS Debit Volume'],axis =1)
                a=[]
                for column in ['Number of Users','Value','Volume']:
                    b=df_ECS[column].sum()
                    a=a.append(b)
                #arr=[df_ECS.columns[9:12],a]
                #tuples = list(zip(*arr))

                
            if 'RTGS' in df.sheet_names:
                df_RTGS=df1['RTGS']
                df_RTGS=df_RTGS.loc[:, ~df_RTGS.columns.str.contains('^Unnamed: 0')]
                df_RTGS=df_RTGS.rename(columns = {'Bank Wise RTGS Inward and Outward November 2017':'S.No', 'Unnamed: 2':'Participant','Unnamed: 3':'Inward Volume Interbank','Unnamed: 4':'Inward Volume Customer','Unnamed: 5':'Inward Volume Total','Unnamed: 6':'Inward Volume %','Unnamed: 7':'Inward Value Interbank','Unnamed: 8':'Inward Value Customer','Unnamed: 9':'Inward Value Total','Unnamed: 10':'Inward Value %','Unnamed: 11':'Outward Volume Interbank','Unnamed: 12':'Outward Volume Customer','Unnamed: 13':'Outward Volume Total','Unnamed: 14':'Outward Volume %','Unnamed: 15':'Outward Value Interbank','Unnamed: 16':'Outward Value Customer','Unnamed: 17':'Outward Value Total','Unnamed: 18':'Outward Value %'})
                df_RTGS=df_RTGS.iloc[3:-1]
                for column in df_RTGS.columns[2:18]:
                    df_RTGS[column]=df_RTGS[column].astype(str).astype(float)
                df_RTGS['Volume'] = df_RTGS.apply(lambda x: x['Inward Volume Total']+x['Outward Volume Total'],axis =1)
                df_RTGS['Value']=df_RTGS.apply(lambda x: x['Inward Value Total']+x['Outward Value Total'],axis =1)
                c=[]
                for column in ['Volume','Value']:
                    d=df_RTGS[column].sum()
                    c=c.append(d)

            
            if 'NEFT' in df.sheet_names:    
                df_NEFT=df1['NEFT']
                df_NEFT=df_NEFT.loc[:, ~df_NEFT.columns.str.contains('^Unnamed: 0')]
                df_NEFT=df_NEFT.rename(columns = {'NATIONAL ELECTRONIC FUND TRANSFER (NEFT) - NOVEMBER 2017':'S.No', 'Unnamed: 2':'Bank','Unnamed: 3':'Total outward debits No. of Transactions','Unnamed: 4':'Total outward debits Amount','Unnamed: 5':'Received Inward Credits - No of transactions','Unnamed: 6':'Received Inward Credits - Amount'})
                df_NEFT=df_NEFT.iloc[2:-2]
                for column in df_NEFT.columns[2:6]:
                    df_NEFT[column]=df_NEFT[column].astype(str).astype(float)
                df_NEFT['Volume'] = df_NEFT.apply(lambda x: x['Total outward debits No. of Transactions']+x['Received Inward Credits - No of transactions'],axis =1)
                df_NEFT['Value']=df_NEFT.apply(lambda x: x['Total outward debits Amount']+x['Received Inward Credits - Amount'],axis =1)
                e=[]
                for column in ['Volume','Value']:
                    f=df_NEFT[column].sum()
                    e=e.append(f)
                #df.rename(columns={'Adj Close':ticker}, inplace=True)
        #df.drop(['Open','High','Low','Close','Volume'],1,inplace=True)

        
        
            month=files[4:6]
            year=files[6:10]
            
            if 'ECS' not in df.sheet_names:
                arr=[[year,month,c[0]/2+e[0]/2,c[1]/2+e[1]/2],]
                df_v1=pd.DataFrame(arr,columns=['Year','Month','Volume','Value'])
            if 'RTGS' not in df.sheet_names:
                arr=[[year,month,a[2]+e[0]/2,a[1]+e[1]/2],]
                df_v1=pd.DataFrame(arr,columns=['Year','Month','Volume','Value'])
            if 'NEFT' not in df.sheet_names:
                arr=[[year,month,a[2]+c[0]/2,a[1]+c[1]/2],]
                df_v1=pd.DataFrame(arr,columns=['Year','Month','Volume','Value'])
            else:
                arr=[[year,month,a[2]+c[0]/2+e[0]/2,a[1]+c[1]/2+e[1]/2],]
                df_v1=pd.DataFrame(arr,columns=['Year','Month','Volume','Value'])
            #arr=[month,year]
            #tuples = list(zip(*arr))

        if main_df.empty:
            main_df = df_v1
        else:
            main_df = main_df.append(df_v1)

        if count % 10 == 0:
            print(count)
    print(main_df.head())
    main_df.to_csv('RBI_joined_data.csv')


compile_data()

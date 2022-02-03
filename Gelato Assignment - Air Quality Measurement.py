
# coding: utf-8

# # Air Quality - Gelato Assignment

# This notebook demonstrates how to ingest transform and load  Air Quality data  to oracle database 
# what about the dataset ?? 
# the Air Quality System (AQS) which contains data from approximately 4,000 monitoring stations around the country,
# mainly in urban areas. Data from the AQS is considered the "gold standard" for determining outdoor air pollution

# ### Import Packages We Need

# In[8]:


import json
import urllib.request as req
import pandas as pd
import numpy as np
# import db
# import log class


# # Extracting The data

# In[3]:


try:
    df = req.urlopen("https://data.cdc.gov/api/views/cjae-szjv/rows.json?accessType=DOWNLOAD")  
except Exception as e:
    print('An error occurred while attempting to retrieve data from the API.')
    #### build log to ES
    #loger.log("ERRPR :: ",e)  
    sys.exit()

 

df = json.load(df)
df_data = df['data']
df_meta = df['meta']  

#### build log to ES 
#loger.log("INFO :: ",e)  
    

## getting metadata-columns

schema = [key['name']  for key in df_meta['view']['columns'] ]
schema_filter = [key['name']  for key in df_meta['view']['columns'] if key['dataTypeName'] == 'meta_data' ]

## create pandas dataframe

### df_ is  main df
extracted_df = pd.DataFrame(df_data,columns =schema)

## filtering out metadata-columns

df_ = extracted_df.drop(schema_filter,axis='columns')


# # stablish a connection with RDBMS

# In[18]:


#dsn = cx_Oracle.makedsn('localhost','1521','XE')
#conn=cx_Oracle.connect(user= r'HR',password=r'xmaster' ,dsn=dsn)
#cur = conn.cursor()
cur =  get_db() ## for security


#################### build log to ES ################
#################   loger.log("INFO :: ") ################  


# # basic data cleaning 

# In[5]:


# creating column list for insertion
cols = "`,`".join([str(i) for i in df_.columns.tolist()])
cols=cols.replace("Value","score")


# df_['CountyName'] = df_['CountyName'].str.replace(r"'","")
def CountryReplacer(text):
    if str(text)=="Prince George's":
        return "Prince George"
    
    elif str(text)=="O'Brien":
        return "O Brien"
        
    else :
        return text

df_['CountyName']=df_['CountyName'].apply(CountryReplacer)
    
df_=df_.dropna(axis='index',how='any')


### Data Quality check #####

# df_ = df_.loc[(df["MeasureId"] >  0 ) & ((df["CountyFips"] >  0 )) & ((df["ReportYear"] >  1900 )) & ((df["Value"] >  0 )) ]

#################### build log to ES ################
#################   loger.log("INFO :: ") ################  


# # ETL measure dimention

# In[6]:


measure_df = df_[["MeasureId","MeasureName","MeasureType","StratificationLevel"]]
uniq_measure_df = measure_df.drop_duplicates()


# In[26]:


#assuing that we wanna add new with no updates --> if make id unique
# Insert DataFrame recrds one by one.
for i,row in uniq_measure_df.iterrows():
    sql = "INSERT INTO AIR_QUALITY_MEASURES (MeasureId,MeasureName,MeasureType,StratificationLevel)  VALUES {}".format(tuple(row))
    cur.execute(sql)
    
    # the connection is not autocommitted by default, so we must commit to save our changes
    conn.commit()
    

#################### build log to ES ################
#################   loger.log("INFO :: ") ################      


# # ETL UNIT dimention

# In[49]:


unit_df=df_[["Unit","UnitName"]].drop_duplicates()


# In[50]:


unit_df


# In[53]:


#assuing that we wanna add new with no updates --> if make id unique
# Insert DataFrame recrds one by one.
for i,row in unit_df.iterrows():
    sql = "INSERT INTO AIR_QUALITY_UNITS (Unit,UnitName)  VALUES {}".format(tuple(row))
    cur.execute(sql)
    
    # the connection is not autocommitted by default, so we must commit to save our changes
    conn.commit()
    

#################### build log to ES ################
#################   loger.log("INFO :: ") ################      


# # ETL LOCATION dimention

# In[75]:


location_df = df_[["StateFips","StateName","CountyFips","CountyName"]].drop_duplicates()


# In[76]:


location_df


# In[81]:


#assuing that we wanna add new with no updates --> if make id unique
# Insert DataFrame recrds one by one.
for i,row in location_df.iterrows():
    sql = "INSERT INTO AIR_QUALITY_LOCATION (StateFips,StateName,CountyFips,CountyName)  VALUES {}".format(tuple(row))
    cur.execute(sql)
    
    # the connection is not autocommitted by default, so we must commit to save our changes
    conn.commit()

    

#################### build log to ES ################
#################   loger.log("INFO :: ") ################      


# # ETL Date dimention

# In[110]:


date_df =  df_["ReportYear"].drop_duplicates()
date_df = dict(date_df)


# In[123]:


#assuing that we wanna add new with no updates --> if make id unique
# Insert DataFrame recrds one by one.
for v in date_df.values():
    sql = "INSERT INTO AIR_QUALITY_DATE (YEAR)  VALUES ({})".format(v)
    cur.execute(sql)
    
    # the connection is not autocommitted by default, so we must commit to save our changes
    conn.commit()
    

#################### build log to ES ################
#################   loger.log("INFO :: ") ################      


# # ETL AIR_QUALITY_Score Fact 

# In[122]:


##### loading dimentions 
## Get  measure Key 
cur.execute("select measurekey,measureid from HR.AIR_QUALITY_MEASURES")
measure_lst = cur.fetchall()
measure_dim=pd.DataFrame(measure_lst,columns = ["measurekey","MeasureId"])


# In[127]:


##### loading dimentions 
## Get  unit Key 
cur.execute("select UNITKEY,UNITNAME from HR.AIR_QUALITY_UNITS")
unit_lst = cur.fetchall()
unit_dim=pd.DataFrame(unit_lst,columns = ['unitkey','UnitName'])


# In[128]:


##### loading dimentions 
## Get  location Key 
cur.execute("select COUNTYKEY,CountyFips from HR.AIR_QUALITY_LOCATION")
location_lst = cur.fetchall()
location_dim=pd.DataFrame(location_lst,columns = ["countykey","CountyFips"])


# In[130]:


##### loading dimentions 
## Get DATE Key 
cur.execute("select dateKey,year from HR.AIR_QUALITY_DATE")
date_lst = cur.fetchall()
date_dim=pd.DataFrame(date_lst,columns = ["dateKey","ReportYear"])


# In[131]:


##Join  fact with measure-dim
fact_df = df_[['MeasureId','CountyFips','ReportYear','Value', 'UnitName', 'DataOrigin', 'MonitorOnly']]
fact_df = fact_df.astype({'MeasureId': 'int64','CountyFips':'int64','ReportYear':'int64','Value':'float64','MonitorOnly':'int64'})
fact_df = pd.merge(fact_df,measure_dim,on="MeasureId",how=("inner")).drop('MeasureId', 1)


# In[132]:


##Join  fact with unit_dim
fact_df = pd.merge(fact_df,unit_dim,on="UnitName",how=("inner")).drop('UnitName', 1)


# In[133]:


##Join  fact with location_dim
fact_df = pd.merge(fact_df,location_dim,on="CountyFips",how=("inner")).drop('CountyFips', 1)


# In[134]:


##Join  fact with date_dim
fact_df = pd.merge(fact_df,date_dim,on="ReportYear",how=("inner")).drop('ReportYear', 1)


# In[136]:


fact_df.columns


# In[ ]:


#assuing that we wanna add new with no updates --> if make id unique
# Insert DataFrame recrds one by one.
for i,row in fact_df.iterrows():
    sql = "INSERT INTO AIR_QUALITY_SCORE (Score,DataOrigin,MonitorOnly,measurekey,unitkey,countykey,dateKey)  VALUES {}".format(tuple(row))
    cur.execute(sql)
    
    # the connection is not autocommitted by default, so we must commit to save our changes
    conn.commit()
    
    

#################### build log to ES ################
#################   loger.log("INFO :: ") ################      


# # close connection

# In[ ]:


close_db()


#################### build log to ES ################
#################   loger.log("INFO :: ") ################  


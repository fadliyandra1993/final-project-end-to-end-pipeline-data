#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
from os import name
import pandas as pd
import numpy as np

import requests
from sqlalchemy import create_engine
from psycopg2 import connect
from psycopg2.extras import execute_values


# In[2]:

# Connection MySQL
username_mysql = 'root'
password_mysql = 'fadli123456'
host_mysql = '172.18.0.1'
port_mysql = 3306
database_mysql = 'final_project'

engine_mysql = create_engine('mysql+mysqlconnector://{}:{}@{}:{}/{}'.format(username_mysql, password_mysql, host_mysql, port_mysql, database_mysql))
engine_conn_mysql = engine_mysql.connect()

# Connection PostgreSQL
username_postgre = 'root'
password_postgre = 'fadli123456'
host_postgre = '172.18.0.1'
port_postgre = 5440
database_postgre = 'final_project_covid19'

engine_postgre = create_engine('postgresql://{}:{}@{}:{}/{}'.format(username_postgre, password_postgre, host_postgre, port_postgre, database_postgre))
conn_engine_postgre = engine_postgre.connect()

# In[5]:

def insert_data_to_mysql() :
    response = requests.get('https://covid19-public.digitalservice.id/api/v1/rekapitulasi_v2/jabar/harian?level=kab')
    data = response.json()
    df = pd.DataFrame(data['data']['content']) 

    #engine, engine_conn = connection_mysql()
    df.to_sql(name='data_covid19', con=engine_mysql, if_exists="replace", index=False)
    engine_mysql.dispose()

def insert_dim_province(data):

    data = data.filter(["kode_prov", "nama_prov"])
    data = data.drop_duplicates(["kode_prov", "nama_prov"])

    return data

def insert_dim_district(data):
    data = data.filter(["kode_kab", "kode_prov", "nama_kab"])
    data = data.drop_duplicates(["kode_kab", "kode_prov", "nama_kab"])

    return data

def insert_dim_case(data):
    column_start = ["suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded",
                     "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", 
                     "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_end = ["id", "status_name", "status_detail", "status"]

    data = data[column_start]
    data = data[:1]
    data = data.melt(var_name="status", value_name="total")
    data = data.drop_duplicates("status").sort_values("status")
    
    data['id'] = np.arange(1, data.shape[0]+1)
    data[['status_name', 'status_detail']] = data['status'].str.split('_', n=1, expand=True)
    data = data[column_end]

    return data

def insert_fact_province_daily(data, dim_case):

    column_start = ["tanggal", "kode_prov", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_end = ['date', 'province_id', 'status', 'total']

    # AGGREGATE
    data = data[column_start]
    data = data.melt(id_vars=["tanggal", "kode_prov"], var_name="status", value_name="total").sort_values(["tanggal", "kode_prov", "status"])
    data = data.groupby(by=['tanggal', 'kode_prov', 'status']).sum()
    data = data.reset_index()

    # REFORMAT
    data.columns = column_end
    data['id'] = np.arange(1, data.shape[0]+1)

    # MERGE
    dim_case = dim_case.rename({'id': 'case_id'}, axis=1)
    data = pd.merge(data, dim_case, how='inner', on='status')
    
    data = data[['id', 'province_id', 'case_id', 'date', 'total']]
    
    return data

def insert_fact_province_monthly(data, dim_case):
    column_start = ["tanggal", "kode_prov", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_end = ['month', 'province_id', 'status', 'total']

    # AGGREGATE
    data = data[column_start]
    data['tanggal'] = data['tanggal'].apply(lambda x: x[:7])
    data = data.melt(id_vars=["tanggal", "kode_prov"], var_name="status", value_name="total").sort_values(["tanggal", "kode_prov", "status"])
    data = data.groupby(by=['tanggal', 'kode_prov', 'status']).sum()
    data = data.reset_index()

    # REFORMAT
    data.columns = column_end
    data['id'] = np.arange(1, data.shape[0]+1)

    # MERGE
    dim_case = dim_case.rename({'id': 'case_id'}, axis=1)
    data = pd.merge(data, dim_case, how='inner', on='status')

    data = data[['id', 'province_id', 'case_id', 'month', 'total']]
    
    return data


def insert_fact_province_yearly(data, dim_case):
    column_start = ["tanggal", "kode_prov", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_end = ['year', 'province_id', 'status', 'total']

    # AGGREGATE
    data = data[column_start]
    data['tanggal'] = data['tanggal'].apply(lambda x: x[:4])
    data = data.melt(id_vars=["tanggal", "kode_prov"], var_name="status", value_name="total").sort_values(["tanggal", "kode_prov", "status"])
    data = data.groupby(by=['tanggal', 'kode_prov', 'status']).sum()
    data = data.reset_index()

    # REFORMAT
    data.columns = column_end
    data['id'] = np.arange(1, data.shape[0]+1)

    # MERGE
    dim_case = dim_case.rename({'id': 'case_id'}, axis=1)
    data = pd.merge(data, dim_case, how='inner', on='status')

    data = data[['id', 'province_id', 'case_id', 'year', 'total']]
    
    return data

def insert_fact_district_monthly(data, dim_case):
    column_start = ["tanggal", "kode_kab", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_end = ['month', 'district_id', 'status', 'total']

    # AGGREGATE
    data = data[column_start]
    data['tanggal'] = data['tanggal'].apply(lambda x: x[:7])
    data = data.melt(id_vars=["tanggal", "kode_kab"], var_name="status", value_name="total").sort_values(["tanggal", "kode_kab", "status"])
    data = data.groupby(by=['tanggal', 'kode_kab', 'status']).sum()
    data = data.reset_index()

    # REFORMAT
    data.columns = column_end
    data['id'] = np.arange(1, data.shape[0]+1)

    # MERGE
    dim_case = dim_case.rename({'id': 'case_id'}, axis=1)
    data = pd.merge(data, dim_case, how='inner', on='status')

    data = data[['id', 'district_id', 'case_id', 'month', 'total']]
    
    return data


def insert_fact_district_yearly(data, dim_case):
    column_start = ["tanggal", "kode_kab", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_end = ['year', 'district_id', 'status', 'total']

    # AGGREGATE
    data = data[column_start]
    data['tanggal'] = data['tanggal'].apply(lambda x: x[:4])
    data = data.melt(id_vars=["tanggal", "kode_kab"], var_name="status", value_name="total").sort_values(["tanggal", "kode_kab", "status"])
    data = data.groupby(by=['tanggal', 'kode_kab', 'status']).sum()
    data = data.reset_index()

    # REFORMAT
    data.columns = column_end
    data['id'] = np.arange(1, data.shape[0]+1)
    
    # MERGE
    dim_case = dim_case.rename({'id': 'case_id'}, axis=1)
    data = pd.merge(data, dim_case, how='inner', on='status')

    data = data[['id', 'district_id', 'case_id', 'year', 'total']]
    
    return data

def insert_data_to_postgre():
    data = pd.read_sql(sql='data_covid19', con=engine_mysql)
    engine_mysql.dispose()

    # filter needed column
    dim_province = insert_dim_province(data)
    dim_district = insert_dim_district(data)
    dim_case = insert_dim_case(data)

    dim_province.to_sql('dim_province', con=engine_postgre, index=False, if_exists='replace')
    dim_district.to_sql('dim_district', con=engine_postgre, index=False, if_exists='replace')
    dim_case.to_sql('dim_case_table', con=engine_postgre, index=False, if_exists='replace')

    fact_province_daily = insert_fact_province_daily(data, dim_case)
    fact_province_monthly = insert_fact_province_monthly(data, dim_case)
    fact_province_yearly = insert_fact_province_yearly(data, dim_case)
    fact_district_monthly = insert_fact_district_monthly(data, dim_case)
    fact_district_yearly = insert_fact_district_yearly(data, dim_case)

    fact_province_daily.to_sql('fact_province_daily', con=engine_postgre, index=False, if_exists='replace')
    fact_province_monthly.to_sql('fact_province_monthly', con=engine_postgre, index=False, if_exists='replace')
    fact_province_yearly.to_sql('fact_province_yearly', con=engine_postgre, index=False, if_exists='replace')
    fact_district_monthly.to_sql('fact_district_monthly', con=engine_postgre, index=False, if_exists='replace')
    fact_district_yearly.to_sql('fact_district_yearly', con=engine_postgre, index=False, if_exists='replace')

    engine_mysql.dispose()


    
# In[ ]:





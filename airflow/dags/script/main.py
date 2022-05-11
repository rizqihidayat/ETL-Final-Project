import json
from os import name
import pandas as pd
import numpy as np

import requests
from sqlalchemy import create_engine
from psycopg2 import connect
from psycopg2.extras import execute_values


def connection_mysql():
    with open ('dags/script/credentials.json', "r") as cred:
        credential = json.load(cred)
        credential = credential['mysql_lake']

    username = credential['username']
    password = credential['password']
    host = credential['host']
    port = credential['port']
    database = credential['database']

    engine = create_engine('mysql+mysqlconnector://{}:{}@{}:{}/{}'.format(username, password, host, port, database))
    engine_conn = engine.connect()
    print("Connect Engine MySQL")
    return engine, engine_conn


def connection_postgresql(conn_type):
    with open ('dags/script/credentials.json', "r") as cred:
        credential = json.load(cred)
        credential = credential['postgres_warehouse']

    username = credential['username']
    password = credential['password']
    host = credential['host']
    port = credential['port']
    database = credential['database']
    
    if conn_type == 'engine':
        engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(username, password, host, port, database))
        conn_engine = engine.connect()
        print("Connect Engine Postgresql")
        return engine, conn_engine
    else:
        conn = connect(
            user=username,
            password=password,
            host=host,
            port=port,
            database=database
            )
        cursor = conn.cursor()
        print("Connect Cursor Postgresql")
        return conn, cursor


def insert_raw_to_mysql():
    response = requests.get('https://covid19-public.digitalservice.id/api/v1/rekapitulasi_v2/jabar/harian?level=kab')
    data = response.json()
    df = pd.DataFrame(data['data']['content']) 
    
    engine, engine_conn = connection_mysql()
    df.to_sql(name='rekapitulasi_kasus_harian', con=engine, if_exists="replace", index=False)
    engine.dispose()


def insert_dim_province(data):
    column_start = ["kode_prov", "nama_prov"]
    column_end = ["province_id", "province_name"]

    data = data[column_start]
    data = data.drop_duplicates(column_start)
    data.columns = column_end

    return data


def insert_dim_district(data):
    column_start = ["kode_kab", "kode_prov", "nama_kab"]
    column_end = ["district_id", "province_id", "province_name"]

    data = data[column_start]
    data = data.drop_duplicates(column_start)
    data.columns = column_end

    return data


def insert_dim_case(data):
    column_start = ["suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
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


def insert_raw_to_warehouse():
    engine, engine_conn = connection_mysql()
    data = pd.read_sql(sql='rekapitulasi_kasus_harian', con=engine)
    engine.dispose()

    # filter needed column
    column = ["tanggal", "kode_prov", "nama_prov", "kode_kab", "nama_kab", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    data = data[column]

    dim_province = insert_dim_province(data)
    dim_district = insert_dim_district(data)
    dim_case = insert_dim_case(data)

    fact_province_daily = insert_fact_province_daily(data, dim_case)
    fact_province_monthly = insert_fact_province_monthly(data, dim_case)
    fact_province_yearly = insert_fact_province_yearly(data, dim_case)
    fact_district_monthly = insert_fact_district_monthly(data, dim_case)
    fact_district_yearly = insert_fact_district_yearly(data, dim_case)

    engine, conn_engine = connection_postgresql(conn_type='engine')

    dim_province.to_sql('dim_province', con=engine, index=False, if_exists='replace')
    dim_district.to_sql('dim_district', con=engine, index=False, if_exists='replace')
    dim_case.to_sql('dim_case', con=engine, index=False, if_exists='replace')

    fact_province_daily.to_sql('fact_province_daily', con=engine, index=False, if_exists='replace')
    fact_province_monthly.to_sql('fact_province_monthly', con=engine, index=False, if_exists='replace')
    fact_province_yearly.to_sql('fact_province_yearly', con=engine, index=False, if_exists='replace')
    fact_district_monthly.to_sql('fact_district_monthly', con=engine, index=False, if_exists='replace')
    fact_district_yearly.to_sql('fact_district_yearly', con=engine, index=False, if_exists='replace')

    engine.dispose()
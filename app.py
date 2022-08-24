#!/usr/bin/python3

import os
import json
import sqlparse

import pandas as pd
import numpy as np

import connection_source
import connection_warehouse

if __name__ == '__main__':
    print(f"[INFO] Service ETL is Starting .....")
    conn_dwh, engine_dwh  = connection_warehouse.conn()
    cursor_dwh = conn_dwh.cursor()

    conf = connection_source.config('postgresql')
    conn, engine = connection_source.psql_conn(conf)
    cursor = conn.cursor()

    path_query = os.getcwd()+'/query/'
    
    #menambahkan tabel dim_orders dan mengisi datanya
    query = sqlparse.format(
        open(
            path_query+'query.sql','r'
            ).read(), strip_comments=True).strip()

    query_dwh = sqlparse.format(
        open(
            path_query+'dwh_design.sql','r'
            ).read(), strip_comments=True).strip()

    #membuat tabel dim_users 

    adddimuser = sqlparse.format(
        open(
            path_query+'query_dim_users.sql','r'
            ).read(), strip_comments=True).strip()

    #menambahkan data ke tabel dim_users 
    addDatadim_users = sqlparse.format(
        open(
            path_query+'dim_users.sql','r'
            ).read(), strip_comments=True).strip()

    #membuat tabel fact_orders
    addfactorders = sqlparse.format(
        open(
            path_query+'query_fact_orders.sql','r'
            ).read(), strip_comments=True).strip()

    #menambahkan data ke tabel fact_orders 
    addDatafact_orders = sqlparse.format(
        open(
            path_query+'fact_orders.sql','r'
            ).read(), strip_comments=True).strip()

    try:
        print(f"[INFO] Service ETL is Running .....")
        df = pd.read_sql(query, engine)
        dfdim_users = pd.read_sql(adddimuser, engine)
        dffact_orders = pd.read_sql(addfactorders, engine)
        
        cursor_dwh.execute(query_dwh)
        cursor_dwh.execute(addDatadim_users)
        cursor_dwh.execute(addDatafact_orders)
        conn_dwh.commit()

        dfdim_users.to_sql('dim_users', engine_dwh, if_exists='append', index=False)
        print(f"[INFO] Service ETL dim_users is success .....")

        dffact_orders.to_sql('fact_orders', engine_dwh, if_exists='append', index=False)
        print(f"[INFO] Service ETL fact_orders is Success .....")   

        df.to_sql('dim_orders', engine_dwh, if_exists='append', index=False)
        print(f"[INFO] Service ETL dim_orders is Success .....")

    except:
        print(f"[INFO] Service ETL is Failed .....")




    

    
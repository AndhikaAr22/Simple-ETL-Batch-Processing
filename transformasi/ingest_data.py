import pandas as pd
from connection.postgre_connection import param_config, connect_postgre
from model.query import insert_table_accountscountry, insert_table_accountdata, insert_table_accountsseries
from transformasi.transform import cleansing_account_country, cleansing_account_data, cleansing_account_series

conf_postgres_data_lake = param_config("conn_postgres_datalake")
conf_postgres_data_warehouse = param_config("conn_postgres_dwh")
conn, engine = connect_postgre(conf_postgres_data_warehouse)

def insert_data_warehouse_country():
    # conn, engine = connect_postgre(conf_postgres_data_warehouse)
    curr = conn.cursor()
    df_account_country = cleansing_account_country(conf_postgres_data_lake)
    df_account_country.dropna(inplace=True)

    query = insert_table_accountscountry()

    for i, row in df_account_country.iterrows():
        curr.execute(query, list(row))
    conn.commit()
    return print("insert data success")    


def insert_data_warehouse_accounts_data():
    # conn,engine = connect_postgre(conf_postgres_data_warehouse)
    curr = conn.cursor()
    df_account_data = cleansing_account_data(conf_postgres_data_lake)
    df_account_data.dropna(inplace=True)

    query = insert_table_accountdata()

    for i, row in df_account_data.iterrows():
        print(list(row))
        curr.execute(query, list(row))
    conn.commit()

    return print("insert data success")    


def insert_data_warehouse_accounts_series():
    # conn, engine = connect_postgre(conf_postgres_data_warehouse)
    curr = conn.cursor()
    df_account_series = cleansing_account_series(conf_postgres_data_lake)
    df_account_series.dropna(inplace=True)

    query = insert_table_accountsseries()

    for i, row in df_account_series.iterrows():
        print(list(row))
        curr.execute(query, list(row))
    conn.commit()
    return print("insert data success")    

import pandas as pd
from connection.postgre_connection import param_config, postgreconnection
from database.insert_data import insert_data_tabel1, insert_data_tabel2, insert_data_tabel3
from model.create_table import create_table_dwh
from model.query import insert_table_accountscountry, insert_table_accountdata, insert_table_accountsseries
from transformasi.transform import cleansing_account_country, cleansing_account_data, cleansing_account_series

conf_postgres_data_lake = param_config("conn_postgres_datalake")
conf_postgres_data_warehouse = param_config("conn_postgres_dwh")

# create table dwh
def create_table_data_warehouse(conf_postgres_data_warehouse):
    conn = postgreconnection(conf_postgres_data_warehouse)
    curr = conn.cursor()
    query = create_table_dwh()
    curr.execute(query)
    conn.commit()

    conn.close()
    curr.close()
    return print("table dwh created")


# insert data to dwh
def insert_data_warehouse_country():
    conn = postgreconnection(conf_postgres_data_warehouse)
    curr = conn.cursor()
    df_account_country = cleansing_account_country(conf_postgres_data_lake)
    df_account_country.dropna(inplace=True)

    query = insert_table_accountscountry()

    for i, row in df_account_country.iterrows():
        curr.execute(query, list(row))
    conn.commit()
    return print("insert data success")    


def insert_data_warehouse_accounts_data():
    conn = postgreconnection(conf_postgres_data_warehouse)
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
    conn = postgreconnection(conf_postgres_data_warehouse)
    curr = conn.cursor()
    df_account_series = cleansing_account_series(conf_postgres_data_lake)
    df_account_series.dropna(inplace=True)

    query = insert_table_accountsseries()

    for i, row in df_account_series.iterrows():
        print(list(row))
        curr.execute(query, list(row))
    conn.commit()
    return print("insert data success")    




if __name__ == '__main__':

    # insert data to data lake
    insert_data_tabel1(conf_postgres_data_lake)
    insert_data_tabel2(conf_postgres_data_lake)
    insert_data_tabel3(conf_postgres_data_lake)

    # created table dwh
    create_table_data_warehouse(conf_postgres_data_warehouse)

    # insert data to dwh
    insert_data_warehouse_country()
    insert_data_warehouse_accounts_data()
    insert_data_warehouse_accounts_series()


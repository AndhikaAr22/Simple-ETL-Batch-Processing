import pandas as pd
import csv
from connection.postgre_connection import param_config, connect_postgre


path_accounts_data = '/home/andhika/Simple-ETL/data2/Wealth-AccountData.csv'
path_accounts_country = '/home/andhika/Simple-ETL/data2/Wealth-AccountsCountry.csv'
path_accounts_series = '/home/andhika/Simple-ETL/data2/Wealth-AccountSeries.csv'

conf_postgres_data_lake = param_config("conn_postgres_datalake")
conn, engine = connect_postgre(conf_postgres_data_lake)




def insert_data_tabel1():
    df_account_data = pd.read_csv(path_accounts_country, sep=',')
    df_account_data.to_sql("anccounts_country", engine)
    return print("insert data successfully")

def insert_data_tabel2():
    df_account_data = pd.read_csv(path_accounts_data, sep=',')
    df_account_data.to_sql("anccounts_data", engine)
    return print("insert data 2 successfully")

def insert_data_tabel3():
    df_account_data = pd.read_csv(path_accounts_series, sep=',')
    df_account_data.to_sql("anccounts_series", engine)
    return print("insert data 3 successfully")



        
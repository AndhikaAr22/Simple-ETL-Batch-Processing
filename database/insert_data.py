from connection.postgre_connection import param_config, connect_postgre
import pandas as pd

## TODO : coba refactor codenya dikit biar lebih rapih lagi. 

conf_postgres_data_lake = param_config("conn_postgres_datalake")

def insert_data_tabel1(conf_postgres_data_lake):
    df_account_data = pd.read_csv('/home/andhika/Simple-ETL/data2/Wealth-AccountData.csv', sep=',')
    conn, engine = connect_postgre(conf_postgres_data_lake) ## ini best practice, cuma masih banyak cara lagi untuk masukin data ke posrgre yg lebih simple dan ga perlu buat postgres connection banyak2, cukup pake 1 function aja udh cukup sbnrnya
    df_account_data.to_sql("anccounts_data", engine)
    return print("insert data successfully")

def insert_data_tabel2(conf_postgres_data_lake):
    df_account_data = pd.read_csv('/home/andhika/Simple-ETL/data2/Wealth-AccountsCountry.csv', sep=',')
    conn, engine = connect_postgre(conf_postgres_data_lake) ## ini best practice, cuma masih banyak cara lagi untuk masukin data ke posrgre yg lebih simple dan ga perlu buat postgres connection banyak2, cukup pake 1 function aja udh cukup sbnrnya
    df_account_data.to_sql("anccounts_country", engine)
    return print("insert data 2 successfully")

def insert_data_tabel3(conf_postgres_data_lake):
    df_account_data = pd.read_csv('/home/andhika/Simple-ETL/data2/Wealth-AccountSeries.csv', sep=',') ## penulsan path coba pake relative path stylenya jgn hardcode didalam script
    conn, engine = connect_postgre(conf_postgres_data_lake) ## ini best practice, cuma masih banyak cara lagi untuk masukin data ke posrgre yg lebih simple dan ga perlu buat postgres connection banyak2, cukup pake 1 function aja udh cukup sbnrnya
    df_account_data.to_sql("anccounts_series", engine)
    return print("insert data 3 successfully")

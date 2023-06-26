import pandas as pd
from connection.postgre_connection import param_config, postgreconnection

conf_postgres_data_lake = param_config("conn_postgres_datalake")

# account_country
def cleansing_account_country(conf_postgres_data_lake):
    conn = postgreconnection(conf_postgres_data_lake)
    curr = conn.cursor()
    curr.execute("SELECT * FROM anccounts_country") ## betted query ini dibuat as function aja di folder model.query.
    data_accountscountry = curr.fetchall()
    df = pd.DataFrame(data_accountscountry, columns=[col[0] for col in curr.description])
    account_country = df[['Code', 'Short Name', 'Table Name', 'Long Name', 'Currency Unit']]
    return account_country.drop_duplicates(subset=['Code'])

    
def cleansing_account_data(conf_postgres_data_lake):
    conn = postgreconnection(conf_postgres_data_lake)
    curr = conn.cursor()
    curr.execute("SELECT * FROM anccounts_data") ## betted query ini dibuat as function aja di folder model.query.
    data_accountsdata = curr.fetchall()
    df = pd.DataFrame(data_accountsdata, columns=[col[0] for col in curr.description])
    account_data = df[['Country Name', 'Country Code', 'Series Name', 'Series Code', '1995 [YR1995]', '2000 [YR2000]', '2005 [YR2005]', '2010 [YR2010]', '2014 [YR2014]']]
    return account_data.drop_duplicates()


def cleansing_account_series(conf_postgres_data_lake):
    conn = postgreconnection(conf_postgres_data_lake)
    curr = conn.cursor()
    curr.execute("SELECT * FROM anccounts_series")
    data_accountseries = curr.fetchall()
    df = pd.DataFrame(data_accountseries, columns=[col[0] for col in curr.description])
    account_series = df[['Code', 'Topic', 'Indicator Name']]
    return account_series.drop_duplicates()


## TODO : -> buat function dimana isinya list dari ketiga query diatas, lalu fucntion yg udh dibuat dipanggil di script ini aja. tinggal cari tau gimana caranya untuk manggil query2 nya ke masing2 function transformasi yg udh lu buat.

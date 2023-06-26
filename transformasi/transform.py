import pandas as pd
from connection.postgre_connection import param_config, postgreconnection




conf_postgres_data_lake = param_config("conn_postgres_datalake")

# account_country
def cleansing_account_country(conf_postgres_data_lake):
    conn = postgreconnection(conf_postgres_data_lake)
    curr = conn.cursor()
    curr.execute("SELECT * FROM anccounts_country")
    data_accountscountry = curr.fetchall()
    df = pd.DataFrame(data_accountscountry, columns=[col[0] for col in curr.description])
    account_country = df[['Code', 'Short Name', 'Table Name', 'Long Name', 'Currency Unit']]
    account_country = account_country.drop_duplicates(subset=['Code'])
    print(account_country.head())
    return account_country

    
def cleansing_account_data(conf_postgres_data_lake):
    conn = postgreconnection(conf_postgres_data_lake)
    curr = conn.cursor()
    curr.execute("SELECT * FROM anccounts_data")
    data_accountsdata = curr.fetchall()
    df = pd.DataFrame(data_accountsdata, columns=[col[0] for col in curr.description])
    account_data = df[['Country Name', 'Country Code', 'Series Name', 'Series Code', '1995 [YR1995]', '2000 [YR2000]', '2005 [YR2005]', '2010 [YR2010]', '2014 [YR2014]']]
    account_data = account_data.drop_duplicates()
    print(account_data.head())
    return account_data


def cleansing_account_series(conf_postgres_data_lake):
    conn = postgreconnection(conf_postgres_data_lake)
    curr = conn.cursor()
    curr.execute("SELECT * FROM anccounts_series")
    data_accountseries = curr.fetchall()
    df = pd.DataFrame(data_accountseries, columns=[col[0] for col in curr.description])
    account_series = df[['Code', 'Topic', 'Indicator Name']]
    account_series = account_series.drop_duplicates()
    print(account_series.head())
    return account_series



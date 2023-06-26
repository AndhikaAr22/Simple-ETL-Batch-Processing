import psycopg2
from connection.postgre_connection import param_config, con_postgres, postgreconnection
from model.create_database import create_database_lake, create_database_dwh

def create_database():
    # conn = psycopg2.connect(database="postgres", user="postgres", password="admin", host="localhost", port="5432")
    # curr = conn.cursor()
    
    conf_postgres = param_config("postgresql")
    conn = con_postgres(conf_postgres)
    curr = conn.cursor()
    conn.set_isolation_level(0)
    curr.execute("COMMIT")

    query = create_database_lake()
    curr.execute(query)

    curr.close()
    conn.close()

    return print("New Database Created")
    
def create_database_dwh():
    conf_postgres = param_config("conn_postgres_datalake")
    # conn = postgreconnection(conf_postgres) ## -> kenapa pake 2 funtion yang fungsinya sama? cukup memakai 1 fungis saja tinggal bedain aja cara pemanggilan config di database dwhnya
    conn = con_postgres(conf_postgres) ## -> better kaya gini
    curr = conn.cursor()
    conn.set_isolation_level(0)
    curr.execute("COMMIT")

    query = create_database_dwh()
    curr.execute(query)

    curr.close()
    conn.close()
    
    return print("New Database Created")



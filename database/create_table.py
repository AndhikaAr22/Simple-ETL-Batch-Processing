from connection.postgre_connection import param_config , con_postgres
from model.create_table import create_table_dwh


conf_postgres_data_warehouse = param_config("conn_postgres_dwh")



def create_table_datawarehouse(conf_postgres_data_warehouse):
    # conn = postgreconnection(conf_postgres_data_warehouse) ## cukup pake 1 function aja. tinggal bedain di config filenya dan cara pemanggilan masih2 confignya.
    conn = con_postgres(conf_postgres_data_warehouse)
    curr = conn.cursor()
    query = create_table_dwh()
    curr.execute(query)
    conn.commit()

    conn.close()
    curr.close()
    return print("table account scountry created")
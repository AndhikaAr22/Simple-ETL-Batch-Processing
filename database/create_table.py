from connection.postgre_connection import param_config , postgreconnection
from model.create_table import create_table_dwh


conf_postgres_data_warehouse = param_config("conn_postgres_dwh")



def create_table_datawarehouse(conf_postgres_data_warehouse):
    conn = postgreconnection(conf_postgres_data_warehouse)
    curr = conn.cursor()
    query = create_table_dwh()
    curr.execute(query)
    conn.commit()

    conn.close()
    curr.close()
    return print("table account scountry created")
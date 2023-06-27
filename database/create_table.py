from connection.postgre_connection import param_config , connect_postgre
from model.create_table import create_table_dwh


conf_postgres_data_warehouse = param_config("conn_postgres_dwh")
conn, engine = connect_postgre(conf_postgres_data_warehouse)


def create_table_datawarehouse():
    curr = conn.cursor()
    query = create_table_dwh()
    curr.execute(query)
    conn.commit()

    conn.close()
    curr.close()
    return print("table dwh created")
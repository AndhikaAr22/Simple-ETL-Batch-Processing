from database.insert_data import insert_data_tabel1, insert_data_tabel2, insert_data_tabel3
from database.create_table import create_table_datawarehouse
from transformasi.ingest_data import insert_data_warehouse_country, insert_data_warehouse_accounts_data, insert_data_warehouse_accounts_series



if __name__ == '__main__':

    # insert data to data lake
    insert_data_tabel1()
    # insert_data_tabel2()
    # insert_data_tabel3()

    # created table dwh
    # create_table_datawarehouse()

    # # insert data to dwh
    # insert_data_warehouse_country()
    # insert_data_warehouse_accounts_data()
    # insert_data_warehouse_accounts_series()


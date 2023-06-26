
# table accountscountry
def insert_table_accountscountry():
    return """INSERT INTO accountscountry(
    country_code,
    short_name,
    table_name ,
    long_name ,
    current_unit)
    VALUES (%s, %s, %s, %s, %s)"""


# table accountseries
def insert_table_accountsseries():
    return """INSERT INTO accountseries(
    series_code,
    topic,
    indicator_name 
    
    )
    VALUES (%s, %s, %s)"""



# table accountsdata
def insert_table_accountdata():
    return """INSERT INTO accountsdata(
             country_name,
             country_code,
             indicator_name,
             indicator_code,
             year_1995,
             year_2000,
             year_2005,
             year_2010,
             year_2014 
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
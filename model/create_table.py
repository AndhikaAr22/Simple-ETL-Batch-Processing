def create_table_dwh():
    query = f"""
    CREATE TABLE IF NOT EXISTS accountsdata(
            country_name VARCHAR,
            country_code VARCHAR,
            indicator_name VARCHAR,
            indicator_code VARCHAR,
            year_1995 numeric,
            year_2000 numeric,
            year_2005 numeric,
            year_2010 numeric,
            year_2014 numeric
                );

    CREATE TABLE IF NOT EXISTS accountscountry (
            country_code VARCHAR PRIMARY KEY,
            short_name VARCHAR,
            table_name VARCHAR,
            long_name VARCHAR,
            current_unit VARCHAR
                );
    
    CREATE TABLE IF NOT EXISTS accountseries(
                series_code VARCHAR,
                topic VARCHAR,
                indicator_name VARCHAR
                );
    
                 """
    return query
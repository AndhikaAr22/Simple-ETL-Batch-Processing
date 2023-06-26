import psycopg2
import json
from sqlalchemy import create_engine


def param_config(config):
    with open("config.json", "rb") as file:
        conf = json.load(file)

    try:
        return conf[config]
    except:
        return("check config")

def con_postgres(conf):
    while True:
        try:
            conn = psycopg2.connect(
                host= conf["host"],
                user= conf["user"],
                password= conf["password"],
                port=conf["port"]
            )
            break
        except:
            raise Exception("Cant Connect To Postgres DB")
    return conn

def connect_postgre(conf): ## -> sebenarnya fucntion ini gausah ada lebih bagus karna udh ada functuon con_postgres yg fungsinya itu sama aja seperti functuin ini.
    try:
        conn = psycopg2.connect(
            host= conf["host"],
            user= conf["user"],
            password= conf["password"],
            database= conf["database"],
            port=conf["port"]
        )
        print("success connect postgresql")
        engine = create_engine(f"postgresql+psycopg2://{conf['user']}:{conf['password']}@{conf['host']}:{conf['port']}/{conf['database']}")
        return conn, engine

    except Exception as e:
        print("can't connect postgresql")
        print(e)




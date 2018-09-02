import os
import logging
import yaml

import pandas as pd

encoding = "utf8"

root = os.getcwd()
cred_path = os.path.join(root, "credentials.yml")
with open(cred_path, "r") as f:
    cred = yaml.load(f)

PSSWD = cred["MYSQL_PSSWD"]
USER  = cred["MYSQL_USER"]
HOST  = cred["HOST"]

logging_path = os.path.join(os.getcwd(), "logging", "text_log.log")

logging.basicConfig(level=logging.INFO,
    format="%(levelname)s - %(asctime)s - %(msg)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(logging_path),
        logging.StreamHandler(),
    ])

def connect():
    """ Create MySQL engine """
    import sqlalchemy
    engine = sqlalchemy.create_engine(
        "mysql+pymysql://%s:%s@%s/openbravopos?charset=%s" %
        (USER, PSSWD, HOST, encoding))
    try:
        return engine.raw_connection()
    except sqlalchemy.exc.OperationalError as e :
        logging.critical("Wrong credentials! "
            "Check credentials in `credentials.yml`.")
        quit()

def read_sql(query):
    """ Read query from MySQL """
    cnx = connect()
    data = pd.read_sql(query, cnx)
    cnx.close()
    return data

def fetch_table(table_name, columns=["*", ]):
    """ Select `columns` from table in MySQL """
    cols = ", ".join(columns)
    query = "SELECT %s FROM %s;" % (cols, table_name)
    data = read_sql(query)
    logging.info("Number of items in %s: %d" % (table_name, data.shape[0]))
    return data

def insert(data, table_name):
    """ Insert data into table """
    cnx = connect()
    insert_query = """INSERT INTO {}
        (id
         , reference
         , code
         , name
         , pricebuy
         , pricesell
         , category
         , taxcat
         , iscom
         , isscale)

    VALUES (
        %(ID)s, %(REFERENCE)s, %(CODE)s, %(NAME)s,
        %(PRICEBUY)s, %(PRICESELL)s, %(CATEGORY)s,
        %(TAXCAT)s, %(ISCOM)s, %(ISSCALE)s
    ) ON DUPLICATE KEY UPDATE
        pricebuy = VALUES(pricebuy)
        , pricesell = VALUES(pricesell)
    ;
    """.format(table_name)
    cursor = cnx.cursor()

    rows = list()
    for i, row in data.iterrows():
        rows.append(row.to_dict())

    cursor.executemany(insert_query, rows)
    cursor.close()
    cnx.commit()
    cnx.close()
    logging.info("Insertion finished.")

if __name__ == "__main__":
    pass

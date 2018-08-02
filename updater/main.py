import os
import os.path as op
import sys
import logging

import pandas as pd

def TAXCAT(product_group):
    """ Calculate `TAXCAT` from `product_group` """
    tax_groups = (923, 941, 946, 957, 959, 975, 985, 986, )
    return "%03d" % int(product_group not in tax_groups)

def PRICESELL(data):
    """ Calculate `PRICESELL` from `rrp` & `TAXCAT` """
    tmp = data["TAXCAT"].apply(lambda x: 1.19 if x == "000" else 1.07)
    return data["rrp"] / tmp

def generate_hash(x): # TODO `ID`
    """ Generate `ID` value """
    from uuid import uuid4
    return str(uuid4())

def initial_category(x): # TODO added by simon
    """ Fill `CATEGORY` """
    return "000"

def fill_1b(x): # TODO `ISCOM`, `ISSCALE`
    """ Fill `ISCOM` & `ISSCALE` """
    return b"\x01"

def calculate_fields(data):
    """ Calculate necessary fields:
    1) `TAXCAT`    from `product_group`
    2) `PRICESELL` from `rrp` & `TAXCAT`
    3) Fill `CATEGORY` with `000`, `ID` with generated hashes,
       `ISCOM` & `ISSCALE` with `1b`
       """
    data["TAXCAT"]    = data["product_group"].apply(TAXCAT)
    data["temp"]      = data["TAXCAT"]\
        .apply(lambda x: 1.19 if x == "000" else 1.07)
    data["PRICESELL"] = (data["rrp"] / data["temp"]).round(2)
    data["CATEGORY"]  = data.apply(initial_category, axis=1)
    data["ID"]        = data.apply(generate_hash, axis=1)
    for col in ["ISCOM", "ISSCALE"]:
        data[col] = data.apply(fill_1b, axis=1)
    return data

def select_file(folder, rows=8):
    """ Select menu for input directory """
    files = [file for file in os.listdir(folder) if ".csv" in file]
    page  = 0
    while True:
        for i, name in zip(range(rows), files[page * rows:(page + 1) * rows]):
            print(i, name)
        try:
            choice = int(input(
                "Select file. (8 for prev page, 9 for next page)\n"))
        except ValueError as e:
            continue
        if choice == 9 and len(files):
            page += 1
        elif choice == 8 and page > 0:
            page -= 1
        elif choice in list(range(rows)):
            try:
                return files[page * 8 + choice]
            except IndexError as e:
                continue

def main():
    from updater.inserter import log_data, prepare_to_insert,\
                                 decide_before_insert
    from updater.consistency_checker import read_update, clean_input
    from updater.db_connect import fetch_table, insert

    root = os.getcwd()
    logging_folder = op.join(root, "logging")

    filename = select_file(op.join(root, "input"))
    update_path = op.join(root, "input", filename)

    # Dictionary of all used file_paths
    paths = {
        # Input
        "update": update_path,

        # Logging files
        "consistency": op.join(logging_folder, "constistency_fail.csv"),
        "skip_rows":   op.join(logging_folder, "skipped_rows.csv"),
        "insert_rows": op.join(logging_folder, "inserted_rows.csv"),
        "update_rows": op.join(logging_folder, "updated_rows.csv"),
    }

    # Read `update.csv`
    update = read_update(paths["update"])

    # Check consistency
    update = clean_input(update, paths["consistency"])

    # Rename ready columns
    rename_columns = {
        "id":     "REFERENCE",
        "ean":    "CODE",
        "p_NAME": "NAME",
        "price":  "PRICEBUY",
    }
    update.rename(columns=rename_columns, inplace=True)
    logging.info("Number of rows after cleaning: %d" % update.shape[0])

    # Fetch `products` from database
    columns = list(rename_columns.values()) + ["PRICESELL", ]
    products = fetch_table("products", columns)
    products["REFERENCE"] = products["REFERENCE"].astype(int)
    products["CODE"]      = products["CODE"].astype(float)
    products["PRICESELL"] = products["PRICESELL"].round(2)

    # Add neccessary columns
    update = calculate_fields(update)

    # Decide action for each update item
    update = decide_before_insert(update, products)

    # Log decisions
    for decision in ("skip", "insert", "update", ):
        data = update[update["decision"] == decision].drop("decision", axis=1)
        logging.info("Number of %6s rows: %d" % (decision, data.shape[0]))
        log_data(data, paths["%s_rows" % decision], decision)

    # Leave only insert & update data
    update = update[update["decision"] != "skip"].drop("decision", axis=1)
    # Format columns & insert data
    update = prepare_to_insert(update)
    insert(update, "products")
    logging.info("----------------------------------------------------------")

if __name__ == "__main__":
    main()

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

def initial_category(x):
    """ Fill `CATEGORY` """
    return "000"

def fill_0b(x): # TODO `ISCOM`, `ISSCALE`
    """ Fill `ISCOM` & `ISSCALE` """
    return b"\x00"

def calculate_fields(data):
    """ Calculate necessary fields:
    1) `TAXCAT`    from `product_group`
    2) `PRICESELL` from `rrp` & `TAXCAT`
    3) Fill `CATEGORY` with `000`, `ID` with generated hashes,
       `ISCOM` & `ISSCALE` with `1b`
       """
    data["TAXCAT"]    = data["product_group"].apply(TAXCAT)
    data["temp"]      = data["TAXCAT"]\
        .apply(lambda x: 1.19 if x == "001" else 1.07)
    data["PRICESELL"] = (data["rrp"] / data["temp"]).round(13)
    data["CATEGORY"]  = data.apply(initial_category, axis=1)
    data["ID"]        = data.apply(generate_hash, axis=1)
    for col in ["ISCOM", "ISSCALE"]:
        data[col] = data.apply(fill_0b, axis=1)
    return data

def select_file(folder, rows=8):
    """ Select menu for input directory """
    files = [file for file in os.listdir(folder) if ".csv" in file]
    page  = 0
    while True:
        for i, name in zip(range(rows), files[page * rows:(page + 1) * rows]):
            print("%d)" % i, name)
        try:
            choice = int(input(
                "Select file. (8 for prev page, 9 for next page): "))
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
    from updater.inserter \
        import prepare_to_insert, decide_before_insert, touch_folder
    from updater.consistency_checker import read_update, clean_input
    from updater.db_connect import fetch_table, insert

    root = os.getcwd()

    touch_folder(op.join(os.getcwd(), "logging"))
    logging.basicConfig(level=logging.INFO,
        format="%(levelname)s - %(asctime)s - %(msg)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(op.join(root, "logging", "text_log.log")),
            logging.StreamHandler(),
        ])

    filename = select_file(op.join(root, "input"))
    update_path = op.join(root, "input", filename)

    # Dictionary of all used file_paths
    logging_folder = "%s_log" % op.splitext(filename)[0]
    logging_folder = op.join("logging", logging_folder)
    logging.info("Logging folder: {}".format(logging_folder))
    touch_folder(op.join(os.getcwd(), logging_folder))
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
        "id":         "REFERENCE",
        "ean":        "CODE",
        "p_NAME":     "NAME",
        "pricepunit": "PRICEBUY",
    }
    update.rename(columns=rename_columns, inplace=True)
    logging.info("Rows after cleaning count: %d" % update.shape[0])

    # Fetch `products` from database
    columns = list(rename_columns.values()) + ["PRICESELL", ]
    products = fetch_table("products", columns)
    products["REFERENCE"] = products["REFERENCE"].astype(int)
    products["CODE"]      = products["CODE"].astype(float)
    products["PRICESELL"] = products["PRICESELL"].round(13)

    # Add neccessary columns
    update = calculate_fields(update)

    # Decide action for each update item
    update = decide_before_insert(update, products, paths)

    # Leave only insert & update data
    update = update[update["decision"] != "skip"].drop("decision", axis=1)
    # Format columns & insert data
    update = prepare_to_insert(update)
    insert(update, "products")

if __name__ == "__main__":
    main()

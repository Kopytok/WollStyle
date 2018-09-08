import os
import logging

import pandas as pd

def touch_folder(path):
    """ Create folder if it does not exist """
    if os.path.exists(path):
        return
    os.mkdir(path)

def log_data(data, path, title="checked"):
    """ Save dataframe into `.csv` file if it has rows """
    import csv

    if data.shape[0] > 0:
        data.to_csv(path, index=False, encoding="cp1252", sep=";",
                    quoting=csv.QUOTE_NONNUMERIC)
        p = path.replace(os.getcwd(), "")
        logging.info("Saved %s rows to %s" % (title, p))
    else:
        return

def duplicate_ind(update, products, col):
    """ Check if `col` value in `update` already exists in `product` """
    flag_col = "%s_dup" % col
    update = update\
        .merge(products[[col]], on=col, how="left", indicator=True)\
        .rename(columns={"_merge": flag_col})
    # True if duplicated (already exists in products):
    update[flag_col] = (update[flag_col] != "left_only")
    return update

def price_ind(col, update, products):
    """
    1) Merge `update` rows with both `REFERENCE` and `CODE`
        matching.
    2) Check if the value is `same`, `lower`, `higher`
    3) Return `update` with `price_ind` & `price_prod` columns
    """

    ind_columns = ["REFERENCE_dup", "CODE_dup"]
    ix = update[ind_columns].all(1)

    header_columns = ["REFERENCE", "CODE"]
    columns = header_columns + [col]
    update = update.merge(products[columns], on=header_columns,
                          how="left", suffixes=["", "_prod"])

    rules = {
        "same":   "==",
        "lower":  "<",
        "higher": ">",
    }
    for rule, op in rules.items():
        ix = eval("update[col]%supdate[col+'_prod']" % op)
        update.loc[ix, col + "_ind"] = rule

    return update

def decide_before_insert(update, products):
    """
    1) Create in `update` indicators for `REFERENCE`,
       `NAME` & `CODE` if the row already exists in `products`
    2) Adds "#" before `NAME` if that `NAME` already exists with
       different `REFERENCE` & `CODE`
    3) Decision:
     - skip if one of `REFERENCE` AND `CODE` matches
        and the other does not
     - insert if neither `REFERENCE` nor `CODE` match
     - update if both `REFERENCE` and `CODE` match
     4) Return `update` with `decision` column and aux columns
     """

    # Create flags for `REFERENCE`, `CODE, & `NAME`
    for col in ("REFERENCE", "NAME", "CODE", ):
        update = duplicate_ind(update, products, col)

    # Rename `NAME` if it exists with different `CODE` & `REFERENCE`
    ix = update["NAME_dup"] & \
         ~update[["REFERENCE_dup", "CODE_dup"]].any(1)

    update["NAME"] = update["NAME"].where(~ix, "#" + update["NAME"])

    # Compare `PRICEBUY` % `PRICESELL` where `REFERENCE` and `CODE` match
    for col in ("PRICEBUY", "PRICESELL", ):
        update = price_ind(col, update, products)

    price_cols = ["PRICEBUY_ind", "PRICESELL_ind"]
    # Decision for each row
    check_columns = ["REFERENCE_dup", "CODE_dup", ]
    decisions = {
        "skip":   (update["REFERENCE_dup"] != update["CODE_dup"]) | # bad
                  (update[price_cols] == "same").all(1),    # already exists
        "insert": ~update[check_columns].any(1),            # x = 0, y = 0
        "update":  update[check_columns].all(1) &           # x = 1, y = 1
                  (update[price_cols] != "same").any(1), # changed existing
    }

    for decision, ix in decisions.items():
        update.loc[ix, "decision"] = decision

    return update

def prepare_to_insert(data):
    """
    1) Select only neccessary columns
    2) Replace `NaN` with None
    3) Returns only necessary columns
    """

    columns = ["REFERENCE", "CODE", "NAME", "PRICEBUY",
               "ID", "TAXCAT", "PRICESELL", "CATEGORY",
               "ISCOM", "ISSCALE"]
    extra_columns = ["product_group", "rrp", ]
    tmp = data[columns + extra_columns].copy()

    # Replace NaN with None
    tmp = tmp.where(pd.notnull(data), None)

    # Leave only columns that will be inserted
    tmp.drop(extra_columns, axis=1, inplace=True)

    logging.info("Number of rows to be inserted: %d" % tmp.shape[0])
    return tmp

if __name__ == "__main__":
    pass

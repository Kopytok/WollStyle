import logging
import os

import pandas as pd

from .inserter import touch_folder, log_data

logging_path = os.path.join(os.getcwd(), "logging", "text_log.log")

logging.basicConfig(level=logging.INFO,
    format="%(levelname)s - %(asctime)s - %(msg)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(logging_path),
        logging.StreamHandler(),
    ])

def read_update(path):
    """ Reads update dataframe from `path` """
    usecols = ["id", "ean", "product_group", "colorn", "name",
               "pricepunit", "rrp"]
    try:
        update = pd.read_csv(path, sep=";", encoding="cp1252", usecols=usecols)
    except FileNotFoundError as e:
        logging.critical("No such file in `input` folder!")
        quit()
    logging.info("Number of input rows: %d" % update.shape[0])
    return update

def consistency_check(row):
    """ True if bad (not equal), False otherwise or if not wool """
    if (900  <= row["product_group"] <=  999 or
            9000 <= row["product_group"] <= 9999):
        return False
    else:
        return 10000 * row["product_group"] + row["colorn"] != row["id"]

def name_column(row):
    """ Converts `name` and `colorn` into `p_NAME` """
    return "%s %04d" % (row["name"], row["colorn"])

def clean_input(data, save_filtered=None):
    """
        1) Creates `p_NAME`
        2) Checks consistency
        3) Checks if `ean` is missing
        4) Checks if any of `ean`, `id`, `p_NAME` has duplicates
        5) Saves failed rows to log file
        6) Outputs good rows, properly ordered.

    `save_filtered` is path of logging file """

    columns = data.columns.tolist()
    data["p_NAME"]      = data.apply(name_column,       axis=1)
    data["consistency"] = data.apply(consistency_check, axis=1)
    # True if bad (null):
    data["ean_null"]    = data["ean"].isnull()
    for col in ("ean", "id", "p_NAME", ):
        # True if bad (duplicated):
        data["%s_dup" % col] = data[col].duplicated(keep=False)

    check_columns = ["id_dup", "ean_dup", "p_NAME_dup",
                     "ean_null", "consistency"]
    data["fail"] = data[check_columns].any(1)

    if save_filtered:
        touch_folder(os.path.dirname(save_filtered))
        log_data(data[data["fail"]], save_filtered, "consistency fail")

    return data.loc[~data["fail"], columns + ["p_NAME"]]\
        .reset_index(drop=True)

if __name__ == "__main__":
    pass

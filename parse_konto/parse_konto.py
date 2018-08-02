import datetime
import os
import os.path as op
import re
import csv
import pandas as pd

ENCODING = "cp1252"
OUT_COLUMNS = ["Buchungstag", "SollHabenKNZ", "DATEV_Buchungstext",
               "BU", "Gegenkonto", "Konto", "Umsatz", "Verwendungszweck_fill",
               "Verwendungszweck", "Beleg1"]

ROOT = op.abspath(op.join(__file__, op.pardir))
try:
    patterns_path = op.join(ROOT, "patterns.xlsx")
    PATTERNS = pd.read_excel(patterns_path, encoding=ENCODING, sep=";")
except FileNotFoundError as e:
    print("Place `patterns.xlsx` into `parse konto` directory")
    quit()

PATTERNS.fillna("", inplace=True)

def select_file(folder, rows=8):
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

def touch_folder(path):
    """ Creates folder if it does not exist """
    os.mkdir(path) if not op.exists(path) else None

def join_columns(arr):
    not_missing = [it for it in arr if it == it]
    joined = " ".join(not_missing).replace("\"", "")
    if len(joined) >= 160:
        print("Shortened string: %s" % joined)
    return joined[:160]

def umsatz_handle(data):
    knz_handle = lambda x: "H" if x < 0 else "S"

    soll_haben = ["Soll", "Haben"]
    data[soll_haben] = data[soll_haben].astype(float)
    data["Umsatz"] = data[soll_haben].sum(axis=1)
    data["SollHabenKNZ"] = data["Umsatz"].apply(knz_handle)
    data["Umsatz"] = data["Umsatz"].abs().apply(lambda x: "%.2f" % x)
    return data

def format_decimals(x):
    return x.replace(".", "").replace(",", ".") if x == x else 0

def read_kontoumsaetze(path):
    data = pd.read_csv(path, sep=";", encoding=ENCODING, skiprows=4)
    data = data[data["Buchungstag"] != "Kontostand"]

    # Format dates
    for col in "Buchungstag", "Wert":
        data[col] = pd.to_datetime(data[col], format="%d.%m.%Y")

    data["Buchungstag"] = data["Buchungstag"].dt.strftime("%d%m%Y")

    # Format numeric
    for col in "Soll", "Haben":
        data[col] = data[col].apply(format_decimals)

    rename_columns = {
        "Begünstigter / Auftraggeber": "Auftraggeber",
        "Währung":                     "Waehrung"
    }
    data.rename(columns=rename_columns, inplace=True)
    return data

def do_regex(text):
    global PATTERNS

    for _, item in PATTERNS.iterrows():
        if re.search(item["Regex"], text):
            out_dict = {
                "Konto":              item["Konto"],
                "DATEV_Buchungstext": text,
                "Regex":              item["Regex"]
            }

            if len(item["Gegenkonto"]) > 4:
                split = item["Gegenkonto"].split("-")
                out_dict["BU"], out_dict["Gegenkonto"] = split
            else:
                out_dict["BU"] = ""
                out_dict["Gegenkonto"] = item["Gegenkonto"]

            if "Electronic Cash Einreichung" in text:
                try:
                    referenz_nr = text.split("TERMINAL")[1]
                    date = referenz_nr[10:16]
                    date_beleg1 = datetime.datetime.strptime(date, "%y%m%d")
                except IndexError as e:
                    referenz_nr = "6809307420151029182210"
                    date_beleg1 = datetime.date(2015, 10, 29)
                    print("Date fake!!!")

                out_dict["Beleg1"] = date_beleg1.strftime("%Y%m%d")
                buchungstext = "EC Umsätze vom {} - Referenz-Nr. = {}".format(
                    date_beleg1.strftime("%d.%m.%Y"),
                    referenz_nr)
                out_dict["DATEV_Buchungstext"] = buchungstext

            if item["IF regex 1"]:
                pattern = item["IF regex 1"]
                repl = item["IF substitute 1"]
                string = out_dict["DATEV_Buchungstext"]

                out_dict["DATEV_Buchungstext"] = re.sub(pattern, repl, string)
                if item["IF regex 2"]:
                    pattern = item["IF regex 2"]
                    repl = ""
                    string = out_dict["DATEV_Buchungstext"]

                    out_dict["DATEV_Buchungstext"] = re.sub(
                        pattern, repl, string)

            if len(out_dict["DATEV_Buchungstext"]) >= 60:
                tmp_text = out_dict["DATEV_Buchungstext"]
                out_dict["DATEV_Buchungstext"] = tmp_text[:60]
            return pd.Series(out_dict)
    return None

def shorten_verwendungszweck(x):
    if len(x) >= 210:
        print("Following string was shortened to 210 symbols: %s" % x[:23])
    return x[:210]

def main():
    # Read file from `input` directory
    filename = select_file(op.join(ROOT, "input"))
    path = op.join(ROOT, "input", filename)
    data = read_kontoumsaetze(path)

    # Manipulations
    data = umsatz_handle(data)
    that_word = "Verwendungszweck"
    data[that_word] = data.iloc[:, 2:8].apply(join_columns, axis=1)

    out_cols = ["BU", "Beleg1", "DATEV_Buchungstext", "Gegenkonto",
                "Konto", "Regex"]
    data[out_cols] = data[that_word].apply(do_regex)[out_cols]

    data[that_word + "_fill"] = that_word
    data[that_word] = data[that_word].apply(shorten_verwendungszweck)

    # Save output into `output` directory
    touch_folder(op.join(ROOT, "output"))
    out_path = op.join(ROOT, "output", filename)
    data[OUT_COLUMNS].to_csv(out_path, sep=";", index=False,
                             header=False, encoding=ENCODING,
                             quoting=csv.QUOTE_ALL,
                             line_terminator=";\n")
    return data

if __name__ == "__main__":
    data = main()
    print("\nOutput shape: %d x %d" % data.shape)

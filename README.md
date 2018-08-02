# Wollstyle
Files parsing and database interaction.

------
The project consists of 2 tasks:
 1. **parse_konto** - transform input table into specified format
 1. **updater**     - check input data consistency, prepare it for the insertion, update `products` table in MySQL database with logging

------

### 1. Konto parser usage:

 1. In terminal move into `parse_konto` directory.
 2. Add input file into `input` directory (e.g. `test.csv`).
 3. Place`patterns.xslx` into `parse_konto` directory.
The structure becomes as follows:

```
- input/test.csv
- parse_konto.py
- patterns.xlsx
- ...
```

 4. In terminal run `python3 parse_konto.py`.
 5. Select input file from menu.
 6. Output appears in `output` folder with the same filename. (The `output` directory is created if it haven't existed).

------

## 2. Updater usage:

 1. In terminal move into `updater` directory.
 2. Edit credentials in `credentials.yml`. Make sure not to share that file for security reasons.
 3. Add update data into `input` directory (e.g. `test.csv`).
The structure becomes as follows:

```
- input/test.csv
- lib/
- credentials.yml
- main.py
- ...
```

 4. Run `python3 main.py`.
 5. Select input file from menu.
 6. Output appears in `output` folder with the same filename. (The `output` directory is created if it haven't existed)

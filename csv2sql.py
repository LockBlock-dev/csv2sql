import sqlite3, pandas, pathlib, time, os, sys

TYPES = {
    "str": "TEXT",
    "int": "INTEGER",
    "float": "REAL",
    "bytes": "BLOB",
    "bool": "BOOL",
    "NoneType": "NULL"
}
DB_PATH = pathlib.Path("db.sq3")

def showTable(list, columns = 1):
    max = 0
    for i in list:
            if max < (len(i[0]) or len(i)) :
                max = (len(i[0]) or len(i))
    if columns == 1:
        return "- " + "\n- ".join(list)
    elif columns == 2:
        return "- " + "\n- ".join([f"{(max - len(i[0]) + 1) * ' '}=> ".join(i) for i in list])

if DB_PATH.exists():
    answer = input("INFO: Database file already exists. Do you want to delete it? (y/n): ")
    if answer == "y":
        os.remove(DB_PATH)

db = sqlite3.connect(DB_PATH)
cursor = db.cursor()
available = []

for f in os.listdir():
    if f.endswith(".csv"):
        available.append(f)

if (len(available) == 0):
    print("ERR: No CSV files found")
    sys.exit()

print(f"INFO: Available CSV files:\n{showTable(available, columns = 1)}")
file = input("Enter the name of the desired CSV file (including .csv): ")

csv_data = pandas.read_csv(file)
sample = csv_data.values[0]
file = file.replace(".csv", "")
content_types = {}
prevent_duplicated = False

try:
    cursor.execute(f"CREATE TABLE {file} (id INTEGER PRIMARY KEY)")
except:
    answer = input("ERR: La table existe déjà. Souhaitez-vous la supprimer? (y/n): ")
    if answer == "y":
        cursor.execute(f"DROP TABLE {file}")
        cursor.execute(f"CREATE TABLE {file} (id INTEGER PRIMARY KEY)")
    else:
        prevent_duplicated = True

#start benchmark
start = time.time()

if not prevent_duplicated:
    for c, v in zip(csv_data.columns, sample):
        cursor.execute(f"ALTER TABLE {file} ADD COLUMN {c} {TYPES[type(v).__name__]}")
        content_types[c] = type(v).__name__

    print(f"INFO: Auto generated columns:\n{showTable(content_types.items(), columns = 2)}")

for row in csv_data.values:
    new_row = []
    for e in row:
        e = str(e).replace("'", " ")
        if e == "nan":
            e = "NULL"
        new_row.append(f"'{e}'")
    cursor.execute(f'INSERT INTO {file}("id", {", ".join(csv_data.columns)}) VALUES(NULL, {", ".join(new_row)})')

db.commit()
db.close()

#end benchmark
end = time.time()

print(f"INFO: Script took {round(end - start, 2)}s")

import pymongo
import pandas as pd
import requests
import csv
import os

# TODO - datum as datetime?
# TODO - region names instead of codes?
# TODO - aggregate by age?


def download_csv(url, filename):
    # check if infected already downloaded
    if not os.path.exists(filename):
        # download infected csv
        response = requests.get(url)
        with open(filename, 'w') as f:
            writer = csv.writer(f)
            for line in response.iter_lines():
                writer.writerow(line.decode('utf-8').split(','))


def insert_df_to_mongo(db, col_name, df):
    # drop collection if already exists?
    if col_name in db.list_collection_names():
        db[col_name].drop()
    mycol = db[col_name]
    mycol.insert_many(df.to_dict('records'))


DATA_FOLDER = "data/"
CSV_FILES = {
    "infected":
        {"url": "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/osoby.csv",
         "filename": DATA_FOLDER + "infected.csv"},
    "cured":
        {"url": "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/vyleceni.csv",
         "filename": DATA_FOLDER + "cured.csv"},
    "dead":
        {"url": "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/umrti.csv",
         "filename": DATA_FOLDER + "dead.csv"},
    "hospitalized":
        {"url": "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/hospitalizace.csv",
         "filename": DATA_FOLDER + "hospitalized.csv"},
}

# MongoDB connection
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["upa-covid"]

# Download CSV files into ./data/ folder
# create folder if it doesn't exist
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)

download_csv(CSV_FILES["infected"]["url"], CSV_FILES["infected"]["filename"])  # infected
download_csv(CSV_FILES["cured"]["url"], CSV_FILES["cured"]["filename"])  # cured
download_csv(CSV_FILES["dead"]["url"], CSV_FILES["dead"]["filename"])  # dead
download_csv(CSV_FILES["hospitalized"]["url"], CSV_FILES["hospitalized"]["filename"])  # hospitalized

# Manually deal with each collection
# Infected CSV
df_infected = pd.read_csv(CSV_FILES["infected"]["filename"])
df_infected = df_infected.drop(["nakaza_v_zahranici", "nakaza_zeme_csu_kod"], axis=1)  # remove unwanted columns
print(df_infected.head())
insert_df_to_mongo(mydb, "infected", df_infected)  # insert into NoSQL db

# Cured CSV
df_cured = pd.read_csv(CSV_FILES["cured"]["filename"])
print(df_cured.head())
insert_df_to_mongo(mydb, "cured", df_cured)  # insert into NoSQL db

# Dead CSV
df_dead = pd.read_csv(CSV_FILES["dead"]["filename"])
print(df_dead.head())
insert_df_to_mongo(mydb, "dead", df_dead)  # insert into NoSQL db

# Hospitalized CSV
df_hospitalized = pd.read_csv(CSV_FILES["hospitalized"]["filename"])
print(df_hospitalized.head())
insert_df_to_mongo(mydb, "hospitalized", df_hospitalized)  # insert into NoSQL db

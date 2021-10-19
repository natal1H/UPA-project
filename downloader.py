import pymongo
import pandas as pd
import requests
import csv
import os

DATA_FOLDER = "data/"
INFECTED_CSV_LINK = "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/osoby.csv"
INFECTED_CSV_FILE = DATA_FOLDER + "osoby.csv"

# MongoDB connection
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["upa-covid"]

# Download CSV files into ./data/ folder
# create folder if it doesn't exist
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)

# check if infected already downloaded
if not os.path.exists(INFECTED_CSV_FILE):
    # download infected csv
    response = requests.get(INFECTED_CSV_LINK)
    with open(DATA_FOLDER + 'osoby.csv', 'w') as f:
        writer = csv.writer(f)
        for line in response.iter_lines():
            writer.writerow(line.decode('utf-8').split(','))

# Infected CSV
df_infected = pd.read_csv(INFECTED_CSV_FILE)
df_infected = df_infected.drop(["nakaza_v_zahranici", "nakaza_zeme_csu_kod"], axis=1)  # remove unwanted columns
print(df_infected.head())
df_infected = df_infected.head(5)  # TODO remove

# insert into NoSQL db
# drop collection if already exists?
if "infected" in mydb.list_collection_names():
    mydb["infected"].drop()
mycol = mydb["infected"]
mycol.insert_many(df_infected.to_dict('records'))

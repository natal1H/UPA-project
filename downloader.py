import pymongo
import pandas as pd
import requests
import csv
import os


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
    "tests":
        {"url": "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/testy-pcr-antigenni.csv",
         "filename": DATA_FOLDER + "tests.csv"},
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
download_csv(CSV_FILES["tests"]["url"], CSV_FILES["tests"]["filename"])  # tests

# Manually deal with each collection
# Infected CSV
df_infected = pd.read_csv(CSV_FILES["infected"]["filename"], usecols=lambda c: c in {'datum', 'vek', 'pohlavi', 'kraj_nuts_kod', 'okres_lau_kod'}, sep=",")
df_infected.rename(columns={'datum': 'date', 'vek': 'age', 'pohlavi': 'gender', 'kraj_nuts_kod': 'region', 'okres_lau_kod': 'district'}, inplace=True)

# rename gender names (M/Z) to full name (male/female)
gender_dict = {"M": "male", "Z": "female"}
df_infected = df_infected.replace({"gender": gender_dict})

# convert age to int
df_infected['age'] = pd.to_numeric(df_infected['age'], errors='coerce')
df_infected = df_infected.dropna(subset=['age'])
df_infected['age'] = df_infected['age'].astype('int')

insert_df_to_mongo(mydb, "infected", df_infected)  # insert into NoSQL db

# Cured CSV
df_cured = pd.read_csv(CSV_FILES["cured"]["filename"], usecols=lambda c: c in {'datum', 'vek', 'pohlavi', 'kraj_nuts_kod', 'okres_lau_kod'}, sep=",")
df_cured.rename(columns={'datum': 'date', 'vek': 'age', 'pohlavi': 'gender', 'kraj_nuts_kod': 'region', 'okres_lau_kod': 'district'}, inplace=True)

# rename gender names (M/Z) to full name (male/female)
df_cured = df_cured.replace({"gender": gender_dict})

# convert age to int
df_cured['age'] = pd.to_numeric(df_cured['age'], errors='coerce')
df_cured = df_cured.dropna(subset=['age'])
df_cured['age'] = df_cured['age'].astype('int')

insert_df_to_mongo(mydb, "cured", df_cured)  # insert into NoSQL db

# Dead CSV
df_dead = pd.read_csv(CSV_FILES["dead"]["filename"], usecols=lambda c: c in {'datum', 'vek', 'pohlavi', 'kraj_nuts_kod', 'okres_lau_kod'}, sep=",")
df_dead.rename(columns={'datum': 'date', 'vek': 'age', 'pohlavi': 'gender', 'kraj_nuts_kod': 'region', 'okres_lau_kod': 'district'}, inplace=True)

# rename gender names (M/Z) to full name (male/female)
df_dead = df_dead.replace({"gender": gender_dict})

# convert age to int
df_dead['age'] = pd.to_numeric(df_dead['age'], errors='coerce')
df_dead = df_dead.dropna(subset=['age'])
df_dead['age'] = df_dead['age'].astype('int')

insert_df_to_mongo(mydb, "dead", df_dead)  # insert into NoSQL db

# Hospitalized CSV
df_hospitalized = pd.read_csv(CSV_FILES["hospitalized"]["filename"], usecols=lambda c: c in {'datum', 'pacient_prvni_zaznam'}, sep=",")
df_hospitalized.rename(columns={'datum': 'date', 'pacient_prvni_zaznam': 'patients'}, inplace=True)
df_hospitalized['date'] = pd.to_datetime(df_hospitalized['date'])
df_hospitalized['patients'] = pd.to_numeric(df_hospitalized['patients'], errors='coerce')
df_hospitalized = df_hospitalized.dropna(subset=['patients'])
df_hospitalized['patients'] = df_hospitalized['patients'].astype('int')
df_hospitalized['month'] = df_hospitalized["date"].dt.to_period("M")
grouped_hospitalized = df_hospitalized.groupby(['month'])['patients'].sum()
grouped_hospitalized = grouped_hospitalized.reset_index()

# convert Period month to Object (can't store Period to mongo)
grouped_hospitalized['month'] = grouped_hospitalized['month'].astype(str)
grouped_hospitalized['month'] = pd.to_datetime(grouped_hospitalized['month']).apply(lambda x: x.strftime('%Y-%m'))
insert_df_to_mongo(mydb, "hospitalized", grouped_hospitalized)  # insert into NoSQL db

# Tests CSV
df_tests = pd.read_csv(CSV_FILES["tests"]["filename"], usecols=lambda c: c in {'datum', 'pocet_PCR_testy', 'pocet_AG_testy'}, sep=",")
df_tests.rename(columns={'datum': 'date', 'pocet_PCR_testy': 'tests_PCR', 'pocet_AG_testy': 'tests_antigen'}, inplace=True)
df_tests['date'] = pd.to_datetime(df_tests['date'])

# convert tests PCR to int
df_tests['tests_PCR'] = pd.to_numeric(df_tests['tests_PCR'], errors='coerce')
df_tests = df_tests.dropna(subset=['tests_PCR'])
df_tests['tests_PCR'] = df_tests['tests_PCR'].astype('int')

# convert tests antigen to int
df_tests['tests_antigen'] = pd.to_numeric(df_tests['tests_antigen'], errors='coerce')
df_tests = df_tests.dropna(subset=['tests_antigen'])
df_tests['tests_antigen'] = df_tests['tests_antigen'].astype('int')

df_tests['month'] = df_tests["date"].dt.to_period("M")
grouped_tests = df_tests.groupby(['month']).agg({'tests_PCR': 'sum', 'tests_antigen': 'sum'})
grouped_tests = grouped_tests.reset_index()
grouped_tests['tests'] = grouped_tests['tests_PCR'] + grouped_tests['tests_antigen']
grouped_tests = grouped_tests.drop(["tests_PCR", "tests_antigen"], axis=1)  # drop PCR and antigen tests

# convert Period month to Object (can't store Period to mongo)
grouped_tests['month'] = grouped_tests['month'].astype(str)
grouped_tests['month'] = pd.to_datetime(grouped_tests['month']).apply(lambda x: x.strftime('%Y-%m'))

insert_df_to_mongo(mydb, "tests", grouped_tests)  # insert into NoSQL db

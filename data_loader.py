import pymongo
import pandas as pd
import requests
import csv
import os
from argparse import ArgumentParser

"""UPA - 1st part
    Theme: Covid-19
    Authors: 
        - Filip Bali (xbalif00)
        - Natália Holková (xholko02)
        - Roland Žitný (xzitny01)
"""

parser = ArgumentParser(prog='UPA-data_loader')
# parser.add_argument('-m', '--mongo', help="Mongo db location", default="mongodb://localhost:27017/")

# TODO VB DB
parser.add_argument('-m', '--mongo', help="Mongo db location",
                    default="mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000")
parser.add_argument('-f', '--folder', help="Folder for downloading csv files", default="data/")
parser.add_argument('-d', '--database', help="Database name", default="UPA-db")

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
    "vaccinated_regions":
        {"url": "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/ockovani.csv",
         "filename": DATA_FOLDER + "vaccinated-regions.csv"},
    "vaccinated_people":
        {"url": "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/ockovani-demografie.csv",
         "filename": DATA_FOLDER + "vaccinated-people.csv"},
    "vaccinated_geography":
        {"url": "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/ockovani-geografie.csv",
         "filename": DATA_FOLDER + "vaccinated-geography.csv"},
}
gender_dict = {"M": "male", "Z": "female"}


def download_csv(url, filename):
    """
    Get CSV data.

    Args:
        url: csv URL
        filename: name of file

    """
    # check if already downloaded
    if not os.path.exists(filename):
        # download csv
        response = requests.get(url)
        with open(filename, 'w') as f:
            writer = csv.writer(f)
            for line in response.iter_lines():
                writer.writerow(line.decode('utf-8').split(','))


def insert_df_to_mongo(db, col_name, df):
    """
    Inserts data frame into mongo DB.
    If collections are already inserted, drop them and load again.

    Args:
        db: Mongo DB
        col_name: collection name
        df: data frame

    """
    # drop collection if already exists?
    if col_name in db.list_collection_names():
        db[col_name].drop()
    mycol = db[col_name]
    mycol.insert_many(df.to_dict('records'))


def load_infected(db):
    """
    Loads infected as:  date | age | gender | region | district

    Args:
        db: Mongo DB

    """
    # Infected CSV
    df_infected = pd.read_csv(CSV_FILES["infected"]["filename"],
                              usecols=lambda c: c in {'datum', 'vek', 'pohlavi', 'kraj_nuts_kod', 'okres_lau_kod'},
                              sep=",")
    df_infected.rename(columns={'datum': 'date', 'vek': 'age', 'pohlavi': 'gender', 'kraj_nuts_kod': 'region',
                                'okres_lau_kod': 'district'}, inplace=True)

    # rename gender names (M/Z) to full name (male/female)
    df_infected = df_infected.replace({"gender": gender_dict})

    # convert age to int
    df_infected['age'] = pd.to_numeric(df_infected['age'], errors='coerce')
    df_infected = df_infected.dropna(subset=['age'])
    df_infected['age'] = df_infected['age'].astype('int')

    insert_df_to_mongo(db, "infected", df_infected)  # insert into NoSQL db


def load_cured(db):
    """
    Loads cured as:  data | age | gender | region | district

    Args:
        db: Mongo DB

    """
    # Cured CSV
    df_cured = pd.read_csv(CSV_FILES["cured"]["filename"],
                           usecols=lambda c: c in {'datum', 'vek', 'pohlavi', 'kraj_nuts_kod', 'okres_lau_kod'},
                           sep=",")
    df_cured.rename(columns={'datum': 'date', 'vek': 'age', 'pohlavi': 'gender', 'kraj_nuts_kod': 'region',
                             'okres_lau_kod': 'district'}, inplace=True)

    # rename gender names (M/Z) to full name (male/female)
    df_cured = df_cured.replace({"gender": gender_dict})

    # convert age to int
    df_cured['age'] = pd.to_numeric(df_cured['age'], errors='coerce')
    df_cured = df_cured.dropna(subset=['age'])
    df_cured['age'] = df_cured['age'].astype('int')

    insert_df_to_mongo(db, "cured", df_cured)  # insert into NoSQL db


def load_dead(db):
    """
    Loads dead as:  date | age | gender | region | district

    Args:
        db: Mongo DB

    """
    # Dead CSV
    df_dead = pd.read_csv(CSV_FILES["dead"]["filename"],
                          usecols=lambda c: c in {'datum', 'vek', 'pohlavi', 'kraj_nuts_kod', 'okres_lau_kod'}, sep=",")
    df_dead.rename(columns={'datum': 'date', 'vek': 'age', 'pohlavi': 'gender', 'kraj_nuts_kod': 'region',
                            'okres_lau_kod': 'district'}, inplace=True)

    # rename gender names (M/Z) to full name (male/female)
    df_dead = df_dead.replace({"gender": gender_dict})

    # convert age to int
    df_dead['age'] = pd.to_numeric(df_dead['age'], errors='coerce')
    df_dead = df_dead.dropna(subset=['age'])
    df_dead['age'] = df_dead['age'].astype('int')

    insert_df_to_mongo(db, "dead", df_dead)  # insert into NoSQL db


def load_hospitalized(db):
    """
    Loads hospitalized as:  month | patients

    Args:
        db: Mongo DB

    """
    # Hospitalized CSV
    df_hospitalized = pd.read_csv(CSV_FILES["hospitalized"]["filename"],
                                  usecols=lambda c: c in {'datum', 'pacient_prvni_zaznam'}, sep=",")
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
    insert_df_to_mongo(db, "hospitalized", grouped_hospitalized)  # insert into NoSQL db


def load_tests(db):
    """
    Loads tests as:  month | tests

    Args:
        db: Mongo DB

    """
    # Tests CSV
    df_tests = pd.read_csv(CSV_FILES["tests"]["filename"],
                           usecols=lambda c: c in {'datum', 'pocet_PCR_testy', 'pocet_AG_testy'}, sep=",")
    df_tests.rename(columns={'datum': 'date', 'pocet_PCR_testy': 'tests_PCR', 'pocet_AG_testy': 'tests_antigen'},
                    inplace=True)
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

    insert_df_to_mongo(db, "tests", grouped_tests)  # insert into NoSQL db


def load_vaccinated(db):
    """
    Loads vaccinated as:
        vaccinated_regions:  region | count

        vaccinated_people:  date | age_group | gender

    Args:
        db: Mongo DB

    """
    # vaccinated regions
    df_vac_reg = pd.read_csv(CSV_FILES["vaccinated_regions"]["filename"],
                             usecols=lambda c: c in {'datum', 'kraj_nuts_kod'}, sep=",")
    df_vac_reg.rename(columns={'datum': 'date', 'kraj_nuts_kod': 'region'}, inplace=True)
    grouped_vac_reg = df_vac_reg.groupby(["region"]).count()
    grouped_vac_reg = grouped_vac_reg.reset_index()
    grouped_vac_reg = grouped_vac_reg.rename(columns={'date': 'vaccinated'})

    insert_df_to_mongo(db, "vaccinated_regions", grouped_vac_reg)  # insert into NoSQL db

    # vaccinated people
    # (date, age_group, gender)
    df_vac_people = pd.read_csv(CSV_FILES["vaccinated_people"]["filename"],
                                usecols=lambda c: c in {'datum', 'vekova_skupina', 'pohlavi'}, sep=",")
    df_vac_people.rename(columns={'datum': 'date',
                                  'vekova_skupina': 'age_group',
                                  'pohlavi': 'gender',
                                  }, inplace=True)

    # rename gender names (M/Z) to full name (male/female)
    df_vac_people = df_vac_people.replace({"gender": gender_dict})

    insert_df_to_mongo(db, "vaccinated_people", df_vac_people)  # insert into NoSQL db

    # vaccinated geography
    # (date, region, vaccine code)
    df_vac_geo = pd.read_csv(CSV_FILES["vaccinated_geography"]["filename"],
                             usecols=lambda c: c in {'datum',
                                                     'kraj_nuts_kod',
                                                     'vakcina_kod'}, sep=",")
    df_vac_geo.rename(columns={'datum': 'date',
                               'kraj_nuts_kod': 'region',
                               'vakcina_kod': "vaccine code"
                               }, inplace=True)

    insert_df_to_mongo(db, "vaccinated_geography", df_vac_geo)  # insert into NoSQL db

    if 'A3_view' in db.list_collection_names():
        db['A3_view'].drop()
    db.create_collection(
        'A3_view',
        viewOn='vaccinated_geography',
        pipeline=[{
            '$lookup': {
                'from': "vaccinated_people",
                'localField': "date",
                'foreignField': "date",
                'as': "vaccinated_people_data"
            }
        },
            {
                '$unwind': "$vaccinated_people_data"
            },
            {"$project": {"date": 1,
                          'region': 1,
                          'kraj_nuts_kod': '$vaccinated_people_data.kraj_nuts_kod',
                          'vaccine code': 1,
                          'age_group': '$vaccinated_people_data.age_group'}
             }
        ]
    )

    if 'C1_view' in db.list_collection_names():
        db['C1_view'].drop()

    db.create_collection(
        'C1_view',
        viewOn='A3_view',
        pipeline=[{
            '$lookup': {
                'from': "infected",
                'localField': "date",
                'foreignField': "date",
                'as': "infected_data"
            }
        },
            {
                '$unwind': "$infected_data"
            },
            {"$project": {"date": 1,
                          'region': 1,
                          'kraj_nuts_kod': 1,
                          'district': '$infected_data.district',
                          'vaccine code': 1,
                          'age_group': 1}
             }
        ]
    )

def main():
    """
    Run the data loader - downloads d
    Example:
        ./data_loader.py --mongo mongodb://localhost:27017/
        # runs data loader on localhost mongo db
    """
    args = parser.parse_args()

    global DATA_FOLDER
    DATA_FOLDER = args.folder

    # MongoDB connection
    mongo_client = pymongo.MongoClient(args.mongo)
    mongo_db = mongo_client[args.database]

    # Download CSV files into ./data/ folder
    # create folder if it doesn't exist
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)

    download_csv(CSV_FILES["infected"]["url"], CSV_FILES["infected"]["filename"])  # infected
    download_csv(CSV_FILES["cured"]["url"], CSV_FILES["cured"]["filename"])  # cured
    download_csv(CSV_FILES["dead"]["url"], CSV_FILES["dead"]["filename"])  # dead
    download_csv(CSV_FILES["hospitalized"]["url"], CSV_FILES["hospitalized"]["filename"])  # hospitalized
    download_csv(CSV_FILES["tests"]["url"], CSV_FILES["tests"]["filename"])  # tests
    download_csv(CSV_FILES["vaccinated_regions"]["url"],
                 CSV_FILES["vaccinated_regions"]["filename"])  # vaccinated regions
    download_csv(CSV_FILES["vaccinated_people"]["url"],
                 CSV_FILES["vaccinated_people"]["filename"])  # vaccinated regions
    download_csv(CSV_FILES["vaccinated_geography"]["url"],
                 CSV_FILES["vaccinated_geography"]["filename"])  # vaccinated geography

    print("All documents downloaded. Loading into database now...")

    # Manually deal with each collection
    load_infected(mongo_db)
    print("- infected loaded")
    load_cured(mongo_db)
    print("- cured loaded")
    load_dead(mongo_db)
    print("- dead loaded")
    load_hospitalized(mongo_db)
    print("- hospitalized loaded")
    load_tests(mongo_db)
    print("- tests loaded")
    load_vaccinated(mongo_db)
    print("- vaccinated loaded")

    print("All done.")


if __name__ == '__main__':
    main()

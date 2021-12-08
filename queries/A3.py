import pandas as pd
import pymongo
from bson.json_util import dumps
import json

def A3_extract_csv(db, csv_location="A3.csv"):
    pipeline = [
        {"$match":
             {'pohlavi':
                  {"$in": ['Z', 'M']},
              }
         },

        {"$project": {
            "kraj_nuts_kod": 1,
            "pohlavi": 1,
            "vekova_skupina": 1,
        }},
        {"$group":
            {
                "_id": {'kraj_nuts_kod': '$kraj_nuts_kod', 'pohlavi': '$pohlavi', 'vekova_skupina': '$vekova_skupina'},
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"kraj_nuts_kod": 1}}
    ]

    res_vaccinated = db.vaccinated_by_profession.aggregate(pipeline)
    res_vaccinated = dumps(list(res_vaccinated))
    res_vaccinated = json.loads(res_vaccinated)

    df_vaccinated = pd.json_normalize(res_vaccinated)
    df_vaccinated = df_vaccinated.rename(columns={"_id.kraj_nuts_kod": "cznuts"})

    df_vaccinated = df_vaccinated[df_vaccinated['cznuts'].notna()]

    # Get regions from mongodb
    pipeline = [
        {"$project": {
            "cznuts": 1,
            "text": 1
        }}
    ]

    res_regions = db.region_enumerator.aggregate(pipeline)
    res_regions = dumps(list(res_regions))
    res_regions = json.loads(res_regions)
    df_regions = pd.json_normalize(res_regions)

    # Rename region codes in vaccinated dataframe to actual text names
    for region_code in df_regions['cznuts'].unique():
        text_name = df_regions[df_regions["cznuts"] == region_code]["text"].values[0]
        df_vaccinated['cznuts'] = [text_name if code == region_code else code for code in df_vaccinated['cznuts']]

    # Rename columns in vaccinated dataframe
    df_vaccinated = df_vaccinated.rename(columns={"cznuts": "region", "_id.pohlavi": "gender",
                                                  "_id.vekova_skupina": "age_group"})
    df_vaccinated = df_vaccinated.set_index('region') # Ok?
    df_vaccinated.to_csv(csv_location, sep=';', encoding='utf-8')


def A3_plot_graph(csv_location="A3.csv", save_location="A3.png"):
    pass
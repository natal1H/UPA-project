import pandas as pd
import pymongo
from bson.json_util import dumps
import json

def a3(db):
    pipeline = [
        {"$match":
             {'pohlavi':
                  {"$in": ['Z','M']},
              # 'vekova_skupina':
              #     {"$in": ['70-74', '60-64']},
              }
         },

        {"$project": {
            "kraj_nuts_kod": 1,
            "pohlavi": 1,
            "vekova_skupina": 1,
        }},
        {"$group":
            {
                "_id": { 'kraj_nuts_kod': '$kraj_nuts_kod', 'pohlavi': '$pohlavi', 'vekova_skupina': '$vekova_skupina'},
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"kraj_nuts_kod": 1}}
    ]


    df = db.vaccinated_by_profession.aggregate(pipeline)
    df = dumps(list(df))
    df = json.loads(df)

    # pd_data_2020 = pd.DataFrame.from_dict(data_2020)
    df = pd.json_normalize(df)
    df = df.rename(
        columns={"_id.kraj_nuts_kod": "cznuts"})

    df = df[df['cznuts'].notna()]


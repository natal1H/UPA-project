import pymongo
import pandas as pd
import matplotlib.pyplot as plt
from bson.json_util import dumps
import re
import json
import seaborn as sns

"""UPA - 2nd part
    Theme: Covid-19

    Operations with query VL1:
    --

    Authors: 
        - Filip Bali (xbalif00)
        - Natália Holková (xholko02)
        - Roland Žitný (xzitny01)
"""


def VL2_extract_csv(db, csv_location="VL2.csv"):
    """
    Smrť po krajoch
    """

    pipeline = [
        {"$match":
             {'date':
                  {"$regex": "^202"},
              }
         },
        {"$project": {
            "date": 1,
            "region": 1,
        }
        },
        {"$group":
            {
                "_id": {"cznuts": "$region"},
                "dead_count": {"$sum": 1},
            }
        },

    ]

    df_dead = db.dead.aggregate(pipeline)
    df_dead = dumps(list(df_dead))
    df_dead = json.loads(df_dead)

    df_dead = pd.json_normalize(df_dead).reset_index(drop=True)
    df_dead = df_dead.rename(
        columns={"_id.cznuts": "cznuts",
                 })
    df_dead = df_dead[df_dead['cznuts'].notna()]

    pipeline = [
        {"$project": {
            "text": 1,
            "cznuts": 1,
            "region_shortcut": 1,
        }
        }
    ]

    region_enum = db.region_enumerator.aggregate(pipeline)
    region_enum = dumps(list(region_enum))
    region_enum = json.loads(region_enum)

    region_enum = pd.json_normalize(region_enum)
    region_enum = region_enum.drop('_id.$oid', 1)
    region_enum = region_enum[region_enum['cznuts'] != 'CZZZZ']

    df_dead = pd.merge(df_dead, region_enum, on='cznuts')
    df_dead = df_dead.rename(
        columns={"text": "region_name",
                 })

    ############################################################################
    pipeline = [
        {"$match":
             {'date':
                  {"$regex": "^202"},
              '$or':[
                      {'shot_order': 2, 'vaccine_code': 'CO01'},
                      {'shot_order': 2, 'vaccine_code': 'CO02'},
                      {'shot_order': 2, 'vaccine_code': 'CO03'},
                      {'shot_order': 1, 'vaccine_code': 'CO04'}
                      ],
              }
         },
        {"$project": {
            "date": 1,
            "cznuts": 1,
            "shot_count": 1,
            }
        },
        {"$group":
            {
                "_id": {"cznuts": "$cznuts"},
                'vaccinated_count': {
                        '$sum': '$shot_count'
                }
            }
        },

    ]

    df_vaccinated_region = db.vaccinated_geography.aggregate(pipeline)
    df_vaccinated_region = dumps(list(df_vaccinated_region))
    df_vaccinated_region = json.loads(df_vaccinated_region)

    df_vaccinated_region = pd.json_normalize(df_vaccinated_region).reset_index(drop=True)
    df_vaccinated_region = df_vaccinated_region.astype({"vaccinated_count": int})
    df_vaccinated_region = df_vaccinated_region.rename(
        columns={"_id.cznuts": "cznuts",
                 })
    df_dead_vaccinated_region = pd.merge(df_dead, df_vaccinated_region, on='cznuts')

    pipeline = [
        {"$project": {
            "value": 1,
            "territory_code": 1,
            "territory_txt": 1,
            "valid_date": 1,
            "gender_code": 1,
            "gender_txt": 1,
            "age_code": 1,
            "age_txt": 1,
        }
        }
    ]

    demo = db.demographic_data.aggregate(pipeline)
    demo = dumps(list(demo))
    demo = json.loads(demo)

    df_demo = pd.json_normalize(demo)
    df_demo = df_demo.drop(columns='_id.$oid')

    df_demo = df_demo[(df_demo['valid_date'] == '2020-12-31') &
                      ((df_demo['territory_code'] == '3018') |
                       (df_demo['territory_code'] == '3026') |
                       (df_demo['territory_code'] == '3034') |
                       (df_demo['territory_code'] == '3042') |
                       (df_demo['territory_code'] == '3051') |
                       (df_demo['territory_code'] == '3069') |
                       (df_demo['territory_code'] == '3077') |
                       (df_demo['territory_code'] == '3085') |
                       (df_demo['territory_code'] == '3093') |
                       (df_demo['territory_code'] == '3107') |
                       (df_demo['territory_code'] == '3115') |
                       (df_demo['territory_code'] == '3123') |
                       (df_demo['territory_code'] == '3131') |
                       (df_demo['territory_code'] == '3140'))
                      ]

    df_demo['value'] = df_demo['value'].astype(int)

    df_demo = df_demo[df_demo['gender_code'].isnull() &
                      df_demo['age_code'].isnull()]

    # df_demo_sum = df_demo.groupby('territory_code')['value'].sum()
    # df_demo_sum = df_demo_sum.reset_index()
    # df_demo_sum = df_demo_sum.rename(columns={"territory_code": "value", "value": "population"})
    # df_demo_sum['value'] = df_demo_sum['value'].astype(int)

    df_demo['value'] = df_demo['value'].astype(int)
    df_demo = df_demo.rename(columns={"territory_txt": "region_name", "value": "population"})
    df_demo = df_demo.reset_index()
    df_demo = df_demo.drop(columns=['gender_code',
                                    'age_code',
                                    'valid_date',
                                    'gender_txt',
                                    'age_txt',
                                    'index',
                                    'territory_code',
                                    ])


    df_dead_vaccinated_region = pd.merge(df_dead_vaccinated_region, df_demo, on='region_name')

    df_dead_vaccinated_region = df_dead_vaccinated_region.drop(['cznuts','region_name',], 1)

    df_dead_vaccinated_region.to_csv(csv_location, sep=';', encoding='utf-8')
    ...

def VL2_plot_graph(csv_location="A1.csv", save_location=""):
    sns.set_style("darkgrid")

    df = pd.read_csv('VL2.csv', sep=";", encoding="utf-8")
    df = df.drop(['Unnamed: 0'], axis=1)

    fig, axes = plt.subplots(1, 1, figsize=(18, 18))


    ...
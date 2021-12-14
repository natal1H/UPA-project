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


def VL1_extract_csv(db, csv_location="VL1.csv"):
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
            "region_shortcut": 1,
            "cznuts": 1,
            }
        }
    ]

    region_enum = db.region_enumerator.aggregate(pipeline)
    region_enum = dumps(list(region_enum))
    region_enum = json.loads(region_enum)

    region_enum = pd.json_normalize(region_enum)
    region_enum = region_enum.drop('_id.$oid', 1)
    region_enum = region_enum[region_enum['cznuts'] !='CZZZZ']

    df_dead = pd.merge(df_dead, region_enum, on='cznuts')
    df_dead = df_dead.drop('cznuts', 1)

    df_dead.to_csv(csv_location, sep=';', encoding='utf-8')


def VL1_plot_graph(save_location="VL1.png"):
    sns.set_style("darkgrid")


    df = pd.read_csv('VL1.csv', sep=";", encoding="utf-8")
    df = df.drop(['Unnamed: 0'], axis=1)
    # df_graph = df[["region_shortcut", "infected_normalized_q1", "infected_normalized_q2",
    #                 "infected_normalized_q3", "infected_normalized_q4"]]

    fig, axes = plt.subplots(1, 1, figsize=(18, 18))

    # Graph designing
    g = sns.barplot(x='region_shortcut', y='value', hue='variable', data=pd.melt(df, ['region_shortcut']))


    g.set_xlabel("Kraj", fontsize=20)
    g.set_ylabel("Počet úmrtí", fontsize=20)
    g.set_title("Počet úmrtí v krajoch za pandémiu COVID-19", fontsize=30)
    labels = ["Úmrtia v krajoch"]
    h, l = axes.get_legend_handles_labels()
    axes.legend(h, labels, loc='upper left', title="", fontsize=20)
    plt.setp(axes.xaxis.get_majorticklabels(), rotation=45, fontsize=25)

    if len(save_location) > 0:
        plt.savefig(save_location)
    plt.show()
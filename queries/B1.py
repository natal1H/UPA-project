import pandas as pd
import pymongo
from argparse import ArgumentParser
from bson.json_util import dumps
import json
from matplotlib import pyplot as plt
import seaborn as sns

"""UPA - 2nd part
    Theme: Covid-19

    Operations with query B1:
    Sestavte 4 žebříčky krajů "best in covid" za poslední 4 čtvrtletí (1 čtvrtletí = 1 žebříček).
    Jako kritérium volte počet nově nakažených přepočtený na jednoho obyvatele kraje.
    Pro jedno čtvrtletí zobrazte výsledky také graficky.
    Graf bude pro každý kraj zobrazovat
        - celkový počet nově nakažených           -----> (jeden sloupcový graf)
        - celkový počet obyvatel                  --^
        - počet nakažených na jednoho obyvatele.  -----> (jeden čárový graf)
    Graf můžete zhotovit kombinací dvou grafů do jednoho
        (jeden sloupcový graf zobrazí první dvě hodnoty a druhý, čárový graf, hodnotu třetí).

    Authors: 
        - Filip Bali (xbalif00)
        - Natália Holková (xholko02)
        - Roland Žitný (xzitny01)
"""

parser = ArgumentParser(prog='UPA-data_loader')
parser.add_argument('-m', '--mongo', help="Mongo db location",
                    default="mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000")
parser.add_argument('-d', '--database', help="Database name", default="UPA-db")


def B1_extract_csv(db):
    # TODO Q3-2020 - Q3-2021

    pipeline = [
        {"$match":
             {'date':
                  { "$regex": "^2020"}}
         },
        {"$project": {
            "date": 1,
            "region": 1,
            "quarter": {
                "$switch": {
                    "branches": [
                        {"case": {"$lte": [{"$month": {"$toDate": "$date"}}, 3]}, "then": 1},
                        {"case": {"$lte": [{"$month": {"$toDate": "$date"}}, 6]}, "then": 2},
                        {"case": {"$lte": [{"$month": {"$toDate": "$date"}}, 9]}, "then": 3},
                        {"case": {"$lte": [{"$month": {"$toDate": "$date"}}, 12]}, "then": 4}]
                }
            }
        }},
        {"$group":
            {
                "_id": {"region": "$region", "quarter": "$quarter"},
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"_id.region": 1, "_id.quarter": 1}}
    ]

    Q_infected_2020 = db.infected.aggregate(pipeline)
    json_Q_infected_2020 = dumps(list(Q_infected_2020))
    json_Q_infected_2020 = json.loads(json_Q_infected_2020)

    # pd_data_2020 = pd.DataFrame.from_dict(data_2020)
    df_Q_infected_2020 = pd.json_normalize(json_Q_infected_2020)

    # Delete rows when _id.region value is NaN
    df_Q_infected_2020 = df_Q_infected_2020[df_Q_infected_2020['_id.region'].notna()]

    # Jako kritérium volte počet nově nakažených přepočtený na jednoho obyvatele kraje.
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

    df_demo_sum = df_demo.groupby('territory_code')['value'].sum()
    df_demo_sum = df_demo_sum.reset_index()
    df_demo_sum = df_demo_sum.rename(columns={"territory_code": "value", "value": "population"})
    df_demo_sum['value'] = df_demo_sum['value'].astype(int)


    # Divide by quarter
    df_Q1_infected_2020 = df_Q_infected_2020[df_Q_infected_2020['_id.quarter'] == 1]
    df_Q1_infected_2020= df_Q1_infected_2020.rename(columns={"count": "infected", "_id.region": "cznuts", "_id.quarter": "quarter"})

    df_Q2_infected_2020 = df_Q_infected_2020[df_Q_infected_2020['_id.quarter'] == 2]
    df_Q2_infected_2020= df_Q2_infected_2020.rename(columns={"count": "infected", "_id.region": "cznuts", "_id.quarter": "quarter"})

    df_Q3_infected_2020 = df_Q_infected_2020[df_Q_infected_2020['_id.quarter'] == 3]
    df_Q3_infected_2020= df_Q3_infected_2020.rename(columns={"count": "infected", "_id.region": "cznuts", "_id.quarter": "quarter"})

    df_Q4_infected_2020 = df_Q_infected_2020[df_Q_infected_2020['_id.quarter'] == 4]
    df_Q4_infected_2020= df_Q4_infected_2020.rename(columns={"count": "infected", "_id.region": "cznuts", "_id.quarter": "quarter"})

    pipeline = [
            {"$project": {
                "cznuts": 1,
                "region_shortcut": 1,
                "value": 1,
                }
            }
    ]
    region_enum = db.region_enumerator.aggregate(pipeline)
    region_enum = dumps(list(region_enum))
    region_enum = json.loads(region_enum)

    df_region_enum = pd.json_normalize(region_enum)
    df_region_enum = df_region_enum.drop(columns='_id.$oid')

    df_Q1_infected_2020 = pd.merge(df_Q1_infected_2020, df_region_enum, on='cznuts')
    df_Q2_infected_2020 = pd.merge(df_Q2_infected_2020, df_region_enum, on='cznuts')
    df_Q3_infected_2020 = pd.merge(df_Q3_infected_2020, df_region_enum, on='cznuts')
    df_Q4_infected_2020 = pd.merge(df_Q4_infected_2020, df_region_enum, on='cznuts')

    df_Q1_infected_2020['value'] = df_Q1_infected_2020['value'].astype(int)
    df_Q2_infected_2020['value'] = df_Q2_infected_2020['value'].astype(int)
    df_Q3_infected_2020['value'] = df_Q3_infected_2020['value'].astype(int)
    df_Q4_infected_2020['value'] = df_Q4_infected_2020['value'].astype(int)

    df_Q1_infected_2020 = pd.merge(df_Q1_infected_2020, df_demo_sum, on='value')
    df_Q2_infected_2020 = pd.merge(df_Q2_infected_2020, df_demo_sum, on='value')
    df_Q3_infected_2020 = pd.merge(df_Q3_infected_2020, df_demo_sum, on='value')
    df_Q4_infected_2020 = pd.merge(df_Q4_infected_2020, df_demo_sum, on='value')

    df_Q1_infected_2020['infected_normalized'] = df_Q1_infected_2020['infected'] / df_Q1_infected_2020['population']
    df_Q2_infected_2020['infected_normalized'] = df_Q2_infected_2020['infected'] / df_Q2_infected_2020['population']
    df_Q3_infected_2020['infected_normalized'] = df_Q3_infected_2020['infected'] / df_Q3_infected_2020['population']
    df_Q4_infected_2020['infected_normalized'] = df_Q4_infected_2020['infected'] / df_Q4_infected_2020['population']

    df_Q1_infected_2020 = df_Q1_infected_2020.set_index('region_shortcut')
    df_Q2_infected_2020 = df_Q2_infected_2020.set_index('region_shortcut')
    df_Q3_infected_2020 = df_Q3_infected_2020.set_index('region_shortcut')
    df_Q4_infected_2020 = df_Q4_infected_2020.set_index('region_shortcut')

    df_Q1_infected_2020.to_csv('B1_Q1.csv', sep=';', encoding='utf-8')
    df_Q2_infected_2020.to_csv('B1_Q2.csv', sep=';', encoding='utf-8')
    df_Q3_infected_2020.to_csv('B1_Q3.csv', sep=';', encoding='utf-8')
    df_Q4_infected_2020.to_csv('B1_Q4.csv', sep=';', encoding='utf-8')


def B1_plot_graph(save_location="B1.png"):
    sns.set_style("darkgrid")

    # Load csv first, rename columns for later mergin, drop unnecessary columns
    df_q1 = pd.read_csv('B1_Q1.csv', sep=";", encoding="utf-8")
    df_q1 = df_q1.rename(columns={"infected": "infected_q1", "infected_normalized": "infected_normalized_q1"})
    df_q1 = df_q1.drop(['cznuts', 'value', 'quarter'], axis=1)
    df_q2 = pd.read_csv('B1_Q2.csv', sep=";", encoding="utf-8")
    df_q2 = df_q2.rename(columns={"infected": "infected_q2", "infected_normalized": "infected_normalized_q2"})
    df_q2 = df_q2.drop(['cznuts', 'value', 'quarter'], axis=1)
    df_q3 = pd.read_csv('B1_Q3.csv', sep=";", encoding="utf-8")
    df_q3 = df_q3.rename(columns={"infected": "infected_q3", "infected_normalized": "infected_normalized_q3"})
    df_q3 = df_q3.drop(['cznuts', 'value', 'quarter'], axis=1)
    df_q4 = pd.read_csv('B1_Q4.csv', sep=";", encoding="utf-8")
    df_q4 = df_q4.rename(columns={"infected": "infected_q4", "infected_normalized": "infected_normalized_q4"})
    df_q4 = df_q4.drop(['cznuts', 'value', 'quarter'], axis=1)

    # Merge dataframes into one
    df_merged = pd.merge(df_q1, df_q2, on=['region_shortcut', 'population'])
    df_merged = pd.merge(df_merged, df_q3, on=['region_shortcut', 'population'])
    df_merged = pd.merge(df_merged, df_q4, on=['region_shortcut', 'population'])

    fig, axes = plt.subplots(2, 1, figsize=(18, 18))
    fig.suptitle("\"Best in covid\" za posledné 4 štvrťroky", fontsize=40)
    # 1st graph - select necessary columns from merged dataframe (region population and newly infected
    df_graph1 = df_merged[["region_shortcut", "population", "infected_q1", "infected_q2", "infected_q3", "infected_q4"]]

    # Graph designing
    g = sns.barplot(x='region_shortcut', y='value', hue='variable', data=pd.melt(df_graph1, ['region_shortcut']), ax=axes[0])
    g.set_xlabel("Kraj", fontsize=20)
    g.set_ylabel("Počet", fontsize=20)
    g.set_title("Vývoj počtu novo nakazených vzhľadom na populáciu kraja", fontsize=30)
    g.set_yscale("log")
    labels = ["Populácia kraja", "Nakazení Q1", "Nakazení Q2", "Nakazení Q3", "Nakazení Q4"]
    h, l = axes[0].get_legend_handles_labels()
    axes[0].legend(h, labels, loc='upper left', title="", fontsize=15)
    plt.setp(axes[0].xaxis.get_majorticklabels(), rotation=45)

    # 2nd graph - select necessary columns from merged dataframe (infected per 1 inhabitant)
    df_graph2 = df_merged[["region_shortcut", "infected_normalized_q1", "infected_normalized_q2",
                           "infected_normalized_q3", "infected_normalized_q4"]]

    # Graph designing
    g = sns.lineplot(x='region_shortcut', y='value', hue='variable', data=pd.melt(df_graph2, ['region_shortcut']), ax=axes[1])
    g.set_xlabel("Kraj", fontsize=20)
    g.set_ylabel("Počet nakazených na 1 obyvateľa", fontsize=20)
    g.set_title("Vývoj počtu nakazených na 1 obyvateľa v krajoch", fontsize=30)
    labels = ["Nakazení na 1 obyvateľa Q1", "Nakazení na 1 obyvateľa Q2", "Nakazení na 1 obyvateľa Q3", "Nakazení na 1 obyvateľa Q4"]
    h, l = axes[1].get_legend_handles_labels()
    axes[1].legend(h, labels, loc='upper left', title="", fontsize=15)
    plt.setp(axes[1].xaxis.get_majorticklabels(), rotation=45)

    if len(save_location) > 0:
        plt.savefig(save_location)
    plt.show()


if __name__ == "__main__":
    args = parser.parse_args()
    # MongoDB connection
    mongo_client = pymongo.MongoClient(args.mongo)
    mongo_db = mongo_client[args.database]

    B1_extract_csv(mongo_db)
    B1_plot_graph("B1.png")

    mongo_client.close()

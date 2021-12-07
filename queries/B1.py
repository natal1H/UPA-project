import pandas as pd
import pymongo
from bson.json_util import dumps
import json

from matplotlib import pyplot as plt


def b1(db):
    """
    B1 description:
    Sestavte 4 žebříčky krajů "best in covid" za poslední 4 čtvrtletí (1 čtvrtletí = 1 žebříček).
    Jako kritérium volte počet nově nakažených přepočtený na jednoho obyvatele kraje.
    Pro jedno čtvrtletí zobrazte výsledky také graficky.
    Graf bude pro každý kraj zobrazovat
        - celkový počet nově nakažených           -----> (jeden sloupcový graf)
        - celkový počet obyvatel                  --^
        - počet nakažených na jednoho obyvatele.  -----> (jeden čárový graf)
    Graf můžete zhotovit kombinací dvou grafů do jednoho
        (jeden sloupcový graf zobrazí první dvě hodnoty a druhý, čárový graf, hodnotu třetí).
    """

    def data_processing():
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
        df_demo = df_demo.drop('_id.$oid', 1)

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
        df_demo_sum= df_demo_sum.rename(columns={"territory_code": "value", "value": "population"})
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
        df_region_enum = df_region_enum.drop('_id.$oid', 1)

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

        df_Q1_infected_2020.to_csv('B1_Q1.csv', sep=';', encoding='utf-8')
        df_Q2_infected_2020.to_csv('B1_Q2.csv', sep=';', encoding='utf-8')
        df_Q3_infected_2020.to_csv('B1_Q3.csv', sep=';', encoding='utf-8')
        df_Q4_infected_2020.to_csv('B1_Q4.csv', sep=';', encoding='utf-8')

    def b1_plot_graph():

        df = pd.read_csv('B1_Q1.csv', sep=";", encoding="utf-8")
        df['value'] = df['value'].astype(int)

        # TODO dvojity graf
        # TODO liniovy graf

        # TODO value je zle, len pre ukazku
        df.plot.bar(x="region_shortcut", y=["infected", "value"], color=['red', 'blue'])
        plt.title("B1 Q1 Bar chart")
        plt.xlabel("Regions", labelpad=15)
        plt.ylabel("Count", labelpad=15)

        plt.show()

    data_processing()
    b1_plot_graph()



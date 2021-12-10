import pandas as pd
import pymongo
from bson.json_util import dumps
import json
from matplotlib import pyplot as plt
import seaborn as sns


def B1_extract_csv(db):
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

    # Load csv first
    df_q1 = pd.read_csv('B1_Q1.csv', sep=";", encoding="utf-8")
    df_q2 = pd.read_csv('B1_Q2.csv', sep=";", encoding="utf-8")
    df_q3 = pd.read_csv('B1_Q3.csv', sep=";", encoding="utf-8")
    df_q4 = pd.read_csv('B1_Q4.csv', sep=";", encoding="utf-8")

    fig, axes = plt.subplots(4, 2, figsize=(14, 18))
    axes = axes.flatten()
    fig.suptitle("\"Best in covid\" za posledné 4 štvrťročia", fontsize=30)

    idx = 0
    for df_q in [df_q1, df_q2, df_q3, df_q4]:
        # 1st graph - bar plots of newly infected and total population of region
        df_graph1 = df_q[['region_shortcut', 'infected', 'population']]
        df_graph1 = df_graph1.astype({"infected": int, "population": int})
        df_graph1.reset_index()
        g = sns.barplot(x='region_shortcut', y='value', hue='variable',
                        data=pd.melt(df_graph1, ['region_shortcut']), ax=axes[idx])
        g.set_yscale("log")

        # 2nd graph - line plot of infected per 1 inhabitant
        df_graph2 = df_q[["region_shortcut", "infected_normalized"]]
        g = sns.lineplot(data=df_graph2, x="region_shortcut", y="infected_normalized", ax=axes[idx + 1])

        idx += 2

    if len(save_location) > 0:
        plt.savefig(save_location)
    plt.show()



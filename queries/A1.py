import pymongo
import pandas as pd
import matplotlib.pyplot as plt
from bson.json_util import dumps
import re
import json

"""UPA - 2nd part
    Theme: Covid-19
    
    Operations with query A1:
    Vytvořte čárový (spojnicový) graf zobrazující vývoj covidové situace po měsících pomocí následujících hodnot: 
    počet nově nakažených za měsíc, počet nově vyléčených za měsíc, počet nově hospitalizovaných osob za měsíc, 
    počet provedených testů za měsíc. Pokud nebude výsledný graf dobře čitelný, zvažte logaritmické měřítko, 
    nebo rozdělte hodnoty do více grafů. 
    
    Authors: 
        - Filip Bali (xbalif00)
        - Natália Holková (xholko02)
        - Roland Žitný (xzitny01)
"""


def A1_extract_csv(db, csv_location="A1.csv"):
    # aggregation pipeline for infected and cured
    pipeline = [
        {"$group": {
            "_id": {
                "year": {"$year": {
                    "$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}
                }},
                "month": {"$month": {
                    "$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}
                }},
            },
            "count": {"$sum": 1}
        }},
        {'$project': {
            "month": {
                "$cond": {
                    "if": {
                        "$gte": ["$_id.month", 10]
                    },
                    "then": {
                        "$concat": [
                            {"$toString": "$_id.year"},
                            "-",
                            {"$toString": "$_id.month"}
                        ]
                    },
                    "else": {
                        "$concat": [
                            {"$toString": "$_id.year"},
                            "-0",
                            {"$toString": "$_id.month"}
                        ]
                    }
                }
            },
            "count": "$count"
        }}
    ]

    # aggregate infected - group by month and year and count total number in that month
    res_infected = db.infected.aggregate(pipeline)
    json_data = dumps(list(res_infected))
    json_data = re.sub('"_id": {"year": [\d]+, "month": [\d]+}, ', '', json_data)
    df_infected = pd.DataFrame.from_dict(json.loads(json_data))
    df_infected = df_infected.rename(columns={"count": "infected"})

    # aggregate cured - group by month and year and count total number in that month
    res_cured = db.cured.aggregate(pipeline)
    json_data = dumps(list(res_cured))
    json_data = re.sub('"_id": {"year": [\d]+, "month": [\d]+}, ', '', json_data)
    df_cured = pd.DataFrame.from_dict(json.loads(json_data))
    df_cured = df_cured.rename(columns={"count": "cured"})

    # get hospitalized by months - already by months
    res_hospitalized = db.hospitalized.find({})
    df_hospitalized = pd.DataFrame(list(res_hospitalized))  # transform to pandas DataFrame
    df_hospitalized.pop("_id")
    df_hospitalized = df_hospitalized.rename(columns={"patients": "hospitalized"})

    # get tests by months - already by months
    res_tests = db.tests.find({})
    df_tests = pd.DataFrame(list(res_tests))  # transform to pandas DataFrame
    df_tests.pop("_id")

    # merge dataframes
    merged = pd.merge(df_infected, df_cured, how='outer', on='month')
    merged = pd.merge(merged, df_hospitalized, how='outer', on='month')
    merged = pd.merge(merged, df_tests, how='outer', on='month')
    merged = merged.sort_values(by=['month'], ascending=True)

    merged.to_csv(csv_location, sep=';', encoding='utf-8')


def A1_plot_graph(csv_location="A1.csv"):
    df = pd.read_csv(csv_location, sep=";", encoding="utf-8")

    df.plot(x="month", y=["infected", "cured", "hospitalized", "tests"], logy=True)
    plt.title("A1")
    plt.xlabel("Month", labelpad=15)
    plt.ylabel("Count", labelpad=15)

    plt.savefig("Plots/A1.png")
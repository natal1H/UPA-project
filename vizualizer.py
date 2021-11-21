import pymongo
from argparse import ArgumentParser
import pandas as pd
import matplotlib.pyplot as plt
from bson.json_util import dumps
import re
import json

"""UPA - 2nd part
    Theme: Covid-19
    Authors: 
        - Filip Bali (xbalif00)
        - Natália Holková (xholko02)
        - Roland Žitný (xzitny01)
"""

parser = ArgumentParser(prog='UPA-data_loader')
parser.add_argument('-m', '--mongo', help="Mongo db location",
                    default="mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000")
parser.add_argument('-d', '--database', help="Database name", default="UPA-db")


def A1(db):
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
    #merged = merged.fillna(0)  # replace NaN with 0
    merged = merged.sort_values(by=['month'], ascending=True)

    merged.plot(x="month", y=["infected", "cured", "hospitalized", "tests"], logy=True)
    plt.title("A1")
    plt.xlabel("Month", labelpad=15)
    plt.ylabel("Count", labelpad=15)

    plt.show()


def main():
    args = parser.parse_args()
    # MongoDB connection
    mongo_client = pymongo.MongoClient(args.mongo)
    mongo_db = mongo_client[args.database]

    A1(mongo_db)

    mongo_client.close()


if __name__ == "__main__":
    main()

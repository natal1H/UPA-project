import pymongo
from bson.json_util import dumps
import json

def B1(db):
    # B1 task

    # year=2020
    pipeline = [
        {"$match":
            {'date':
                 { "$regex": "^2020"}}
        },
        {"$project": {
            "date": 1,
            "district": 1,
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
                "_id": {"district": "$district", "quarter": "$quarter"},
                "count": {"$sum": 1},
            }
        },
        { "$sort": {"_id.district": 1, "_id.quarter": 1}}
    ]

    quarter_infected_2020 = db.infected.aggregate(pipeline)
    json_data_2020 = dumps(list(quarter_infected_2020))
    # print(json_data)


    # year=2021
    pipeline = [
        {"$match":
            {'date':
                 { "$regex": "^2021"}}
        },
        {"$project": {
            "date": 1,
            "district": 1,
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
                "_id": {"district": "$district", "quarter": "$quarter"},
                "count": {"$sum": 1},
            }
        },
        { "$sort": {"_id.district": 1, "_id.quarter": 1}}
    ]

    quarter_infected_2021 = db.infected.aggregate(pipeline)
    json_data_2021 = dumps(list(quarter_infected_2021))
    #print(json_data)

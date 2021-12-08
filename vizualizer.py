import pymongo
from argparse import ArgumentParser
from queries.A1 import A1_extract_csv, A1_plot_graph
from queries.B1 import b1

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


def main():
    args = parser.parse_args()
    # MongoDB connection
    mongo_client = pymongo.MongoClient(args.mongo)
    mongo_db = mongo_client[args.database]

    A1_extract_csv(mongo_db, "A1.csv")
    A1_plot_graph("A1.csv", "A1.png")
    #B1(mongo_db)

    mongo_client.close()


if __name__ == "__main__":
    main()

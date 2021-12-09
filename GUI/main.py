from PyQt5 import QtWidgets # import PyQt5 widgets
import sys
import pymongo
from argparse import ArgumentParser
from queries.A1 import A1_extract_csv, A1_plot_graph
from queries.A3 import A3_extract_csv, A3_plot_graph
from queries.B1 import b1
from MainWindow import Ui_UPA_Covid19 as Ui_MainWindow
"""

"""
parser = ArgumentParser(prog='UPA-data_loader')
parser.add_argument('-m', '--mongo', help="Mongo db location",
                    default="mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000")
parser.add_argument('-d', '--database', help="Database name", default="UPA-db")


def main():
    # TODO nejde to lebo QTpy to vadi takto treba najrprv spustit skript vizualize a potom az zapat GUI
    
    args = parser.parse_args()
    # MongoDB connection
    mongo_client = pymongo.MongoClient(args.mongo)
    mongo_db = mongo_client[args.database]

    A1_extract_csv(mongo_db, "A1.csv")
    A1_plot_graph("A1.csv", "Plots/A1.png")
    A3_extract_csv(mongo_db, "A3.csv")
    A3_plot_graph("A3.csv", "Plots/A3.png")
    #b1(mongo_db)

    mongo_client.close()

if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    window = QtWidgets.QMainWindow()
    ui = Ui_MainWindow()
    ui.setupUi(window)
    window.show()
    sys.exit(app.exec_())
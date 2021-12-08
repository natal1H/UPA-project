from PyQt5 import QtWidgets # import PyQt5 widgets
import sys
from MainWindow import Ui_UPA_Covid19 as Ui_MainWindow


if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    window = QtWidgets.QMainWindow()
    ui = Ui_MainWindow()
    ui.setupUi(window)
    window.show()
    sys.exit(app.exec_())
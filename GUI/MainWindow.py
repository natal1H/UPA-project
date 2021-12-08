# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'MainWindow.ui'
#
# Created by: PyQt5 UI code generator 5.15.4
#
# WARNING: Any manual changes made to this file will be lost when pyuic5 is
# run again.  Do not edit this file unless you know what you are doing.


from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtGui import QPixmap

from queries.A1 import A1_plot_graph


class Ui_UPA_Covid19(object):
    def setupUi(self, UPA_Covid19):
        UPA_Covid19.setObjectName("UPA_Covid19")
        UPA_Covid19.resize(2000, 1200)
        UPA_Covid19.setStyleSheet("background-color: #202124;")
        self.centralwidget = QtWidgets.QWidget(UPA_Covid19)
        self.centralwidget.setObjectName("centralwidget")
        self.gridLayout = QtWidgets.QGridLayout(self.centralwidget)
        self.gridLayout.setContentsMargins(0, 0, 0, 0)
        self.gridLayout.setHorizontalSpacing(15)
        self.gridLayout.setVerticalSpacing(25)
        self.gridLayout.setObjectName("gridLayout")
        self.horizontalLayout_2 = QtWidgets.QHBoxLayout()
        self.horizontalLayout_2.setSpacing(0)
        self.horizontalLayout_2.setObjectName("horizontalLayout_2")
        self.verticalWidget = QtWidgets.QWidget(self.centralwidget)
        self.verticalWidget.setMinimumSize(QtCore.QSize(160, 0))
        self.verticalWidget.setMaximumSize(QtCore.QSize(160, 16777215))
        self.verticalWidget.setObjectName("verticalWidget")
        self.verticalLayout = QtWidgets.QVBoxLayout(self.verticalWidget)
        self.verticalLayout.setSizeConstraint(QtWidgets.QLayout.SetDefaultConstraint)
        self.verticalLayout.setSpacing(25)
        self.verticalLayout.setObjectName("verticalLayout")
        self.A1 = QtWidgets.QPushButton(self.verticalWidget)
        self.A1.setMinimumSize(QtCore.QSize(150, 40))
        self.A1.setMaximumSize(QtCore.QSize(150, 40))
        self.A1.setStyleSheet("QPushButton {\n"
"    background-color: #D0D6DD;\n"
"    border-radius: 8px;\n"
"    padding: 5px;\n"
"    border: 2px solid #385653; \n"
"}\n"
"\n"
"QPushButton:hover\n"
"{\n"
"   background-color: #E7EAEE;\n"
"}\n"
"\n"
"QPushButton:pressed\n"
"{\n"
"   background-color: #FFFFFF;\n"
"}")
        self.A1.setObjectName("A1")
        self.verticalLayout.addWidget(self.A1)
        self.A3 = QtWidgets.QPushButton(self.verticalWidget)
        self.A3.setMinimumSize(QtCore.QSize(150, 40))
        self.A3.setMaximumSize(QtCore.QSize(150, 40))
        self.A3.setStyleSheet("QPushButton {\n"
"    background-color: #D0D6DD;\n"
"    border-radius: 8px;\n"
"    padding: 5px;\n"
"    border: 2px solid #385653; \n"
"}\n"
"\n"
"QPushButton:hover\n"
"{\n"
"   background-color: #E7EAEE;\n"
"}\n"
"\n"
"QPushButton:pressed\n"
"{\n"
"   background-color: #FFFFFF;\n"
"}")
        self.A3.setObjectName("A3")
        self.verticalLayout.addWidget(self.A3)
        self.B2 = QtWidgets.QPushButton(self.verticalWidget)
        self.B2.setMinimumSize(QtCore.QSize(150, 40))
        self.B2.setMaximumSize(QtCore.QSize(150, 40))
        self.B2.setStyleSheet("QPushButton {\n"
"    background-color: #D0D6DD;\n"
"    border-radius: 8px;\n"
"    padding: 5px;\n"
"    border: 2px solid #385653; \n"
"}\n"
"\n"
"QPushButton:hover\n"
"{\n"
"   background-color: #E7EAEE;\n"
"}\n"
"\n"
"QPushButton:pressed\n"
"{\n"
"   background-color: #FFFFFF;\n"
"}")
        self.B2.setObjectName("B2")
        self.verticalLayout.addWidget(self.B2)
        self.C1 = QtWidgets.QPushButton(self.verticalWidget)
        self.C1.setMinimumSize(QtCore.QSize(150, 40))
        self.C1.setMaximumSize(QtCore.QSize(150, 40))
        self.C1.setStyleSheet("QPushButton {\n"
"    background-color: #D0D6DD;\n"
"    border-radius: 8px;\n"
"    padding: 5px;\n"
"    border: 2px solid #385653; \n"
"}\n"
"\n"
"QPushButton:hover\n"
"{\n"
"   background-color: #E7EAEE;\n"
"}\n"
"\n"
"QPushButton:pressed\n"
"{\n"
"   background-color: #FFFFFF;\n"
"}")
        self.C1.setObjectName("C1")
        self.verticalLayout.addWidget(self.C1)
        self.VL1 = QtWidgets.QPushButton(self.verticalWidget)
        self.VL1.setMinimumSize(QtCore.QSize(150, 40))
        self.VL1.setMaximumSize(QtCore.QSize(150, 40))
        self.VL1.setStyleSheet("QPushButton {\n"
"    background-color: #D0D6DD;\n"
"    border-radius: 8px;\n"
"    padding: 5px;\n"
"    border: 2px solid #385653; \n"
"}\n"
"\n"
"QPushButton:hover\n"
"{\n"
"   background-color: #E7EAEE;\n"
"}\n"
"\n"
"QPushButton:pressed\n"
"{\n"
"   background-color: #FFFFFF;\n"
"}")
        self.VL1.setObjectName("VL1")
        self.verticalLayout.addWidget(self.VL1)
        self.VL2 = QtWidgets.QPushButton(self.verticalWidget)
        self.VL2.setMinimumSize(QtCore.QSize(150, 40))
        self.VL2.setMaximumSize(QtCore.QSize(150, 40))
        self.VL2.setStyleSheet("QPushButton {\n"
"    background-color: #D0D6DD;\n"
"    border-radius: 8px;\n"
"    padding: 5px;\n"
"    border: 2px solid #385653; \n"
"}\n"
"\n"
"QPushButton:hover\n"
"{\n"
"   background-color: #E7EAEE;\n"
"}\n"
"\n"
"QPushButton:pressed\n"
"{\n"
"   background-color: #FFFFFF;\n"
"}")
        self.VL2.setObjectName("VL2")
        self.verticalLayout.addWidget(self.VL2)
        self.horizontalLayout_2.addWidget(self.verticalWidget)
        self.line = QtWidgets.QFrame(self.centralwidget)
        self.line.setMinimumSize(QtCore.QSize(35, 0))
        self.line.setFrameShape(QtWidgets.QFrame.VLine)
        self.line.setFrameShadow(QtWidgets.QFrame.Sunken)
        self.line.setObjectName("line")
        self.horizontalLayout_2.addWidget(self.line)
        spacerItem = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Minimum)
        self.horizontalLayout_2.addItem(spacerItem)
        self.verticalLayout_4 = QtWidgets.QVBoxLayout()
        self.verticalLayout_4.setObjectName("verticalLayout_4")
        self.plot_image = QtWidgets.QLabel(self.centralwidget)
        self.plot_image.setText("")
        self.plot_image.setScaledContents(False)
        self.plot_image.setAlignment(QtCore.Qt.AlignCenter)
        self.plot_image.setObjectName("plot_image")
        self.verticalLayout_4.addWidget(self.plot_image)
        self.plot_comment = QtWidgets.QLabel(self.centralwidget)
        self.plot_comment.setMinimumSize(QtCore.QSize(0, 70))
        self.plot_comment.setMaximumSize(QtCore.QSize(16777215, 70))
        self.plot_comment.setStyleSheet("color: white;")
        self.plot_comment.setText("")
        self.plot_comment.setAlignment(QtCore.Qt.AlignCenter)
        self.plot_comment.setObjectName("plot_comment")
        self.verticalLayout_4.addWidget(self.plot_comment)
        self.horizontalLayout_2.addLayout(self.verticalLayout_4)
        spacerItem1 = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Minimum)
        self.horizontalLayout_2.addItem(spacerItem1)
        self.gridLayout.addLayout(self.horizontalLayout_2, 2, 0, 1, 1)
        self.horizontalWidget = QtWidgets.QWidget(self.centralwidget)
        self.horizontalWidget.setMinimumSize(QtCore.QSize(0, 80))
        self.horizontalWidget.setMaximumSize(QtCore.QSize(16777215, 50))
        self.horizontalWidget.setAutoFillBackground(False)
        self.horizontalWidget.setStyleSheet("background-color: #00a9e0")
        self.horizontalWidget.setObjectName("horizontalWidget")
        self.horizontalLayout = QtWidgets.QHBoxLayout(self.horizontalWidget)
        self.horizontalLayout.setContentsMargins(0, 0, 0, 0)
        self.horizontalLayout.setSpacing(0)
        self.horizontalLayout.setObjectName("horizontalLayout")
        self.label = QtWidgets.QLabel(self.horizontalWidget)
        self.label.setMaximumSize(QtCore.QSize(80, 80))
        self.label.setText("")
        self.label.setPixmap(QtGui.QPixmap("unnamed.jpg"))
        self.label.setScaledContents(True)
        self.label.setObjectName("label")
        self.horizontalLayout.addWidget(self.label)
        spacerItem2 = QtWidgets.QSpacerItem(10, 20, QtWidgets.QSizePolicy.Fixed, QtWidgets.QSizePolicy.Minimum)
        self.horizontalLayout.addItem(spacerItem2)
        self.label_2 = QtWidgets.QLabel(self.horizontalWidget)
        self.label_2.setStyleSheet("color: white;\n"
"font-size: 20px;\n"
"font-family: \"Lucida Console\";\n"
"font-weight: bold;")
        self.label_2.setObjectName("label_2")
        self.horizontalLayout.addWidget(self.label_2)
        spacerItem3 = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Minimum)
        self.horizontalLayout.addItem(spacerItem3)
        self.gridLayout.addWidget(self.horizontalWidget, 1, 0, 1, 1)
        UPA_Covid19.setCentralWidget(self.centralwidget)
        self.statusbar = QtWidgets.QStatusBar(UPA_Covid19)
        self.statusbar.setObjectName("statusbar")
        UPA_Covid19.setStatusBar(self.statusbar)

        self.retranslateUi(UPA_Covid19)
        QtCore.QMetaObject.connectSlotsByName(UPA_Covid19)

        # CALL ALL  DOTAZY
        A1_plot_graph()

        # SET UP BUTTONS
        self.A1.clicked.connect(self.A1_plot)
        self.A3.clicked.connect(self.A3_plot)
        self.B2.clicked.connect(self.B2_plot)
        self.C1.clicked.connect(self.C1_plot)
        self.VL1.clicked.connect(self.VL1_plot)
        self.VL2.clicked.connect(self.VL2_plot)

    def retranslateUi(self, UPA_Covid19):
        _translate = QtCore.QCoreApplication.translate
        UPA_Covid19.setWindowTitle(_translate("UPA_Covid19", "UPA - Covid 19"))
        self.A1.setText(_translate("UPA_Covid19", "A1"))
        self.A3.setText(_translate("UPA_Covid19", "A3"))
        self.B2.setText(_translate("UPA_Covid19", "B2"))
        self.C1.setText(_translate("UPA_Covid19", "C1"))
        self.VL1.setText(_translate("UPA_Covid19", "Vlastné 1"))
        self.VL2.setText(_translate("UPA_Covid19", "Vlastné 2"))
        self.label_2.setText(_translate("UPA_Covid19", " UPA - Covid 19         Autori: xbalif00, xholko02, xzitny01"))

    def A1_plot(self):
        """
        """
        self.plot_image.setPixmap(QPixmap("Plots/A1.png"))
        self.plot_comment.setText("A1 TEXT")

    def A3_plot(self):
        """
        """
        self.plot_image.setPixmap(QPixmap("../Plots/A3.png"))
        self.plot_comment.setText("A3 TEXT")

    def B2_plot(self):
        """
        """
        self.plot_image.setPixmap(QPixmap("../Plots/B2.png"))
        self.plot_comment.setText("B2 TEXT")

    def C1_plot(self):
        """
        """
        self.plot_comment.setText("C1 TEXT")

    def VL1_plot(self):
        """"
        """
        self.plot_image.setPixmap(QPixmap("../Plots/VL1.png"))
        self.plot_comment.setText("VL1 TEXT")

    def VL2_plot(self):
        """"
        """
        self.plot_image.setPixmap(QPixmap("../Plots/VL2.png"))
        self.plot_comment.setText("VL2 TEXT")
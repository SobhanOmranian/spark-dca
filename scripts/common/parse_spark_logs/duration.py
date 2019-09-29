#!/usr/bin/env python3

import json
from pprint import pprint
from urllib.request import urlopen
import csv
from datetime import datetime
from api import *
import sys

#import dateutil.parser
#from colorama import Fore
#from colorama import Style

class Task:
    def __init__(self, taskId, duration):
        self.taskId = int(taskId)
        self.duration = int(duration)
    def __str__(self):
        return f"""
		Task: [{self.taskId}]
			Duration: [{self.duration}],
		"""
    def __iter__(self):
        return iter([self.taskId, self.duration])

def writeCSV(fileName, data):
    with open(f"{fileName}.csv", "w") as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        #writer.writerow(Task._fields)
        writer.writerows(data)
        #for task in data:		
            #writer.writerow(task)


#getAppInfo("app-20180803093320-0006")
#getAppInfo("app-20180803092756-0004")
#getAppInfo("app-20180803093050-0005")


#getAppInfo("app-20180803094413-0009")
#getAppInfo("app-20180803093931-0007")
#getAppInfo("app-20180803094159-0008")

# Terasort 30GB - 4 Nodes
#getAppInfo("app-20180803100507-0012")
#getAppInfo("app-20180803100027-0010")
#getAppInfo("app-20180803100252-0011")

# Terasort 30GB - 1 Node
#getAppInfo("app-20180803102049-0015")
#getAppInfo("app-20180803101227-0013")
#getAppInfo("app-20180803101659-0014")


## 16C
#getAppInfo("app-20180816111440-0000")
#getAppInfo("app-20180816111717-0000")
#getAppInfo("app-20180816104221-0000")
## 8C
#getAppInfo("app-20180816105325-0000")
#getAppInfo("app-20180816105651-0000")
#getAppInfo("app-20180816103937-0000")
## 4C
#getAppInfo("app-20180816110249-0000")
#getAppInfo("app-20180816110017-0000")
#getAppInfo("app-20180816102104-0000")
## 2C
#getAppInfo("app-20180816110552-0000")
#getAppInfo("app-20180816110800-0000")
#getAppInfo("app-20180816101445-0000")
## 1C
#getAppInfo("app-20180816100428-0000")
#getAppInfo("app-20180816095935-0000")
#getAppInfo("app-20180816095717-0000")


#printLastExperiments("SVM with Params(10,1.0,0.01,hdfs://fs3.das5.tudelft.nl:9000/user/omranian/HiBench/SVM/Input)", 0, 4);
#printLastExperiments("NWeightGraphX", 5, 5);
#printLastExperiments("NWeightGraphX", 10, 5);


#getAppInfo("app-20181011120941-0000", True)
#getAppInfo("app-20181011121556-0001", True)
#getAppInfo("app-20181011122208-0002", True)
#getAppInfo("app-20181011122820-0003", True)
#getAppInfo("app-20181011123428-0004", True)

#print("\n\n 16 Cores\n\n")
#getAppInfo("app-20181011124349-0000", True)
#getAppInfo("app-20181011124933-0001", True)
#getAppInfo("app-20181011125520-0002", True)
#getAppInfo("app-20181011130128-0003", True)
#getAppInfo("app-20181011130703-0004", True)


#print("\n\n 8 Cores\n\n")
#getAppInfo("app-20181011113309-0000", True)
#getAppInfo("app-20181011113838-0001", True)
#getAppInfo("app-20181011114427-0002", True)
#getAppInfo("app-20181011114953-0003", True)
#getAppInfo("app-20181011115520-0004", True)


#print("\n\n 4 Cores\n\n")
#getAppInfo("app-20181011143511-0000", True)
#getAppInfo("app-20181011144103-0001", True)
#getAppInfo("app-20181011144653-0002", True)
#getAppInfo("app-20181011145254-0003", True)
#getAppInfo("app-20181011145254-0003", True)



#print("\n\n 2 Cores\n\n")
#getAppInfo("app-20181011150536-0000", True)
#getAppInfo("app-20181011151300-0001", True)
#getAppInfo("app-20181011152024-0002", True)
#getAppInfo("app-20181011152755-0003", True)
#getAppInfo("app-20181011153524-0004", True)


#print("\n\n 2 Cores\n\n")
#getAppInfo("app-20181011154344-0000", True)
#getAppInfo("app-20181011155333-0001", True)
#getAppInfo("app-20181011160317-0002", True)
#getAppInfo("app-20181011161313-0003", True)
#getAppInfo("app-20181011162303-0004", True)
#getAppInfo("app-20181010153034-0005", True)

#getAppInfo("app-20181030150153-0000", False)
#getAppInfo("app-20181030150536-0001", False)
#getAppInfo("app-20181030150914-0002", False)
#getAppInfo("app-20181030145528-0000", False)
#getAppInfo("app-20190305145109-0000", True)
#getAppInfo("app-20190305145655-0001", True)
#getAppInfo("app-20190305150214-0002", True)
#getAppInfo("app-20190305150745-0003", True)
#getAppInfo("app-20190305153334-0000", True)
#print("\n".join(sys.argv))
#print(sys.argv[1])
#getAppInfo(sys.argv[1], True)
getTotalOutput(sys.argv[1]);
#getAppInfo("123", True)
#getAppInfo("app-20180903142104-0034", False)
#getAppInfo("app-20180903142454-0035", False)
#getAppInfo("app-20180903142720-0036", False)
#getAppInfo("app-20180822120315-0000")
#getAppInfo("app-20180820140439-0000")

#getInfoForWorkload("kmeans-1000k-300000000r-5col", 16, 16, 4)
#getInfoForWorkload("terasort-Main-300000000r", 16, 8, 1)
#getInfoForWorkloadDefault("terasort-Main-300000000r", 16, 8, 4, 5)

#getInfoForWorkloadNumaRemote("terasort-Main-300000000r", 16, 8, 4, 5)

#experiments = getAllExperimentsNew("lda", "LDA Example with Params(file:///var/scratch/omranian/data/HiBench/LDA/Input,file:///var/scratch/omranian/data/HiBench/LDA/Output,50,5,online,4g)", 4, 16, 24, 5)
#experiments = getAllExperimentsNew("nweight", "NWeightGraphX", 2, 16, 24, 5)
#experiments = getAllExperimentsNew("linear", "LinearRegressionWithSGD with Params(file:///var/scratch/omranian/data/HiBench/Linear/Input,100,1.0E-5)", 2, 16, 24, 5)
#printAllExperimentsNew(experiments)

#writecsv("experiments", experiments)

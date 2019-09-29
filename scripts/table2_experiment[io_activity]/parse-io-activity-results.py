import csv
import numpy as np
import sys
import os
from pathlib import Path

from argparse import ArgumentParser

inputBytes = sys.argv[1]
outputBytes = sys.argv[2]
stagesOutputBytes = sys.argv[3]
replicationFactor = sys.argv[4]
outputFile = sys.argv[5]

appNameIndex = 0
nodeNameIndex = 1
executorIdIndex = 2
executorTotalReadKbIndex = 3
executorTotalWriteKbIndex = 4
HdfsTotalReadKbIndex = 5
HdfsTotalWriteKbIndex = 6


class Executor:

    def __init__(self, appName):
        self.appName = appName
        self.nodeName = ""
        self.executorId = 0
        self.executorTotalReadKb = 0
        self.executorTotalWriteKb = 0
        self.HdfsTotalReadKb = 0
        self.HdfsTotalWriteKb = 0
        
                                       
    def __repr__(self):
        return f"""
            Executor: {self.executorId},
                totalReadKb: {self.totalReadKb()}
                totalWriteKb: {self.totalWriteKb()}
                totalReadAndWriteKb: {self.totalReadAndWriteKb()}
                    """
    def totalReadKb(self):
        return self.executorTotalReadKb + self.HdfsTotalReadKb
    
    def totalWriteKb(self):
        return self.executorTotalWriteKb + self.HdfsTotalWriteKb
    
    def totalReadAndWriteKb(self):
        return self.totalReadKb() + self.totalWriteKb()
                    
class IoActivityExperiment:
    def __init__(self, appName):
        self.appName = appName
        self.executors = []

    def __repr__(self):
        return f"""
            IoActivityExperiment: {self.appName},
                Number of Executors: {len(self.executors)}
                totalReadKb: {self.totalReadKb()}
                totalWriteKb: {self.totalWriteKb()}
                totalReadAndWriteKb: {self.totalReadAndWriteKb()}
                    """
    def totalReadKb(self):
        return sum([item.totalReadKb() for item in self.executors])
    
    def totalWriteKb(self):
        return sum([item.totalWriteKb() for item in self.executors])
    
    def totalReadAndWriteKb(self):
        return sum([item.totalReadAndWriteKb() for item in self.executors])


executors = []
header = []
experiment = [];
            
def getExecutorResultFromLine (input):
    if input.startswith("appName"):
        return;
    
    # rstrip() because of the trailing new line. (e.g., "\n")
    input = input.rstrip().split(",")
    
    global experiment;
    print(input)
    print(input[executorTotalReadKbIndex])
    if experiment == None:
        experiment = IoActivityExperiment(input[appNameIndex])
    
    executor = Executor(f"{input[appNameIndex]}")
    executor.executorId = int(input[executorIdIndex])
    executor.nodeName = input[nodeNameIndex]
    executor.executorTotalReadKb= int(input[executorTotalReadKbIndex])
    executor.executorTotalWriteKb = int(input[executorTotalWriteKbIndex])
    executor.HdfsTotalReadKb = int(input[HdfsTotalReadKbIndex])
    executor.HdfsTotalWriteKb = int(input[HdfsTotalWriteKbIndex])
    print(executor)
    experiment.executors.append(executor)

def writeToOutput(experiment):
#     {Path.home()}/results/io-activity/outputPio.csv
    fileName = outputFile
    file_exists = os.path.isfile(fileName)
    
    with open(fileName, mode='a') as output_file:
        headers = ['appName', 'totalReadKb', 'totalWriteKb', 'totalReadAndWriteKb', 'inputBytes', 'outputBytes',
                'stagesOutputBytes','replicationFactor']
        writer = csv.writer(output_file, delimiter=',')

        if not file_exists:
            writer.writerow(headers)  # file doesn't exist yet, write a header
            
        row = [experiment.appName, experiment.totalReadKb(), experiment.totalWriteKb(),
                experiment.totalReadAndWriteKb(), inputBytes, outputBytes, stagesOutputBytes, replicationFactor]
        
        writer.writerow(row)

experiment = None;
for line in sys.stdin:
    getExecutorResultFromLine(line)
    
print(experiment)
writeToOutput(experiment)

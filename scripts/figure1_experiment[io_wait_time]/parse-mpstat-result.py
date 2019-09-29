import numpy as np
import pandas as pd 
from pathlib import Path
import os
import sys

from argparse import ArgumentParser

sparkHome = os.getenv('SPARK_HOME', '/home/omranian/scala-ide-workspace-spark/spark')
scriptDir = f"{sparkHome}/scripts"
sys.path.append(f'{scriptDir}/common/parse_spark_logs')
from api import *

parser = ArgumentParser()
parser.add_argument("-i", "--inputFile")
parser.add_argument("-o", "--outputFile")

args = parser.parse_args(sys.argv[1:])

inputFile = args.inputFile
outputFile = args.outputFile

data = pd.read_csv(inputFile, index_col="stage")
appName = str(data["appName"].values[0])
appId = appName.split("^", 1)[1]
print(appName)

filtered = data.drop(['nodeId', 'executorId', 'appName', 'cpu'], axis = 1)
filtered = filtered.apply(pd.to_numeric, errors='raise')

averaged = filtered.groupby(filtered.index).mean()

average_df = pd.DataFrame(averaged)

# Add Stage duration to the dataframe
average_df["duration"] = 0
for name, row in average_df.iterrows():
    average_df.loc[name, "duration"] = getStageDurationForApp(appId, name)


average_df.insert(0, 'appName', appName)
average_df = average_df.reset_index()
cols = average_df.columns.tolist()
cols.insert(0, cols.pop(cols.index('appName')))
average_df = average_df.reindex(columns = cols)

def writeToOutput(data):
    global fileNameWithoutExtension
#     f"{Path.home()}/results/mpstat/resultMpstat.csv"
    fileName = outputFile
    file_exists = os.path.isfile(fileName)
    
    with open(fileName, mode='a') as f:
        needHeader = f.tell() == 0  
        data.to_csv(f, index=False, header=needHeader)
        
writeToOutput(average_df)

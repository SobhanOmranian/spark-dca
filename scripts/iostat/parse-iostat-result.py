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
parser.add_argument("-a", "--appName")

args = parser.parse_args(sys.argv[1:])

inputFileName = args.inputFile
outputFileName = args.outputFile
appName = args.appName

data = pd.read_csv(inputFileName, index_col="stage")
data = data[data['appName'] == appName]
appName = str(data["appName"].values[0])
appId = appName.split("^", 1)[1]

filtered = data.drop(['nodeId', 'appName'], axis = 1)
filtered = filtered.apply(pd.to_numeric, errors='raise')

averaged = filtered.groupby(filtered.index).mean()
average_df = pd.DataFrame(averaged)

# Add Stage duration to the dataframe
average_df["duration"] = 0
for name, row in average_df.iterrows():
    stageDuration = 0
    try:
        stageDuration = getStageDurationForApp(appId, name)
    except:
        print(f"no stage info for stage {name}")
    average_df.loc[name, "duration"] = stageDuration


average_df.insert(0, 'appName', appName)
average_df = average_df.reset_index()
cols = average_df.columns.tolist()
cols.insert(0, cols.pop(cols.index('appName')))
average_df = average_df.reindex(columns = cols)
print("average_df:")
print(average_df)

def writeToOutput(data):
    global fileNameWithoutExtension
#     f"{Path.home()}/results/iostat/resultIostat.csv"
    fileName = outputFileName 
    file_exists = os.path.isfile(fileName)
    
    with open(fileName, mode='a') as f:
        needHeader = f.tell() == 0
        
        data.to_csv(f, index=False, header=needHeader)
        
writeToOutput(average_df)
import numpy as np
import pandas as pd 
from pathlib import Path
import os
import sys

sparkHome = os.getenv('SPARK_HOME', '/home/omranian/scala-ide-workspace-spark/spark')
scriptDir = f"{sparkHome}/scripts"
sys.path.append(f'{scriptDir}/common/parse_spark_logs')
from api import *

from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-i", "--inputFile")
parser.add_argument("-o", "--outputFile")

args = parser.parse_args(sys.argv[1:])

inputFile = args.inputFile
outputFile = args.outputFile

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 2000)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_colwidth', 800)

df = pd.read_csv(inputFile, index_col="stage")

appName = str(df["appName"].values[0])
appId = appName.split("^", 1)[1]

cpuMean = df.groupby(df.index)["cpu"].mean()

# Get the maximum of every executor log. 
# Sum all executor memory usages per node.
# Sum all the nodes to get the overal memory usage for each stage 
memMax = df.groupby(["stage","nodeId","executorId"]).max()
all_nodes_sum = memMax.groupby("stage").sum()["mem"]
all_nodes_mem_sum_df = pd.DataFrame(all_nodes_sum)

cpuMean_df = pd.DataFrame(cpuMean)
cpuMean_df['mem'] = all_nodes_mem_sum_df['mem']
 
# Add Stage duration to the dataframe
cpuMean_df["duration"] = 0
print(cpuMean_df)
for name, row in cpuMean_df.iterrows():
    cpuMean_df.loc[name, "duration"] = getStageDurationForApp(appId, int(float(name)))

# Add appName column to the dataframe
cpuMean_df["appName"] = appName
cpuMean_df["appName"] = cpuMean_df["appName"].apply(lambda x: appName)
  
cpuMean_df = cpuMean_df.reset_index()
cpuMean_df = cpuMean_df[["appName", "stage", "cpu", "mem", "duration"]]
  
# Calculate memory for each stage individually
for name, row in cpuMean_df.iterrows():
    sumOfPreviousRows = cpuMean_df[0:name]["mem"].sum()
    diff = row["mem"] - sumOfPreviousRows
    cpuMean_df.loc[name, "mem"] = diff

def writeToOutput(data):
    global fileNameWithoutExtension
#     f"{Path.home()}/results/top/resultTop.csv"
    fileName = outputFile
    file_exists = os.path.isfile(fileName)
      
    with open(fileName, mode='a') as f:
        needHeader = f.tell() == 0
        data.to_csv(f, index=False, header=needHeader)
          
writeToOutput(cpuMean_df)
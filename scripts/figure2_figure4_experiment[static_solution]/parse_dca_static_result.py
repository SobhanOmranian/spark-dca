import numpy as np
import pandas as pd 
from pathlib import Path
import os
import sys

RESULT_HOME = os.getenv('RESULT_HOME', f"{Path.home()}/results")

from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-n", "--appName")
parser.add_argument("-a", "--adaptive")
parser.add_argument("-t", "--total_cores")
parser.add_argument("-f", "--topFile")
parser.add_argument("-o", "--outputFile")


args = parser.parse_args(sys.argv[1:])

appName = args.appName
adaptive = int(args.adaptive)
totalCores = args.total_cores
topFile = args.topFile
outputFile = args.outputFile

data = pd.read_csv(topFile)
filtered = (data[data['appName'] == appName] )
print(filtered)
output = filtered

output = output[["appName","stage", "duration"]].set_index("stage")
output["usedCores"] = 0
output["totalCores"] = totalCores
output["adaptive"] = adaptive
output["isIo"] = 0

ioStageDict = {
    "terasort" : [0, 1, 2],
    "pagerank": [0, 5],
    "join": [0, 1],
    "aggregation": [0]
}

filtered.set_index("appName")

dca_df = pd.read_csv(f"{RESULT_HOME}/combined.dca")
dca_grouped_df = dca_df.set_index("stage").sort_index().groupby("executorId")

# Add isIo column
for name, row in filtered.iterrows():
    stage = int(row["stage"])
    applicationName = row["appName"]
    print(f"stage {stage}")
    
    # Find the right number of cores used
    totalSelectedCore = 0
    for executorId, group in dca_grouped_df:
        group = group[group.index == stage]
        selectedCore = group["selection"].max()
        print(f"selectedCore for stage {stage} and exec {executorId}: {selectedCore}")
        totalSelectedCore += selectedCore
        print(f"totalSelectedCore: {totalSelectedCore}")
    
    
    for appName, ioStages in ioStageDict.items():
        if appName in applicationName:
            if (stage in ioStages):
                # Only set isIo for non-default runs
                print (totalCores == totalSelectedCore)
                if int(totalSelectedCore) != int(totalCores):
                    output.at[stage,'isIo'] = 1
                output.at[stage,'usedCores'] = totalSelectedCore
                break
            else:
                output.at[stage,'usedCores'] = totalCores
    print(output)

# Move appName to the start
output = output.reset_index()
output = output.set_index('appName').reset_index()


def writeToOutput(data):
    global fileNameWithoutExtension
#     f"{RESULT_HOME}/resultDca.csv"
    fileName = outputFile
    file_exists = os.path.isfile(fileName)
    
    with open(fileName, mode='a') as f:
        needHeader = f.tell() == 0
        data.to_csv(f, index=False, header=needHeader)
        
writeToOutput(output)
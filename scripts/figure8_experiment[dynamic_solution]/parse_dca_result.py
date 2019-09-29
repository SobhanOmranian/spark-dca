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
output = filtered

output = output[["appName","stage", "duration"]].set_index("stage")
output["usedCores"] = 0
output["totalCores"] = totalCores
output["adaptive"] = adaptive
print(output)

filtered.set_index("appName")
print (output)


dca_df = pd.read_csv(f"{RESULT_HOME}/combined.dca")
dca_grouped_df = dca_df.set_index("stage").sort_index().groupby("executorId")

for name, row in filtered.iterrows():
    totalSelectedCore = 0
    stage = int(row["stage"])
#     if (adaptive != 100):
    for name, group in dca_grouped_df:
        group = group[group.index == stage]
        selectedCore = group["selection"].max()
        print(f"selectedCore for stage {stage} and exec {name}: {selectedCore}")
        totalSelectedCore += selectedCore
        
    print(f"totalSelectedCore: {totalSelectedCore}")
    usedCores = totalSelectedCore
#     else:
#         usedCores = totalCores
    
    output.at[stage,'usedCores'] = usedCores
    print(f"stage {stage}")
print(filtered)
print(output['usedCores'])
output = output.reset_index()

# Move appName to the start
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
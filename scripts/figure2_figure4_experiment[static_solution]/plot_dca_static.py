import matplotlib.pyplot as plt
from matplotlib.pyplot import  savefig
import matplotlib.patches as mpatches
import pandas as pd
import numpy as np
from numpy import dtype
from matplotlib.pyplot import ylabel
import math
import re
import os
import sys

sparkHome = os.getenv('SPARK_HOME', '/home/omranian/scala-ide-workspace-spark/spark')
scriptDir = f"{sparkHome}/scripts"
sys.path.append(f'./common')
from util import *
import util

from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-i", "--inputFile")
parser.add_argument("-o", "--outputFilePath")


args = parser.parse_args(sys.argv[1:])
inputFileName = args.inputFile
outputFilePath = args.outputFilePath

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 2000)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_colwidth', 800)
plt.style.use('seaborn-ticks')
plt.rc('font', family='serif')
plt.rcParams.update({'font.size': util.setting_font_size_2})


def normalise (x, min, max):
    return (x - min) / (max - min)


def get_percentage_change(current, previous):
    if current == previous:
        return 100.0
    try:
        return round (((current - previous) / previous) * 100.0 , 2) 
    except ZeroDivisionError:
        return 0
    
    
def findPattern(pattern, name):
    result = None
    matches = re.findall(pattern, name)
    for matchNum, match in enumerate(matches):
        numberMatches = re.findall("(\d+)", match) 
        for mNum, m in enumerate(numberMatches):
            result = m
    return result
    
def findNumberOfNodes(name):
    found = findPattern(r"[0-9]+n", name)
    if (found == None):
        return 4
    else:
        return int(found)
def getMeanAndTransformBack(df, name):
    mean = df.groupby(["stage", "usedCores"]).mean()
    mean = mean.reset_index()
    mean['appName'] = name
    return mean

def findMinimumDuration(group):
    print(group)
    # Find max cores
    maxUsedCores = group["totalCores"].max()
    print(f"maxUsedCores = {maxUsedCores}")
    # Filter default rows
    group = group.loc[group["usedCores"] != maxUsedCores]
    print(group)
    sum = group.groupby(["usedCores"])['duration'].sum()
    print(f"Sum: {sum}")
    minUsedCores = sum.idxmin()
    numCores = int(minUsedCores / numNodes)
    print(numCores, sum[minUsedCores])
    return numCores, sum[minUsedCores]

def getAppNameShort(longName):
    shortName = "unknown"
    for appName in appNames:
        if(appName in longName):
            if appName in appNamesDict:
                dictName = appNamesDict[appName]
                shortName = dictName
            else:
                shortName = appName
            break
    return shortName

appNames = ['lda',
             'SVM',
             'terasort-10000000r',
             'terasort-100000000r',
             'terasort-300000000r',
             's0-s1-terasort-1200000000r',
             'terasort-hdfs-Main-1200000000r',
             'terasort-hdfs-Main-4800000001r',
              'terasort',
               'nweight',
                'SparseNaiveBayes',
               'Join',
               'Aggregation',
               'Scan',
               'ScalaPageRank',
               'WordCount',
               ]
appNamesDict = {
    'terasort-hdfs-Main-1200000000r' : 'terasort-120GB',
    'terasort-hdfs-Main-4800000001r' : 'terasort-480GB'
    }

my_colors = [
    util.color_default,
    util.color_16c,
    util.color_8c,
     util.color_4c,
    util.color_2c,
    'purple'
    ]

color_dict = {
    64: util.color_64c,
    32 :  util.color_default,
    16 : util.color_16c,
    8 : util.color_8c,
    4 : util.color_4c,
    2 : util.color_2c,
    }

c64_patch = mpatches.Patch(color=util.color_64c, label='64c')
c32_patch = mpatches.Patch(color=util.color_default, label='Default (32)')
c16_patch = mpatches.Patch(color=util.color_16c, label='16 threads')
c8_patch = mpatches.Patch(color=util.color_8c, label='8 threads')
c4_patch = mpatches.Patch(color=util.color_4c, label='4 threads')
c2_patch = mpatches.Patch(color=util.color_2c, label='2 threads')
# bestfit_patch = mpatches.Patch(color=my_colors[5], label='BestFit')

data = pd.read_csv(inputFileName, comment='#' , dtype={
    'stage': int,
    'duration': float,
    'usedCores': int,
    'totalCores': int,
    'adaptive': int,
    'isIo': int
    })

data['appName'] = data['appName'].apply(lambda x: x.split("^", 1)[0])
data.replace(['static\d*-'],  [''], regex=True, inplace=True)
data.replace(['-\dus'],  [''], regex=True, inplace=True)
data['appName'] = data['appName'].apply(lambda x:   x.split("^", 1)[0])
data["duration"] = data["duration"].apply(lambda s: s / 1000)
appCount = len(data["appName"].unique().tolist())
print(data)

dfgroup = data.groupby(["appName"], sort=False)
i = 0
numNodes = 4
numberOfRows = math.ceil(appCount / 2)
print(f"numberOfRows- {numberOfRows}") 
print(dfgroup)
for name, group in dfgroup:
    fig = plt.figure(figsize=(6, 5))
    
    #Find max cores
    maxUsedCores = group["totalCores"].max()
    
    # Find number of nodes
    numNodes = findNumberOfNodes(name)
    print(f"numberOfNodes: {numNodes}")
    
    # Find mean for the default
    mean_default = group.loc[group["usedCores"] == maxUsedCores ]
    mean_default = getMeanAndTransformBack(mean_default, name)

    # Find the default duration
    mean_default_duration = mean_default['duration'].sum()
    print(f"Default duration: {mean_default_duration}")

#     continue
    pos = 0
    numExperiments = len(group["usedCores"].unique())
    previous_values = np.array(numExperiments)
    
    # Get all the non-default rows
    print(group)
    group = group.loc[group["usedCores"] != maxUsedCores ]
    print("All non default values:")
    print(group)
    group = getMeanAndTransformBack(group, name)
    
    # Concat the mean default which we found earlier with non default rows
    for namerow, row in group.iterrows():
        if row["isIo"] == 0:
            mean_default_row = (mean_default.loc[mean_default["stage"] == row["stage"]])
            group.loc[namerow, "duration"] = mean_default_row["duration"].values

    group = pd.concat([mean_default, group])
    group = group.sort_values(["totalCores", "stage"], ascending=[False, True])
  
    # Find the minimum duration
    
    minCores, minDuration = findMinimumDuration(group)
    print(f"Min Cores: {minCores} and Min Duration: {minDuration}")
    percentageChange = get_percentage_change(minDuration, mean_default_duration)
    print(f"Percentage change from 32c to {minCores}c: {percentageChange}%")

    for name2, group2 in group.groupby(["stage"], sort=False):
        group2 = group2.reset_index()
        
        # Set the colors
        colors = []
        for namerow, row in group2.iterrows():
            if row['isIo'] == 0:
                colors.append(util.color_default)
            else:
                colors.append(color_dict[row["usedCores"] / numNodes])
        
        dataset = group2["duration"].values
        
        # Find min in each stage
        minRow = group2.loc[group2["duration"].idxmin()]
        assert isinstance(minRow, pd.Series)

        dataset = np.append(dataset, minRow["duration"])
        colors.append(color_dict[minRow["usedCores"] / numNodes])
        
        x = [32, 16, 8, 4, 2, 'bestfit']
        xs = range(len(x))
        y = dataset
        
        barlist = plt.bar(xs, y, bottom=previous_values, color='black', width=0.5, linewidth=2, edgecolor='black')
        i = 0
        for bar in barlist:
            bar.set_facecolor(colors[i])
            bar.set_linewidth(0.7)
            bar.set_edgecolor(util.color_stage_border)
            i = i + 1 
        
        plt.xticks(xs, x)
        previous_values = np.array(previous_values) + np.array(dataset)
    default_patch = mpatches.Patch(color='black', label='Default')
    static_patch = mpatches.Patch(color='r', label='Static')
    if ("PageRank" not in name):
        legend = plt.legend(handles=[c32_patch, c16_patch, c8_patch, c4_patch, c2_patch], frameon=1, fontsize=util.setting_legend_font_size)
        frame = legend.get_frame()
        frame.set_facecolor('lightgrey')
    plt.xlabel("Number of Threads")
    plt.ylabel("Runtime (s)")
#     /Users/sobhan/scala-ide-workspace-spark/spark/publication/Big Data Paradigm/img
    savefig(f"{outputFilePath}/dca_static_{name}.pdf", dpi=100, bbox_inches='tight')

plt.show()
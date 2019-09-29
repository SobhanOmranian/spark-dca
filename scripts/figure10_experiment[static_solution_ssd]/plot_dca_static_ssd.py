import matplotlib.pyplot as plt
from matplotlib.pyplot import  savefig
import matplotlib.patches as mpatches
import pandas as pd
import numpy as np
from numpy import dtype
from matplotlib.pyplot import ylabel
import math
import re

import sys
sys.path.append(f'./common')
import util
from util import *
setup(util.setting_font_size_2, util.style_plot)

from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-i", "--inputFile")
parser.add_argument("-o", "--outputFilePath")

args = parser.parse_args(sys.argv[1:])

inputFileName = args.inputFile
outputFilePath = args.outputFilePath

def normalise (x, min, max):
    return (x-min)/(max-min)
def get_percentage_change(current, previous):
    if current == previous:
        return 100.0
    try:
        return round ( ((current - previous) / previous) * 100.0 , 2) 
    except ZeroDivisionError:
        return 0

appNames = ['lda',
             'SVM',
             'terasort-10000000r',
             'terasort-100000000r',
             'terasort-300000000r',
             's0-s1-terasort-1200000000r',
             'terasort-hdfs-Main-1200000000r',
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
    'terasort-hdfs-Main-1200000000r' : 'terasort-120GB'
    
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

def extractNumberOfCores(appName):
    result = None
    matches = re.findall(r"[0-9]+c", appName)
    for matchNum, match in enumerate(matches):
        numberMatches = re.findall("[0-9]+", match) 
        for mNum, m in enumerate(numberMatches):
            result = m
    return result

# data/dca/dca_static_ssd.csv
data = pd.read_csv(inputFileName, dtype={
    'stage': int,
    'duration': float,
    'usedCores': int,
    'totalCores': int,
    'adaptive': int,
    'isIo': int
    })
data['appName'] = data['appName'].apply (lambda x: x.split("^", 1)[0])
data['appName'] = data['appName'].apply (lambda x: re.sub(r"-[0-9]+c-", '-', x))
data["duration"] = data["duration"].apply(lambda s: s / 1000)
mean_data = data.groupby(["appName", "stage"]).mean()
mean_data = mean_data.reset_index()
appCount = len(data["appName"].unique().tolist())

def getMeanAndTransformBack(df, name):
    mean = df.groupby(["stage", "usedCores"]).mean()
    mean = mean.reset_index()
    mean['appName'] = name
    return mean

def findMinimumDuration(group):
    # Filter default rows
    group = group.loc[group["totalCores"] != 128]
    sum = group.groupby(["usedCores"])['duration'].sum()
    print(sum)
    minUsedCores = sum.idxmin()
    numCores = int( minUsedCores / numNodes)
    print(numCores,sum[minUsedCores])
    return numCores,sum[minUsedCores]

dfgroup = data.groupby(["appName"], sort=False)
i = 0
numNodes = 4
numberOfRows = math.ceil(appCount / 2)
print(f"numberOfRows- {numberOfRows}") 
for name, group in dfgroup:
    fig = plt.figure(figsize=(6,5))
    
    # Find mean for the default
    mean_default = group.loc[group["totalCores"] == 128 ]
    mean_default = getMeanAndTransformBack(mean_default, name)
    print("MEAN DEFAULT:")
    print(mean_default)
    # Find the default duration
    mean_default_duration = mean_default['duration'].sum()
    print(f"Default duration: {mean_default_duration}")
    

    pos = 0
    numExperiments = len(group["totalCores"].unique())
    previous_values = np.array(numExperiments)
    
    # Get all the non-default rows
    group = group.loc[group["totalCores"] != 128 ]
    


    group = getMeanAndTransformBack(group, name)
    
    
    # Concat the mean default which we found earlier with non default rows
    for namerow, row in group.iterrows():
        if row["isIo"] == 0:
            mean_default_row = (mean_default.loc[mean_default["stage"] == row["stage"]])
            group.loc[namerow, "duration"] = mean_default_row["duration"].values
    group = pd.concat([mean_default, group])
    
    group = group.sort_values(["totalCores", "stage"], ascending=[False, True])
    print("Updated group:")
    print(group)

    # Find the minimum duration
    minCores, minDuration = findMinimumDuration(group)
    print(f"Min Cores: {minCores} and Min Duration: {minDuration}")
    percentageChange = get_percentage_change(minDuration, mean_default_duration)
    print(f"Percentage change from 32c to {minCores}c: {percentageChange}%")

    for name2, group2 in group.groupby(["stage"], sort=False):
        group2 = group2.reset_index()
        colors = []
        for namerow, row in group2.iterrows():
            if row['isIo'] == 0:
                colors.append(util.color_default)
            else:
                colors.append(color_dict[row["usedCores"] / 4])
            
        
        dataset = group2["duration"].values
        
        # Find min in each stage
        minRow = group2.loc[group2["duration"].idxmin()]
        assert isinstance(minRow, pd.Series)

        dataset = np.append(dataset,minRow["duration"])
        colors.append(color_dict[minRow["usedCores"] / 4])
        
        x =[32,16,8,4,2, 'bestfit']
        xs = range(len(x))
        y = dataset
        
        
        barlist = plt.bar(xs, y, bottom= previous_values, color='black', width=0.5, linewidth=2, edgecolor='black')
        i = 0
        for bar in barlist:
            bar.set_facecolor(colors[i])
            bar.set_linewidth(0.7)
            bar.set_edgecolor(util.color_stage_border)
            i = i +1 
        
        plt.xticks(xs, x)
        previous_values = np.array(previous_values) + np.array(dataset)
    default_patch = mpatches.Patch(color='black', label='Default')
    static_patch = mpatches.Patch(color='r', label='Static')
    legend = plt.legend(handles=[c32_patch, c16_patch, c8_patch, c4_patch, c2_patch], frameon=1)
    frame = legend.get_frame()
    frame.set_facecolor('lightgrey')
    plt.xlabel(formatLabelForLatex("Number of Threads"))
    plt.ylabel(formatLabelForLatex("Runtime (s)"))
#     /Users/sobhan/scala-ide-workspace-spark/spark/publication/Big Data Paradigm/img
    savefig(f"{outputFilePath}/dca_static_ssd_{name}.pdf",dpi=100, bbox_inches='tight')

plt.show()
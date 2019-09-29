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

from argparse import ArgumentParser
parser = ArgumentParser()
parser.add_argument("-i", "--inputFile")
parser.add_argument("-o", "--outputFilePath")
parser.add_argument("-d", "--diskType")

args = parser.parse_args(sys.argv[1:])

inputFileName = args.inputFile
outputFilePath = args.outputFilePath
diskType = args.diskType

import util
from util import *
setup(util.setting_font_size_2, util.style_plot);

def normalise (x, min, max):
    return (x - min) / (max - min)


def get_percentage_change(current, previous):
    if current == previous:
        return 0
    try:
        return round (((current - previous) / previous) * 100.0 , 2) 
    except ZeroDivisionError:
        return 0


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

ioStageDict = {
    "terasort" : [0, 1, 2],
    "pagerank": [0, 5],
    "join": [0, 1],
    "aggregation": [0]
    
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
    32 : my_colors[0],
    16 : my_colors[1],
    8 : my_colors[2],
    4 : my_colors[3],
    2 : my_colors[4],
    }

default_patch = mpatches.Patch(color=util.color_default, label='default (32 cores)')
bestStatic_patch = mpatches.Patch(color=util.color_static_bestfit, label='static-bestfit')
dynamic_patch = mpatches.Patch(color=util.color_dynamic, label='dynamic')

def getMeanAndStdAndTransformBack(df, name):
    mean = df.groupby(["stage", "usedCores"]).mean()
    std = df.groupby(["stage", "usedCores"]).std()
    mean = mean.reset_index()
    std = std.reset_index()
    mean['appName'] = name
    std['appName'] = name
    return mean, std

def findBestStatic(group, name):
    maxUsedCores = group["totalCores"].max()
    mean_bestStatic = group.loc[group["totalCores"] != maxUsedCores]
    mean_bestStatic = mean_bestStatic.loc[mean_bestStatic["adaptive"] == 100]
    mean_bestStatic, std_bestStatic = getMeanAndStdAndTransformBack(mean_bestStatic, name)
    
    # Find the best static duration
    mean_bestStatic_final = pd.DataFrame(columns=mean_bestStatic.columns)
    
    std_bestStatic_array = []
    for stage, group2 in mean_bestStatic.groupby(["stage"]):
        std_for_cores = []
        if (group2['isIoStage'].iloc[0] == 1):
            # IO STAGE, find the minimum static duration
            minRow = group2.loc[group2["duration"].idxmin()]
            std_for_cores = std_bestStatic.loc[std_bestStatic["totalCores"] == minRow["totalCores"]] 
   
        else:
            minRow = (mean_default.loc[mean_default["stage"] == stage].iloc[0])
            std_for_cores = std_default.loc[std_default["totalCores"] == minRow["totalCores"]] 
        minRow["adaptive"] = 999
        mean_bestStatic_final = mean_bestStatic_final.append(minRow)
        std_for_cores_and_stage = std_for_cores.loc[std_for_cores["stage"] == stage]
        std_bestStatic_array.append(std_for_cores_and_stage["duration"].values[0])
        
        # Distinguish best static records with 999 adaptive
    mean_bestStatic_duration = mean_bestStatic_final['duration'].sum()
    print(f"BestStatic duration: {mean_bestStatic_duration}")
    return mean_bestStatic_final, std_bestStatic_array, mean_bestStatic_duration

def findBestFitStatic(group, name):
    maxUsedCores = group["totalCores"].max()
    print(maxUsedCores)
    # Filter out default rows
    mean_bestStatic = group.loc[group["usedCores"] != maxUsedCores]
    # Filter out dynamic rows
    mean_bestStatic = mean_bestStatic.loc[mean_bestStatic["adaptive"] == 100]
    mean_bestStatic, std_bestStatic = getMeanAndStdAndTransformBack(mean_bestStatic, name)
    
    # In BestFit we also consider the default behaviour:
    mean_bestStatic = pd.concat([mean_bestStatic, mean_default]).reset_index(drop=True)
    
    # Find the best static duration
    mean_bestStatic_final = pd.DataFrame(columns=mean_bestStatic.columns)
    
    std_bestStatic_array = []
    for stage, group2 in mean_bestStatic.groupby(["stage"]):
        std_for_cores = []
        if (group2['isIoStage'].iloc[0] == 1):
            # IO STAGE, find the minimum static duration
            minRow = group2.loc[group2["duration"].idxmin()] 
            print(f"min row for stage {stage}:")
            print(minRow)
            if(minRow["usedCores"] == maxUsedCores):
                std_for_cores = std_default.loc[std_default["usedCores"] == minRow["usedCores"]] 
            else:
                std_for_cores = std_bestStatic.loc[std_bestStatic["usedCores"] == minRow["usedCores"]] 
        else:
            minRow = (mean_default.loc[mean_default["stage"] == stage].iloc[0])
            std_for_cores = std_default.loc[std_default["usedCores"] == minRow["usedCores"]] 
        minRow["adaptive"] = 999
        mean_bestStatic_final = mean_bestStatic_final.append(minRow)
        std_for_cores_and_stage = std_for_cores.loc[std_for_cores["stage"] == stage]
        std_bestStatic_array.append(std_for_cores_and_stage["duration"].values[0])
        
        # Distinguish best static records with 999 adaptive
    mean_bestStatic_duration = mean_bestStatic_final['duration'].sum()
    print(f"BestStatic duration: {mean_bestStatic_duration}")
    return mean_bestStatic_final, std_bestStatic_array, mean_bestStatic_duration

def findDynamic(group, name):
    mean_dynamic = group.loc[group["adaptive"] == 14 ]
    if(mean_dynamic.empty):
        raise ValueError(f'App [{name}] has not been run with the dynamic solution!')
    else:
        mean_dynamic, std_dynamic = getMeanAndStdAndTransformBack(mean_dynamic, name)
        
    # Find the default duration
    mean_dynamic_duration = mean_dynamic['duration'].sum()
    print(f"Dynamic duration: {mean_dynamic_duration}")
    
    return mean_dynamic, std_dynamic, mean_dynamic_duration 
    
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
def round_up_to_even(f):
    return math.ceil(f / 2.) * 2

# data/dca/dca_dynamic.csv
data = pd.read_csv(inputFileName, comment = '#', dtype={
    'stage': int,
    'duration': float,
    'usedCores': int,
    'totalCores': int,
    'adaptive': int
    })
data['appName'] = data['appName'].apply (lambda x: x.split("^", 1)[0])
data['appName'] = data['appName'].apply (lambda x: re.sub(r"-[0-9]+c-", '-', x))
data['appName'] = data['appName'].apply (lambda x: re.sub(r"-[0-9]+a-", '-', x))
data['appName'] = data['appName'].apply (lambda x: re.sub(r"-[0-9]+us-", '-', x))
data.replace(['static\d*-'],  [''], regex=True, inplace=True)

# Add isIo columns
data['isIoStage'] = 0
for name, row in data.iterrows():
    for appName, ioStages in ioStageDict.items():
        if appName in row["appName"]:
            if (row["stage"] in ioStages):
                data.loc[name, "isIoStage"] = 1
                break

data["duration"] = data["duration"].apply(lambda s: s / 1000)
appCount = len(data["appName"].unique().tolist())
# data = data.sort_values(["appName", "totalCores", "adaptive"])
print("All data: ")
print(data)

dfgroup = data.groupby(["appName"], sort=False)
i = 0
numNodes = 4
maxCores = 32
# totalMaxCores = numNodes * maxCores
numberOfRows = math.ceil(appCount / 2)
for name, group in dfgroup:
    maxUsedCores = group["totalCores"].max()
    fig, ax = plt.subplots()
    
    # Find mean for the default
    mean_default = group.loc[group["usedCores"] == maxUsedCores]
    mean_default = mean_default.loc[mean_default["adaptive"] == 100]
    mean_default, std_default = getMeanAndStdAndTransformBack(mean_default, name)
    
    # Find the default duration
    print(mean_default)
    default_duration = mean_default['duration'].sum()
    print(f"Default duration: {default_duration}")
    # ====================================================
    
    # Find mean for the best static
    # ====================================================
    print("Finding best static...")
    mean_bestStatic_final, std_bestStatic_array, bestStatic_duration = findBestFitStatic(group, name)
    print("mean_bestStatic_final")
    print(mean_bestStatic_final)
    # ====================================================
    
    # Find mean for the dynamic
    # ====================================================
    mean_dynamic, std_dynamic, dynamic_duration = findDynamic(group, name)
    print("mean_dynamic")
    print(mean_dynamic)
    # ====================================================
    
    print()
    print("===========================")
    print(f"Application: {name}")
    bestStatic_speedup = get_percentage_change(bestStatic_duration, default_duration)
    print(f"From default [{default_duration}] to bestfit static [{bestStatic_duration}]: {bestStatic_speedup}%")
    dynamic_speedup = get_percentage_change(dynamic_duration, default_duration)
    print(f"From default [{default_duration}] to dynamic [{dynamic_duration}]: {dynamic_speedup}%")
    dynamic_to_bestStatic_speedup = get_percentage_change(dynamic_duration, bestStatic_duration)
    print(f"From bestfit static [{bestStatic_duration}] to dynamic [{dynamic_duration}]: {dynamic_to_bestStatic_speedup}%")
    print("===========================")
    
    maximumDuration = max([default_duration, bestStatic_duration, dynamic_duration])
    print(f"Maximum duration: {maximumDuration}")
    
    mean_all = pd.concat([mean_default, mean_bestStatic_final , mean_dynamic])
    
    x = [formatLabelForLatex("default"), formatLabelForLatex("static-bestfit"), formatLabelForLatex("dynamic")]
    previous_values = np.array(len(x))
    stage_index = 0
    visited_patches = []
    for name2, group2 in mean_all.groupby(["stage"]):
            # Determine bar colors based on the method
            colors = []
            for namerow, row in group2.iterrows():
                if row['adaptive'] == 100:
                    colors.append(util.color_default)
                elif row['adaptive'] == 999:
                    colors.append(util.color_static_bestfit)
                elif row['adaptive'] == 1000 or  row['adaptive'] == 14:
                    colors.append(util.color_dynamic)
                else:
                    colors.append(my_colors[1])
                
            dataset = group2["duration"].values
            
            xs = range(len(x))
            y = dataset
            
            std_for_dynamic = std_dynamic.loc[std_dynamic["stage"] == name2]["duration"].values[0]
                
#             print("std_default:")    
#             print(std_default)
#             
#             print("std_bestStatic_array:")    
#             print(std_bestStatic_array)
#             
#                 
#             print("std_dynamic:")    
#             print(std_dynamic)
            
            y_err = [std_default.loc[std_default["stage"] == name2]["duration"].values[0],
                     std_bestStatic_array[stage_index],
                     std_for_dynamic
                     ]
            stage_index = stage_index + 1
            
            err_size = dict(lw=2, capsize=2.5, capthick=1)
            barlist = plt.bar(xs, y, bottom=previous_values, width=0.35, linewidth=2, edgecolor='black', yerr=y_err, error_kw=err_size)
            i = 0
            for bar in barlist:
                bar.set_facecolor(colors[i])
                bar.set_linewidth(0.7)
                bar.set_edgecolor(util.color_stage_border)
                i = i + 1 
            
            i = 0   
            stage = name2 
            for p in ax.patches:
                if (i in visited_patches):
                    i = i + 1
                    continue
                
                changed_i = i
                if (stage != 0):
                    max_visited = 0
                    if (len(visited_patches) != 0):
                        max_visited = max(visited_patches)
                    changed_i = i % 3
                
                width, height = p.get_width(), p.get_height()

                x_cord = p.get_x()
                y_cord = p.get_y()
                
                usedCores = 0
                if changed_i == 0:
                    usedCores = maxUsedCores
                elif changed_i == 1:
                    # best static
                    isIoStage = group2.loc[group2["adaptive"] == 999]["isIoStage"].values[0] == 1
                    if isIoStage == False:
                        usedCores = maxUsedCores
                    else:
                        usedCores = group2.loc[group2["adaptive"] == 999]["usedCores"].values[0]
                elif changed_i == 2:
                    usedCores = group2.loc[group2["adaptive"] == 14]["usedCores"].values[0]
                    
                print(f"Annotating usedCores: {usedCores} & height: {height}")    
                    
                visited_patches.append(i)
                annotate_color = "whitesmoke"
                minHeight = maximumDuration / 10
                if (height >= minHeight):
                    ax.annotate(makeStringBold(f'{round_up_to_even(usedCores)}/{maxUsedCores}'), (x_cord + width / 2, y_cord), ha='center', va='bottom', color=annotate_color, size=11, weight='bold')
                        
                i = i + 1
            
            plt.xticks(xs, x)
            previous_values = np.array(previous_values) + np.array(dataset)
            
    plt.ylabel(formatLabelForLatex("Runtime (s)"))
    fig.tight_layout() 
#     /Users/sobhan/scala-ide-workspace-spark/spark/publication/Big Data Paradigm/img
    outputFileName = f"dca_dynamic_{name}"
    if diskType == "ssd":
        outputFileName = f"dca_dynamic_ssd_{name}"
        
    savefig(f"{outputFilePath}/{outputFileName}.pdf",dpi=100, bbox_inches='tight')
    
plt.show()
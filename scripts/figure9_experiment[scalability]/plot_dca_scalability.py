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
parser.add_argument("-o", "--outputFileName")

args = parser.parse_args(sys.argv[1:])

inputFileName = args.inputFile
outputFileName = args.outputFileName

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


appNames = [
             'terasort-hdfs-Main-1200000000r',
             'terasort-hdfs-Main-4800000001r',
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
    32 : my_colors[0],
    16 : my_colors[1],
    8 : my_colors[2],
    4 : my_colors[3],
    2 : my_colors[4],
    }

default_16_patch = mpatches.Patch(color=util.color_default, label='default (16 nodes)')
bestStatic_16_patch = mpatches.Patch(color=util.color_static_bestfit, label='static-bestfit (16 nodes)')
dynamic_16_patch = mpatches.Patch(color=util.color_dynamic, label='dynamic (16 nodes)')

none_patch = mpatches.Patch(color=util.color_dynamic, label='', alpha=0)

default_4_patch = mpatches.Patch(color=util.color_default, label='default (4 nodes)', alpha = 0.4)
bestStatic_4_patch = mpatches.Patch(color=util.color_static_bestfit, label='static-bestfit (4 nodes)', alpha = 0.4)
dynamic_4_patch = mpatches.Patch(color=util.color_dynamic, label='dynamic (4 nodes)', alpha = 0.4)
    
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

def getColor(name, adaptive):
    if adaptive == 100:
        return util.color_default
    elif adaptive == 999:
        return util.color_static_bestfit
    elif adaptive == 1000 or  adaptive == 14:
        return util.color_dynamic
    else:
        return my_colors[1]

# data/dca/dca_dynamic_scalability.csv
data = pd.read_csv(inputFileName, comment='#', dtype={
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

nodes_4_yerr = {
    0 : [1.860250, 0.7896021149920945, 2.832190],
    1 : [5.275267, 3.5685929019713116, 4.195378],
    2 : [95.110029, 26.179360481111807, 101.735298]
    }


data["duration"] = data["duration"].apply(lambda s: s / 1000)
appCount = len(data["appName"].unique().tolist())
# data = data.sort_values(["appName", "totalCores", "adaptive"])
print(data)

dfgroup = data.groupby(["appName"], sort=False)
i = 0
numNodes = 4
numberOfRows = math.ceil(appCount / 2)
print(f"numberOfRows- {numberOfRows}") 
fig, ax = plt.subplots()
_width = -0.4;
visited_patches = []
for name, group in dfgroup:
    maxUsedCores = group["totalCores"].max()
    print(f"name- {name}") 
    print("group:")
    print(group) 

    x = [formatLabelForLatex("default"), formatLabelForLatex("static-bestfit"), formatLabelForLatex("dynamic")]
    previous_values = np.array(len(x))
    stage_index = 0
    alpha = 1
    if "1200000000r" in name:
        alpha =0.4
    
    _width += 0.35;
    for name2, group2 in group.groupby(["stage"]):
            colors = []
            for namerow, row in group2.iterrows():
                colors.append(getColor(name, row['adaptive']))

            dataset = group2["duration"].values
            xs = range(len(x))
            xs = np.arange(len(x))
            y = dataset

            stage_index = stage_index + 1
            print("previous_values:")
            print(previous_values)
            err_size = dict(lw=2, capsize=2.5, capthick=1)
            if "1200000000r" in name:
                y_err = nodes_4_yerr[name2]
            else:
                y_err = [0, 0, 0]
            
            barlist = plt.bar(xs + _width, y, bottom=previous_values, width=0.35, linewidth=2, edgecolor='black', error_kw=err_size, alpha=alpha, yerr = y_err)
            
            i = 0
            for bar in barlist:
                bar.set_facecolor(colors[i])
                bar.set_linewidth(0.7)
                bar.set_edgecolor(util.color_stage_border)
                i = i + 1 
            
            i = 0   
            stage = name2 
            for p in ax.patches:
#                 print("")
#                 print(f"visited_patches = {visited_patches}")
                if (i in visited_patches):
                    print(f"already visited patch {i}")
                    i = i + 1
                    continue
                
                changed_i = i
                if (stage != 0):
                    max_visited = 0
                    if (len(visited_patches) != 0):
                        max_visited = max(visited_patches)
#                         print(max_visited)
                    changed_i = i % 3
                
                width, height = p.get_width(), p.get_height()
                x_cord = p.get_x()
                y_cord = p.get_y()
                
                usedCores = 0
                if changed_i % 3 == 0:
                    usedCores = maxUsedCores
                elif changed_i % 3 == 1:
                    # best static
                    usedCores = group2.loc[group2["adaptive"] == 999]["usedCores"].values[0]
                elif changed_i %3 == 2:
                    usedCores = group2.loc[group2["adaptive"] == 14]["usedCores"].values[0]
                    
                visited_patches.append(i)
                annotate_color = "whitesmoke"
                if (height > 42):
                    print(f"annotating patch {i}")    
                    ax.annotate(makeStringBold(f'{round_up_to_even(usedCores)}/{maxUsedCores}'), (x_cord + width / 2, y_cord), ha='center', va='bottom', color=annotate_color, size=8, weight='bold')    
                i = i + 1
            
            plt.xticks(xs, x)
            previous_values = np.array(previous_values) + np.array(dataset)
    plt.ylabel(formatLabelForLatex("Runtime (s)"))


    
plt.legend(handles=[default_16_patch, bestStatic_16_patch, dynamic_16_patch,  none_patch, default_4_patch, bestStatic_4_patch, dynamic_4_patch])
fig.tight_layout() 
# /Users/sobhan/scala-ide-workspace-spark/spark/publication/Big Data Paradigm/img/dca_dynamic_scalability.pdf
savefig(outputFileName, dpi=100, bbox_inches='tight')
plt.show()

import matplotlib.pyplot as plt
from matplotlib.pyplot import  savefig
import pandas as pd
import numpy as np
from numpy import dtype
from matplotlib.pyplot import ylabel
from itertools import cycle, islice
import re
import matplotlib.ticker as ticker
import math

import sys
sys.path.append(f'./common')

import util
from util import *
setup(util.setting_font_size_1, util.style_plot);



from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-i", "--inputFile")
parser.add_argument("-o", "--outputFile")

args = parser.parse_args(sys.argv[1:])

inputFileName = args.inputFile
outputFileName = args.outputFile

def extractNumberOfCores(appName):
    result = None
    matches = re.findall(r"[0-9]+c", appName)
    for matchNum, match in enumerate(matches):
        numberMatches = re.findall("[0-9]+", match) 
        for mNum, m in enumerate(numberMatches):
            result = m
    return result

# "data/dca/dca_scatter_plot_terasort.csv"
data = pd.read_csv(inputFileName)
col_names = ["executorId", "stage", "selection"]
result_df = pd.DataFrame(columns = col_names)

data_grouped = data.groupby(["stage", "executorId"])
for name, group in data_grouped:
    stage = name[0]
    executorId = name[1]
    print(f"name: {name}")
    print(f"stage: {stage}")
    print(f"executorId: {executorId}")
    print(f"group: {group}")
    maxRow = group.loc[group["selection"].idxmax()]
    selection = maxRow["selection"]
    print(selection)
    result_df = result_df.append([{'executorId':executorId, 'stage': stage, 'selection': selection}], ignore_index=True)

offset = -0.2    
index = []
for i in range(0, 4):
    y = result_df.loc[result_df["executorId"] == i]["selection"].values
    index = np.arange(len(y))
    y_index = np.arange(1,6)
    x = 2**y_index
    bar_width = 0.1
    plt.xticks([0,1,2], [0,1,2])
    plt.ylim([0,32])
    yticks_p2 = [ 2**j for j in range(1,6) ]
    yticks_p2 = [0, 2, 4, 8, 16, 32]
    ax = plt.gca()
    yticks = np.arange(6)
    ax.set_yticks(yticks)
    ax.yaxis.set_ticklabels(yticks_p2)

    ax.set_ylabel(formatLabelForLatex("Number of Threads"))
    ax.set_xlabel(formatLabelForLatex("Stages"))
    
    y = [math.log2(x) for x in y] 
    ax.set_ylim([0,5])

    plt.bar(index + offset, y, width = bar_width, label = f"executor {i}", align='center', color = util.colors_viridis[i+1])
    offset = offset + bar_width
    
plt.legend()
# f"/Users/sobhan/scala-ide-workspace-spark/spark/publication/Big Data Paradigm/img/dca_core_plot.pdf"
savefig(outputFileName, dpi=100, bbox_inches='tight')
plt.show()


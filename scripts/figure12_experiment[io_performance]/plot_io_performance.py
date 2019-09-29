import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.pyplot import  savefig
import numpy as np
from numpy import dtype
from matplotlib.pyplot import ylabel
from itertools import cycle, islice
import re
import matplotlib.patches as mpatches
import matplotlib.collections as collections
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
setup(util.setting_font_size_1, 'ggplot');

vals = [2,4,8,16,32]
colors = [plt.cm.Spectral(i/float(len(vals)-1)) for i in range(len(vals))]
print(colors)
my_colors = [
    util.color_default,
    util.color_16c,
    util.color_8c,
     util.color_4c,
    util.color_2c,
    ]

color_dict = {
    32 : my_colors[0],
    16 : my_colors[1],
    8 : my_colors[2],
    4 : my_colors[3],
    2 : my_colors[4],
    }

c32_patch = mpatches.Patch(color=my_colors[0], label='Default (32C)')
c16_patch = mpatches.Patch(color=my_colors[1], label='16C')
c8_patch = mpatches.Patch(color=my_colors[2], label='8C')
c4_patch = mpatches.Patch(color=my_colors[3], label='4C')
c2_patch = mpatches.Patch(color=my_colors[4], label='2C')

from matplotlib.legend_handler import HandlerBase


class AnyObjectHandler(HandlerBase):
    def create_artists(self, legend, orig_handle,
                       x0, y0, width, height, fontsize, trans):
        l1 = plt.Line2D([x0,y0+width], [1.2*height,1.2*height],
                           linestyle="--", color=orig_handle[0])
        l2 = plt.Line2D([x0,y0+width], [0.7*height,0.7*height], 
                           linestyle="--", color=orig_handle[1])
        l3 = plt.Line2D([x0,y0+width], [0.3*height,0.3*height], 
                           linestyle="--", color=orig_handle[2])
        l4 = plt.Line2D([x0,y0+width], [0*height,0*height], 
                           linestyle="--", color=orig_handle[3])
        l5 = plt.Line2D([x0,y0+width], [-0.5*height,-.5*height], 
                           linestyle="--", color=orig_handle[4])
        return [l1, l2,l3,l4, l5]


def extractNumberOfCores(appName):
    result = None
    matches = re.findall(r"[0-9]+c", appName)
    for matchNum, match in enumerate(matches):
        numberMatches = re.findall("[0-9]+", match) 
        for mNum, m in enumerate(numberMatches):
            result = m
    return result

def processFile(fileName):
    data = pd.read_csv(fileName)
    
    data["appName"] = data['appName'].apply(lambda x: x.split("^", 1)[0])
    data = data[data.rkBPerS != 0]
    data = data.set_index("appName")
    data['readAndWriteKbPerS'] = data['rkBPerS'] + data['wkBPerS']
    
    
    data_grouped_by_stage = data.groupby(["stage"])
    for stage, group_by_stage in data_grouped_by_stage:
        print(f"Processing stage: {stage}")
        fig = plt.figure(figsize=(6,5))
        grouped_by_appName = group_by_stage.groupby(["appName"])
        for appName, group_by_name in grouped_by_appName:
            cores = int(extractNumberOfCores(appName))
            color = color_dict[cores]
            print(f"Processing cores: {cores} => color: {color}")
        
            features_of_interest = group_by_name[['readAndWriteKbPerS']]
            mean = features_of_interest['readAndWriteKbPerS'].mean()
            x = np.arange(0, len(group_by_name.index))
            
            p = plt.plot(x, features_of_interest['readAndWriteKbPerS'], label=cores, color=color, alpha=0.3)
            meanList = [mean]*len(x)
            mean_line = plt.plot(x, meanList, linestyle='--', color=color, linewidth=2)
            
        colors = list(color_dict.values())
        plt.legend([tuple(colors), c32_patch, c16_patch, c8_patch, c4_patch, c2_patch], ['Mean', "32", "16", "8", "4", "2"],
               handler_map={tuple: AnyObjectHandler()}, loc="lower right")
        plt.xlabel(formatLabelForLatex("Time (s)"))
        plt.ylabel(formatLabelForLatex("I/O Throughput (KB/s)"))
#         "/Users/sobhan/scala-ide-workspace-spark/spark/publication/Big Data Paradigm/img
        savefig(f"{outputFilePath}/iostat_curve_{diskType}_terasort_s{stage}.pdf", dpi=100, bbox_inches='tight')

#"data/iostat_m/iostat_curve_ssd_terasort.csv"
#"data/iostat_m/iostat_curve_disk_terasort.csv"
processFile(inputFileName)
plt.show()
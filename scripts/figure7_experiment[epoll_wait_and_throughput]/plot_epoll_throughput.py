import matplotlib.pyplot as plt
from matplotlib.pyplot import  savefig
import matplotlib.patches as mpatches
import pandas as pd
import numpy as np
from numpy import dtype
from matplotlib.pyplot import ylabel
import math
import mpl_toolkits.axisartist as AA
from mpl_toolkits.axes_grid1 import host_subplot
from matplotlib import rc

import sys
sys.path.append(f'./common')
import util
from util import *
setup(util.setting_font_size_1, util.style_plot);
# plt.rc('axes', labelsize=11) # fontsize of the x and y labels

from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-i", "--inputFile")
parser.add_argument("-o", "--outputFilePath")

args = parser.parse_args(sys.argv[1:])

inputFileName = args.inputFile
outputFilePath = args.outputFilePath

# data/epoll/epoll_terasort_new.csv
data = pd.read_csv(inputFileName)

data = data.loc[data["executorId"] == 3]
mean = data.groupby(["stage", "cores"]).mean()
mean = mean.reset_index()
mean = mean.groupby(["stage"])
for name, group in mean:
    fig, ax1 = plt.subplots()
    plt.rcParams["figure.figsize"] = (6.4,4.8)
    ax2 = ax1.twinx()
    ax3 = ax1.twinx()
    ax3.spines['right'].set_position(('outward', 60)) 

    epolls = group["totallEpollWait"].values
    throughputs = group["totalTaskBothThroughputFromSampling"].values
    normalised = group["normalisedByTotalTaskThroughputFromSampling"].values
    x =[2, 4, 8, 16, 32]
    xs = range(len(x))
    
    lns1 = ax1.plot(xs, epolls, color=util.color_epoll_wait_time , marker='o', label=f"epoll wait time ($\epsilon$)")
    lns2 = ax2.plot(xs, throughputs, color=util.color_io_throughput, marker='s', label = "I/O throughput ($\mu$)")
    lns3 = ax3.plot(xs, normalised, color=util.color_combined , marker='d', label="congestion index ($\zeta$)")
    
    # added these three lines
    ax1.set_ylabel(formatLabelForLatex('epoll wait time (s)'), color=util.color_epoll_wait_time)
    ax2.set_ylabel(formatLabelForLatex('I/O throughput (MB/s)'), color=util.color_io_throughput)
    ax3.set_ylabel(formatLabelForLatex('congestion index'), color=util.color_combined)
    plt.xticks(xs, x)
    plt.xlabel("Number of Threads")

    # Find selection
    maxRow = group.loc[group["cores"].idxmax()]
    selection_x = int(maxRow["selection"])
    selection_y = group.loc[group["cores"] == selection_x]["normalisedByTotalTaskThroughputFromSampling"].values

    arrow_properties = dict(
                            facecolor="black", width=0.5,
                            headwidth=6, shrink=0.1)
    yax = ax1.get_yaxis()
    yticks = yax.get_major_ticks()
    lastTick = yticks[-1]
    lastTickVal = lastTick.get_view_interval()[1]
    firstTickVal = ax1.get_ylim()[0]
    
    selection_y = selection_y + (firstTickVal  + 50 )
    arrow_start_y = selection_y + (lastTickVal / 3.5)

    selection_x = math.log2(selection_x) - 1
    print(f"lastTickVal y: {lastTickVal}")
    print(f"Selection x: {selection_x}")
    print(f"Selection y: {selection_y}")
    print(f"arrow_start_y : {arrow_start_y}")
    print(f"ax_xlim : {ax1.get_xlim()}")
    print(f"ax_ylim : {ax1.get_ylim()}")
    
    ax1.annotate(
        makeStringBold("Selected"), xy=(selection_x, selection_y),
        xytext=(selection_x, arrow_start_y),
        arrowprops=arrow_properties, fontsize=11)
    
    lns = lns1+lns2+lns3
    ax1.legend(handles=lns, bbox_to_anchor=(1,0), loc="lower right",  bbox_transform=fig.transFigure, fontsize=10) 
    ax1.set_xlabel(formatLabelForLatex("Number of Threads"))
    fig.tight_layout()
#     /Users/sobhan/scala-ide-workspace-spark/spark/publication/Big Data Paradigm/img
    savefig(f"{outputFilePath}/epoll_stage{name}.pdf", dpi=100, bbox_inches='tight')

plt.show()
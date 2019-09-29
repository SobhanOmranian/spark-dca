import matplotlib.pyplot as plt
from matplotlib.pyplot import  savefig
import pandas as pd
import numpy as np
from numpy import dtype
from matplotlib.pyplot import ylabel
from itertools import cycle, islice
import re
import sys

sys.path.append(f'./common')
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-i", "--inputFile")
parser.add_argument("-o", "--outputFilePath")

args = parser.parse_args(sys.argv[1:])

inputFileName = args.inputFile
outputFilePath = args.outputFilePath

import util
from util import *
setup(util.setting_font_size_3, util.style_plot);

def extractNumberOfCores(appName):
    result = None
    matches = re.findall(r"[0-9]+c", appName)
    for matchNum, match in enumerate(matches):
        numberMatches = re.findall("[0-9]+", match) 
        for mNum, m in enumerate(numberMatches):
            result = m
    return result

def getColors(x_index, util_df):
    colors = ["grey"] * len(x_index)
    maxRow = util_df.loc[util_df["util"].idxmax()]
    maxUtilCores = int(maxRow.cores)
    maxUtilCoresIndex = x_index.index(maxUtilCores)
    
    colors[maxUtilCoresIndex] = "r"
    
    return colors
def plotMeanAndStd(data, stage):
    data = data.reset_index()
    data["appName"] = data['appName'].apply(lambda x: x.split("^", 1)[0])
    data = data.set_index("appName")
    data = data.sort_index(ascending=False)
    data_grouped = data.groupby(["appName"], sort=False)
    col_names = ["cores", "util", "std"]
    util_df = pd.DataFrame(columns=col_names)
    for name, group in data_grouped:
        print(f"name: {name}")
        group["util"] = group["util"].apply(lambda x: x / 2)
        group_mean = group.groupby(["appName", "stage"]).mean()
        group_std = group.groupby(["appName", "stage"]).std() 

        group_mean = group_mean.reset_index().set_index(["appName"])
        group_std = group_std.reset_index().set_index(["appName"])
        group_mean = group_mean.loc[group_mean["stage"] == stage]
        group_std = group_std.loc[group_std["stage"] == stage]
        features_of_interest = group_mean[['util']]
        mean = features_of_interest["util"].values[0]
        std = group_std["util"].values[0]
        cores = extractNumberOfCores(name)
        
        util_df.loc[len(util_df)] = [int(cores), mean, std]
        
    util_df = util_df.sort_values(["cores"], ascending=False)
    print(util_df)
    x =[32,16,8,4,2]
    
    xs = range(len(x))
    y = util_df["util"].values
    yerr = util_df["std"].values
    colors = getColors(x, util_df)
    
    ax = plt.gca()
    ax.set_ylim(0,100)
    plt.xticks(xs, x)
    err_size = dict(lw=2, capsize=2.5, capthick=1)
    barlist = plt.bar(xs, y, width=0.5, color = colors, yerr=yerr, error_kw=err_size)
    plt.xlabel(makeStringBold("Number of Threads"))
    plt.ylabel(makeStringBold("Average Disk Utilization (\%)"))
    plt.tight_layout()
    

def plot(fileName, appName, outputFileName, stage):
    data = pd.read_csv(fileName)
    
    # Filter data based on the appName since we've merged all the applications in a single fine
    data = data[data['appName'].str.contains(appName)]
    print(data)
    
    if data.empty:
        return
    
    data = data.set_index("appName")
    data = data.sort_index(ascending=False)
    data_grouped_by_instances = data.groupby(["appName"], sort=False)

    mean_df_for_each_app = pd.DataFrame()
    for name, group in data_grouped_by_instances:
        group_mean = group.groupby(["appName", "stage"]).mean()
        group_mean = group_mean.reset_index().set_index(["appName"])
        mean_df_for_each_app = pd.concat([mean_df_for_each_app, group_mean])
        
    fig = plt.figure(figsize=(6,5))
    plotMeanAndStd(mean_df_for_each_app, stage)
#     /Users/sobhan/scala-ide-workspace-spark/spark/publication/Big Data Paradigm/img
    savefig(f"{outputFilePath}/disk_util_{outputFileName}.pdf",dpi=100, bbox_inches='tight')

# "data/iostat_m/all.csv"
plot(inputFileName, "join", "join", 1)
plot(inputFileName, "aggregation", "aggregation", 0)
plot(inputFileName, "pagerank", "pagerank_s0", 0)
plot(inputFileName, "pagerank", "pagerank_s5", 5)
plot(inputFileName, "terasort", "terasort_s0", 0)
plot(inputFileName, "terasort", "terasort_s1", 1)
plot(inputFileName, "terasort", "terasort_s2", 2)
# plot("data/iostat_m/iostat_curve_disk_join_all.csv", "join", "join", 1)
# plot("data/iostat_m/iostat_curve_disk_aggregation_all.csv", "aggregation", "aggregation", 0)
# plot("data/iostat_m/iostat_curve_disk_pagerank_all.csv", "pagerank", "pagerank_s0", 0)
# plot("data/iostat_m/iostat_curve_disk_pagerank_all.csv", "pagerank", "pagerank_s5", 5)
# plot("data/iostat_m/iostat_curve_disk_terasort_all.csv", "terasort", "terasort_s0", 0)
# plot("data/iostat_m/iostat_curve_disk_terasort_all.csv", "terasort", "terasort_s1", 1)
# plot("data/iostat_m/iostat_curve_disk_terasort_all.csv", "terasort", "terasort_s2", 2)
plt.show()

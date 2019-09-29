import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from numpy import dtype
from matplotlib.pyplot import ylabel
from matplotlib.cm import ScalarMappable
from matplotlib.pyplot import  savefig
import math
from getCpuUsageForStage import *
import sys

from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-i", "--inputFile")
parser.add_argument("-t", "--topFile")
parser.add_argument("-o", "--outputFile")

args = parser.parse_args(sys.argv[1:])

inputFileName = args.inputFile
topFileName = args.topFile
outputFileName = args.outputFile

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 2000)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_colwidth', 800)
plt.style.use('seaborn-ticks')
plt.rc('font', family='serif')
plt.rcParams.update({'font.size': 16})

cdict = {'red':   ((0.0,  0.0, 0.0),
                   (1.0,  1.0, 1.0)),

         'green': ((0.0,  0.0, 0.0),
                   (1,  0.0, 0.0)),

         'blue':  ((0.0,  0.0, 0.0),
                   (1,  0.0, 0.0))}

plt.register_cmap(name='BlackRed', data=cdict)


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
               'PageRank',
               'WordCount', 
               ]
appNamesDict = {
    'terasort-hdfs-Main-1200000000r' : 'Terasort'
    
    }

def normalise (x, min, max):
    return (x-min)/(max-min)
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
# data/mpstat/1/resultMpstat.csv
data = pd.read_csv(inputFileName, comment="#" ,dtype={
    'runtimeMs': int,
    'usr': float,
    'nice': float,
    'sys': float,
    'iowait': float,
    'irq': float,
    'soft': float,
    'steal': float,
    'guest': float,
    'gnice': float,
    'idle': float,
    
    })
data['appName'] = data['appName'].apply (lambda x: x.split("^", 1)[0])
maxIoWait = data.max().iowait
print(f"Maximum IO Wait Time: {maxIoWait}")
fig = plt.figure(figsize=(16,12)) # Create matplotlib figure
plt.xticks([])
plt.yticks([])
appCount = len(data["appName"].unique().tolist())

i = 0
for name, group in data.groupby(["appName"]):
#     print(f"name: {name}")
#     print(f"group: {group}")
#     exit()
    data_mean = group.groupby(["appName", "stage"]).mean()
    data_std = group.groupby(["appName", "stage"]).std()
    mean_data_df = pd.DataFrame(data_mean)
    std_data_df = pd.DataFrame(data_std)
    
    mean_usr_df = mean_data_df["usr"]
    mean_iowait_df = mean_data_df["iowait"]
    mean_duration_df = mean_data_df["duration"].apply(lambda s: s / 1000)
    
    mean_ioBound_df = mean_data_df
    mean_ioBound_df["ioBound"] = 0
    for name, row in mean_ioBound_df.iterrows():
        ioBound = row["iowait"] - row["usr"]
        mean_ioBound_df.loc[name, "ioBound"] = ioBound

    print(mean_ioBound_df)
    mean_ioBound_df = mean_ioBound_df[["ioBound"]]
    mean_ioBound_df = pd.DataFrame(mean_ioBound_df)
    mean_ioBound_array = mean_iowait_df.values
    
    my_colors = []
    for idx, val in enumerate(mean_ioBound_array):
        iowait = mean_iowait_df[[idx]].values[0]
        usr = mean_usr_df[[idx]].values[0]
        io_bound = iowait
        min = 0
        rWeight = 1
        bWeight = 1
        normalized = normalise(io_bound, 0, maxIoWait)
        my_colors.append((normalized, 0, 0))
        
    mean_duration_df = mean_duration_df.reset_index()
    mean_duration_df = mean_duration_df.set_index(["appName", "stage"]).unstack()
    
    print("mean_usr_df")
    print(mean_usr_df)
    
    numberOfRows = math.ceil(appCount) 
    print(numberOfRows)
    temp = (numberOfRows * 100) + 11 + i

    ax = fig.add_subplot(temp)
    colors = ['r', 'b', 'g']
    my_cmap = plt.get_cmap('BlackRed')
    data_color = mean_iowait_df.apply(lambda x: x / maxIoWait).values
    colors = my_cmap(data_color)
#     print(colors)
    print("mean_duration_df")
    print(mean_duration_df)
#     continue
    ax = mean_duration_df.plot(ax = ax, color=colors ,kind='barh', stacked=True, width=0.40, rot=-90, label="test", legend=False, edgecolor='white', linewidth=2, cmap = 'grayscale_map')
    
    plt.setp(ax.get_xticklabels()[0], visible=False)  

    ax.set_ylabel("")
#     ax.set_xlabel("runtime(ms)")
#     ax.set_xlabel("Time (s)")
    plt.yticks([0], [getAppNameShort(name[0])], rotation=-90, size=20)
#    
    for p in ax.patches:
        width, height = p.get_width(), p.get_height()
        if width == 0:
            continue
         
        x, y = p.get_xy() 
        print(f"width = {width}")
         
        row = mean_duration_df[mean_duration_df.apply(lambda r: (r == width).any(), axis=1)]
        print(f"Found row: {row}")
        appName = row.index[0]
        print(f"Found appName: {appName}")
        foundColumn = None
        for c in row.columns:
            newDf = row.loc[lambda row: row[c] == width]
            if(not newDf.empty):
                print(f"Found column: {c[1]}")
                foundColumn = c[1]

        
        cpuUsage = getCpuUsageForStage(topFileName, appName, foundColumn)
        print()
        print(f"cpuUsage: {cpuUsage}") 
        ax.annotate(f'{int(round(cpuUsage))}%', (p.get_x() +.02*width, 0), ha='left', va='center', color='w', rotation=-90, size=24, weight = 'bold')
    i = i + 1
    
plt.xlabel("Time (s)", size=24)
mat = np.random.random((10,10))

data_color = [0,maxIoWait]
my_cmap = plt.get_cmap('BlackRed')
sm = ScalarMappable(cmap=my_cmap, norm=plt.Normalize(0,max(data_color)))
sm.set_array([])
fig.subplots_adjust(right=0.8)
cbar_ax = fig.add_axes([0.85, 0.15, 0.05, 0.7])
cbar = plt.colorbar(sm, cax=cbar_ax)
cbar.set_ticks([0,maxIoWait])
cbar.set_ticklabels([f"0%", f"{math.floor(maxIoWait)}%"])
cbar.ax.tick_params(labelsize=24) 
cbar.set_label('Disk IO Wait', rotation=270,labelpad=25, size=24)

# /Users/sobhan/scala-ide-workspace-spark/spark/publication/Big Data Paradigm/img/iowait.pdf
savefig(outputFileName, dpi=100, bbox_inches='tight')
plt.show()


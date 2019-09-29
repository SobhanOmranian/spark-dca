import csv
import matplotlib.pyplot as plt
from matplotlib.pyplot import  savefig
import numpy as np

import sys
sys.path.append(f'./common')
import util

# plt.style.use(util.style_plot_alternative)
# plt.rcParams.update({'font.size': 12})
from util import *
setup(12, util.style_plot_alternative)

from argparse import ArgumentParser
import sys

parser = ArgumentParser()
parser.add_argument("-i", "--inputFile")
parser.add_argument("-o", "--outputFile")

args = parser.parse_args(sys.argv[1:])

inputFileName = args.inputFile
outputFileName = args.outputFile

class DasNode:

    def __init__(self, nodeName):
        self.nodeName = nodeName
        self.writeTime = 0
        self.readTime = 0
        self.nodeItems = []

    def __repr__(self):
        return f"""Node: [{self.nodeName}]
                    meanWriteTime: [{self.meanWriteTime()}],
                    meanReadTime: [{self.meanReadTime()}],
                    """
    def __str__(self):
        nodeItems = ""
        for index,r in enumerate(self.nodeItems):
            nodeItems += f"{r}\n"
        return f"""
                Node: [{self.nodeName}]
                    meanWriteTime: [{self.meanWriteTime()}],
                    meanReadTime: [{self.meanReadTime()}],
                    nodeItems: 
{nodeItems},
                    """

    def __eq__(self, other):
        return self.nodeName == other.nodeName
    
    def getTimesColumn(self, index):
        if len(self.nodeItems) == 0:
            raise Exception(f"no items found for {self.nodeName}")
        return self.column(index)
    
    def meanReadTime(self):
        if len(self.nodeItems) == 0:
            raise Exception(f"no items found for {self.nodeName}")
        readTimes = self.getTimesColumn(readTime)
        mean = np.mean(readTimes)
        return mean
    
    def stdReadTime(self):
        if len(self.nodeItems) == 0:
            raise Exception(f"no items found for {self.nodeName}")
        data = self.getTimesColumn(readTime)
        result = np.std(data)
        return result
    
    def stdWriteTime(self):
        if len(self.nodeItems) == 0:
            raise Exception(f"no items found for {self.nodeName}")
        data = self.getTimesColumn(writeTime)
        result = np.std(data)
        return result
    
    
    def meanWriteTime(self):
        if len(self.nodeItems) == 0:
            raise Exception(f"no items found for {self.nodeName}")
        writeTimes = self.getTimesColumn(writeTime)
        mean = np.mean(writeTimes)
        return mean
        
    def column(self, i):
        return [int(row[i]) for row in self.nodeItems]
        
nodeNameIndex = 0
blockSizeIndex = 1
blockCountIndex = 2
threadNum = 3
writeTime = 4
readTime = 5

def plot(fileName, figureNum, figureName):
    with open(f"{fileName}") as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        header = next(reader)
        all = list(reader)
        
        nodes = []   
        for x in range(301, 348):
            if x == 301 or x == 302 or x == 311:
                continue
            nodeName = f"node{x}"
            nodeItems = [item for item in all if item[nodeNameIndex] == nodeName]  
            node = DasNode(nodeName)
            node.nodeItems = nodeItems
            print(node)
            print("")
            nodes.append(node)
        
        bar_width = 0.25
        index = np.arange(len(nodes))
        fig = plt.figure(figureNum, figsize=(15,5))
        if figureName:
            plt.title(figureName)
        
        err_size = dict(lw=1, capsize=2, capthick=0.5)
        
        x = np.arange(0,len(nodes),1)
        
        meanWriteTimeOfEachNodes = [node.meanWriteTime() for node in nodes]
        meanWriteTimeOfAllNodes = [np.mean(meanWriteTimeOfEachNodes)]*len(x)
        rect1 = plt.bar(index, meanWriteTimeOfEachNodes, bar_width, label = 'Write Time', yerr=[node.stdWriteTime() for node in nodes],  error_kw=err_size, color=util.colors_viridis[1])
        mean_line = plt.plot(x, meanWriteTimeOfAllNodes, label='Mean Write Time', linestyle='--', color=util.colors_viridis[1], linewidth = 2)
        
        meanReadTimeOfEachNode = [node.meanReadTime() for node in nodes]
        meanReadTimeOfAllNodes = [np.mean(meanReadTimeOfEachNode)]*len(x)
        rect1 = plt.bar(index + (bar_width), meanReadTimeOfEachNode, bar_width, label = 'Read Time', yerr=[node.stdReadTime() for node in nodes],  error_kw=err_size, color=util.colors_viridis[2])
        mean_line = plt.plot(x, meanReadTimeOfAllNodes, label='Mean Read Time', linestyle='--', color=util.colors_viridis[2], linewidth = 2)
        
#         rect1 = plt.bar(index, [sockets[0].l1DcacheLoads, sockets[1].l1DcacheLoads], bar_width, color='g', label="L1Loads")    
#         rect1 = plt.bar(index + (bar_width * 1), [sockets[0].l1DcacheMisses, sockets[1].l1DcacheMisses], bar_width, color='b', label="L1LoadMisses")  
#         rect1 = plt.bar(index + (bar_width * 2), [sockets[0].l1DcacheStores, sockets[1].l1DcacheStores], bar_width, color='r', label="L1Stores")  
#         rect1 = plt.bar(index + (bar_width*3), [sockets[0].l1DcacheStoresMisses, sockets[1].l1DcacheStoresMisses], bar_width, color='tab:orange', label="L1StoreMisses")  
#         rect1 = plt.bar(index + (bar_width*4), [sockets[0].l1DcachePrefetches, sockets[1].l1DcachePrefetches], bar_width, color='tab:purple', label="L1Prefetches")  
#         rect1 = plt.bar(index + (bar_width*5), [sockets[0].l1DcachePrefetchMisses, sockets[1].l1DcachePrefetchMisses], bar_width, color='tab:pink', label="L1PrefetchMisses")
#           
#         rect1 = plt.bar(index + (bar_width * 6), [sockets[0].uopsRetiredAllLoads, sockets[1].uopsRetiredAllLoads], bar_width, color='g', label="RetiredAllLoads")  
#         rect1 = plt.bar(index + (bar_width * 7), [sockets[0].uopsRetiredL1Hit, sockets[1].uopsRetiredL1Hit], bar_width, color='b', label="RetiredL1Hit")  
#         rect1 = plt.bar(index + (bar_width*8), [sockets[0].uopsRetiredL1Miss, sockets[1].uopsRetiredL1Miss], bar_width, color='r', label="RetiredL1Miss")  
#         rect1 = plt.bar(index + (bar_width*9), [sockets[0].uopsRetiredL2Hit, sockets[1].uopsRetiredL2Hit], bar_width, color='tab:orange', label="RetiredL2Hit")
#         rect1 = plt.bar(index + (bar_width*10), [sockets[0].uopsRetiredL2Miss, sockets[1].uopsRetiredL2Miss], bar_width, color='tab:purple', label="RetiredL2Miss")

        plt.xticks(index, [node.nodeName for node in nodes], rotation=70)
        plt.legend(bbox_to_anchor=(1,1), loc="upper right",  bbox_transform=fig.transFigure) 
        plt.xlabel(formatLabelForLatex("Nodes"))
        plt.ylabel(formatLabelForLatex("Time (s)"))

    
# plot("data/das-io-test/all", 1, "IO Performance of DAS Nodes")
# "data/das-io-test/odirect"
plot(inputFileName, 1, "")
# "/Users/sobhan/scala-ide-workspace-spark/spark/publication/Big Data Paradigm/img/das_io_experiment.pdf"
savefig(outputFileName, dpi=100, bbox_inches='tight')
plt.show()

import csv
import numpy as np
import re


class Experiment:

    def __init__(self, id, name, runs):
        self.id = int(id)
        self.name = name
        self.runs = runs
        self.nodeCount = self.findNumberOfNodes()
        self.memory = self.findMemory()
        self.coresPerExecutor = self.findCoresPerExecutor()

    def __str__(self):
        runs = ""
        for index,r in enumerate(self.runs):
            runs += f"Run [{index}]: {r}s\n"
        return f"""
        Experiment: [{self.id}]
                    Name: [{self.name}],
                    Number of Nodes: [{self.nodeCount}],
                    Memory: [{self.memory}],
                    Number of cores per executor: [{self.coresPerExecutor}],
                    Runs: 
{runs},
                    Standard Deviation: [{self.standardDeviation()}],
                    Average: [{self.average()}],
                    """
    def __eq__(self, other):
        return self.id == other.id
    
    def findPattern(self, pattern):
        result = None
        matches = re.findall(pattern, self.name)
        for matchNum, match in enumerate(matches):
            numberMatches = re.findall("(\d+)", match) 
            for mNum, m in enumerate(numberMatches):
                result = m
        return result
    
    def findNumberOfNodes(self):
        return int(self.findPattern(r"[0-9]n"))
    def findMemory(self):
        return int(self.findPattern(r"[0-9]G"))
    def findCoresPerExecutor(self):
        return int(self.findPattern(r"(\d+)c"))
    
    def standardDeviation(self):
        if len(self.runs) == 0:
            raise Exception(f"Experiment [{self.id}] has no runs.")
        return np.std(self.runs)
        
    def average(self):
        if len(self.runs) == 0:
            raise Exception(f"Experiment [{self.id}] has no runs.")
        
        return np.mean(self.runs)

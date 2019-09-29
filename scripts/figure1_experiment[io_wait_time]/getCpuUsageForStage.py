import pandas as pd

def getCpuUsageForStage(topFileName, appName, stageId):
    data = pd.read_csv(topFileName, dtype={
    'stage': int,
    'cpu': float,
    'mem': float
    })
    
    data['appName'] = data['appName'].apply (lambda x: x.split("^", 1)[0])
    data = data.set_index("appName")
    
    appRows = data[data.index == appName]
    mean = appRows.groupby("stage").mean()
    
    cpuUsage = mean[mean.index == stageId]['cpu'].values[0]
    print(cpuUsage)
    return cpuUsage

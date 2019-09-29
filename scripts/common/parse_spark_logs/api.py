
import json
from pprint import pprint
from urllib.request import urlopen
import csv
from datetime import datetime
import numpy
from Experiment import Experiment
#import dateutil.parser
#from colorama import Fore
#from colorama import Style

def getRequest(url):
    data = urlopen(url).read().decode("utf-8");
    return json.loads(data)


def getAllStages(appId):
    stages = []
    appName =getAppName (appId)
    stages = getRequest(f"{base_application_api}/{appId}/stages")

    if len(stages) == 0:
        raise ValueError(f"No application found with id: {appId}")
    return stages

def getStageDurationForApp(appId, stageId):
    data = getStage(str(appId), stageId)
    submitTime = data['submissionTime']
    completeTime = data['completionTime']
    submitTimeParsed = datetime.strptime(submitTime, '%Y-%m-%dT%H:%M:%S.%f%Z')
    completeTimeParsed = datetime.strptime(completeTime, '%Y-%m-%dT%H:%M:%S.%f%Z')
    delta = completeTimeParsed - submitTimeParsed
    #print(f"stage: {data['duration']}")
    return delta.total_seconds() * 1000

def getStagesInfo(appId):
    totalStagesTime = 0;
    #appName = getAppName(appId)
    #print(f"Stages Info for [{appId}] -> : [{appName}]\n")
    data = getAllStages(appId)
    for line in data:
        #print(line)
        stageId = line['stageId']
        name = line['name']
        if ('submissionTime' not in line):
            continue;
        submitTime = line['submissionTime']
        completeTime = line['completionTime']
        submitTimeParsed = datetime.strptime(submitTime, '%Y-%m-%dT%H:%M:%S.%f%Z')
        completeTimeParsed = datetime.strptime(completeTime, '%Y-%m-%dT%H:%M:%S.%f%Z')
        delta = completeTimeParsed - submitTimeParsed
        totalStagesTime += delta.seconds

        #submitTime = dateutil.parser.parse(submitTime)
        #print(submitTime)
        #print(completeTime)
        print(f"Stage ID [{stageId}] with Name [{name}]  duration: {delta.seconds}s or {delta.seconds/60}m")
    print(f"Total time for all stages: [{totalStagesTime}]")
	

def getTotalOutput(appId):
    outputBytes = 0
    stages = getAllStages(appId)
    for stage in stages:
        #print(stage)
        outputBytes += stage['outputBytes']
    print(outputBytes)
    return outputBytes

def getApplications():
    #print("\n=============================================")
    #print(f"All Applications:")
    #print("=============================================")
    data = getRequest(f"{base_application_api}")
    return data;

def getAppIdByName(appName):
    app = getAppInfoByName(appName)
    return app['id']

def getAppDurationByName(appName):
    app = getAppInfoByName(appName)
    #print(app)
    duration = app['attempts'][0]['duration']
    # In seconds
    return round(duration / 1000)

def getAppDurationById(appId):
    app = getAppInfoById(appId)
    #print(app)
    duration = app['attempts'][0]['duration']
    return duration
    # In seconds
    #return round(duration / 1000)

def getAppInfoByName(appName):
    apps = getApplications();
    filteredData = [item for item in apps if item['name'] == appName]
    if len(filteredData) == 0:
        raise ValueError(f"No application found with name: {appName}")
    if len(filteredData) > 1:
        raise ValueError(f"More than one application has been found with name: {appName}")
    return filteredData[0]

def getAppsByName(appName):
    apps = getApplications();
    filteredData = [item for item in apps if item['name'] == appName]
    if len(filteredData) == 0:
        raise ValueError(f"No application found with name: {appName}")
    return filteredData;

def getAppInfoById(appId):
    apps = getApplications();
    filteredData = [item for item in apps if item['id'] == appId]
    if len(filteredData) == 0:
        raise ValueError(f"No application found with id: {appId}")
    if len(filteredData) > 1:
        raise ValueError(f"More than one application has been found with id: {appId}")
    return filteredData[0]

def getJobsInfo(appId):
    totalJobsTime = 0;
    appName = getAppName(appId)
    print("\n=============================================")
    print(f"Job Info for [{appId}] -> : [{appName}]")
    print("=============================================")
    data = getRequest(f"{base_application_api}/{appId}/jobs")
    for line in data:
        jobId = line['jobId']
        name = line['name']
        submitTime = line['submissionTime']
        completeTime = line['completionTime']
        submitTimeParsed = datetime.strptime(submitTime, '%Y-%m-%dT%H:%M:%S.%f%Z')
        completeTimeParsed = datetime.strptime(completeTime, '%Y-%m-%dT%H:%M:%S.%f%Z')
        delta = completeTimeParsed - submitTimeParsed
        totalJobsTime += delta.seconds
        #submitTime = dateutil.parser.parse(submitTime)
        #print(submitTime)
        #print(completeTime)
        print(f"Job ID [{jobId}] with Name [{name}]  duration: {delta.seconds}s or {delta.seconds/60}m")
    print(f"Total time for all stages: [{totalJobsTime}]");
	

def getStage(appId, stage):
    print(f"App ID: {appId}, Stage ID: {stage}")
    data =  getRequest(f"{base_application_api}/{appId}/stages/{stage}/0")
    return data
def getTasksForStage(appId, stage):
    tasks = []
    data = getRequest(f"{base_application_api}/{appId}/stages/{stage}/0/taskList?length=100000")
    for line in data:
        t = Task(line["taskId"], line["duration"])
        tasks.append(t)
    assert len(tasks) > 0, f"No tasks found for app [{appId}] in stage[{stage}]"
    return tasks

def getAppName(appId):
    data = getRequest(f"{base_application_api}/{appId}/environment")
    appName = [t[1] for t in  data['sparkProperties'] if t[0] == 'spark.app.name'][0]
    return appName

def getAppInfo(appId, withStages=False):
    #getJobsInfo(appId);
    duration = getAppDurationById(appId)
    appName = getAppName(appId)
    print("\n=============================================")
    print(f"App [{appName}] [{appId}] Duration = {duration}s")
    print("=============================================")

    if (withStages):
        getStagesInfo(appId)

def getInfo(a, b, c):
    getAppIdByName('numa-datagen-16c-8G-1n')
    appDuration = getAppDurationByName('numa-datagen-16c-8G-1n')
    #print(appDuration)

def printStagesByName(appName):
    appId = getAppIdByName(appName)
    getStagesInfo(appId)

def printAppInfoByName(appName):
    appId = getAppIdByName(appName)
    duration = getAppDurationByName(appName)
    print("\n=============================================")
    print(f"App [{appName}] [{appId}] Duration = {duration}s")
    print("=============================================")
    #getStagesInfo(appId)

def getInfoForWorkload(workloadName, cores, memory, nodes):
    #name = f"{workloadName}-{cores}c-{memory}G-{nodes}n"
    name = f"{workloadName}-{memory}G-{nodes}n"
    defaultAppName = f"default-{name}"
    numaAppName =  f"numa-{name}"
    numaRemoteAppName = f"numaR-{name}"

    #printAppInfoByName(numaRemoteAppName)
    printAppInfoByName(defaultAppName)
    printAppInfoByName(numaAppName)

def getInfoForWorkloadDefault(workloadName, cores, memory, nodes, howManyRuns):
    name = f"{workloadName}-{cores}c-{memory}G-{nodes}n"
    defaultAppName = f"default-{name}"
    durations = getDurationsForWorkloadName(defaultAppName, howManyRuns)

    for i, duration in enumerate(durations):
        print(f"Run [{i}] = {duration}")
        
    avg = numpy.mean(durations)
    std = numpy.std(durations)
    print(f"Average = [{avg}]")
    print(f"Standard Deviation = [{std}]")


def getInfoForWorkloadNuma(workloadName, cores, memory, nodes, howManyRuns):
    name = f"{workloadName}-{cores}c-{memory}G-{nodes}n"
    name = f"numa-{name}"
    durations = getDurationsForWorkloadName(name, howManyRuns)

    for i, duration in enumerate(durations):
        print(f"Run [{i}] = {duration}")
        
    avg = numpy.mean(durations)
    std = numpy.std(durations)
    print(f"Average = [{avg}]")
    print(f"Standard Deviation = [{std}]")


def getApplicationName(mode, workloadName, cores, memory, nodes, howManyRuns):
    #name = f"{workloadName}-{cores}c-{memory}G-{nodes}n"
    name = f"{workloadName}-{memory}G-{nodes}n"
    if mode == 1:
        name = f"numa-{name}"
    elif mode == 2:
        #name = f"numa-remote-{name}"
        name = f"numaR-{name}"
    else:
        name = f"default-{name}"
    return name

def getExperiment(appName, howManyRuns):
    durations = getDurationsForWorkloadName(appName, howManyRuns)
    if appName.startswith("numa-remote"):
        appName = appName.replace("numa-remote", "numaR")
    return Experiment(1, appName, durations)

def getAllExperimentsForNodes(workloadName, cores, memory, nodes, howManyRuns):
    experiments = []
    for i in range(3):
        mode = i
        appName = getApplicationName(mode, workloadName, cores, memory, nodes, howManyRuns)
        ex = getExperiment(appName, howManyRuns)
        experiments.append(ex)
    return experiments

def getLastExperiments(appName, startIndex, n):
    applications = getAppsByName(appName);
    length = len(applications)
    lastIndex = startIndex + n
    if length < lastIndex:
        raise ValueError(f"App [{appName}] has only [{length}] runs which is not equal to the runs specified:{startIndex + n}")
    return applications[startIndex:lastIndex]


def printLastExperiments(appName, startIndex, n):
    applications = getLastExperiments(appName, startIndex, n)
    durations = []
    print("==================")
    for app in applications:
        duration = getAppDurationById(app['id']);
        durations.append(duration)
        print(f"App [{app['id']}] [{app['name']}] duration: {duration}s")

    print(f"\nstandard dev: {numpy.std(durations)}")
    print(f"mean: {numpy.mean(durations)}")
    print("==================\n\n")
    

def getAllExperiments(workloadName, cores, memory, howManyRuns):
    experiments = []
    for i in range(1,5):
        nodes = i
        experiments.extend(getAllExperimentsForNodes(workloadName, cores, memory, nodes, howManyRuns))
    return experiments

def printAllExperimentsNew (experiments):
    filtered = experiments[0:5]
    printFilteredExperiments(filtered)
    filtered = experiments[5:10]
    printFilteredExperiments(filtered)
    filtered = experiments[10:15]
    printFilteredExperiments(filtered)
    filtered = experiments[15:20]
    printFilteredExperiments(filtered)

def printFilteredExperiments(filtered):
    print("==================")
    durations = []
    for app in filtered:
        duration = getAppDurationById(app['id']);
        durations.append(duration)
        print(f"App [{app['id']}] [{app['name']}] duration: {duration}s")

    print(f"\nstandard dev: {numpy.std(durations)}")
    print(f"mean: {numpy.mean(durations)}")
    print("==================\n\n")

        
def getAllExperimentsNew(experimentName, excessName, nodeNum, cores, mem, numberOfExperiments):
    experiments = []
    experiments.extend(getAllExperimentsForMode(-1, experimentName, excessName, nodeNum, cores, mem, numberOfExperiments))
    experiments.extend(getAllExperimentsForMode(0, experimentName, excessName, nodeNum, cores, mem, numberOfExperiments))
    experiments.extend(getAllExperimentsForMode(1, experimentName, excessName, nodeNum, cores, mem, numberOfExperiments))
    experiments.extend(getAllExperimentsForMode(2, experimentName, excessName, nodeNum, cores, mem, numberOfExperiments))
    return experiments;
def getAllExperimentsForMode(numaMode, experimentName, excessName, nodeNum, cores, mem, numberOfExperiments):
    result = [];
    appName = getAppNameForMode(numaMode, experimentName, excessName, nodeNum, cores, mem, numberOfExperiments)
    result = getLastExperiments(appName, 0, 5)
    return result

def getAppNameForMode(numaMode, experimentName, excessName, nodeNum, cores, mem, numberOfExperiments):
    appName = ""
    numaModeString = getNumaModeString(numaMode)
    execAndCoresAndMemString = getExecNumAndCoresAndMemForMode(numaMode, cores, mem)

    appName = f"""{numaModeString}-{experimentName}-{nodeNum}n-{execAndCoresAndMemString}-{excessName}"""
    return appName
def getExecNumAndCoresAndMemForMode(numaMode, cores, mem):
    if (numaMode == -1):
        #return f"""1e-{cores * 2}c-{mem * 2}g"""
        return f"""1e-{mem * 2}g"""
     
    #return f"""2e-{cores}c-{mem}g"""
    return f"""2e-{mem}g"""
def getNumaModeString(numaMode):
    result = ""
    if (numaMode ==  -1 or numaMode == 0 ):
        result = "default"
    elif (numaMode == 1 ):
        result = "numa"
    elif (numaMode == 2):
        result = "numaR"
    return result

def getInfoForWorkloadNumaRemote(workloadName, cores, memory, nodes, howManyRuns):
    name = getApplicationName(2, workloadName, cores, memory, nodes, howManyRuns)
    durations = getDurationsForWorkloadName(name, howManyRuns)

    for i, duration in enumerate(durations):
        print(f"Run [{i}] = {duration}")
        
    avg = numpy.mean(durations)
    std = numpy.std(durations)
    print(f"Average = [{avg}]")
    print(f"Standard Deviation = [{std}]")

def getDurationsForWorkloadName(workloadName, howManyRuns):
    durations = []
    for i in range(1, howManyRuns+1):
        name = f"{workloadName}-{i}x"
        #appId = getAppIdByName(appName)
        duration = getAppDurationByName(name)
        durations.append(duration)

    return durations;

def writecsv(fileName, experiments):
    with open(f"{fileName}.csv", "w") as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        for experiment in experiments:
            row = []
            row.append(experiment.name)
            row.extend(experiment.runs)
            writer.writerow(row)

    

def parseAndOutputCsv(appId, stage):
    tasks = getTasksForStage(appId, stage)
    for t in tasks:
        print(t)
    appName = getAppName(appId)
    writeCSV(f"{appName}-s{stage}", tasks)

base_api = "http://localhost:18080/api/v1"
base_application_api = f"{base_api}/applications"


import json
from pprint import pprint
from urllib.request import urlopen
import csv

class Task:
    def __init__(self, taskId, duration, gcTime, executorId):
        self.taskId = int(taskId)
        self.duration = int(duration)
        self.gcTime = int(gcTime)
        self.executorId = int(executorId)
    def __str__(self):
        return f"""
		Task: [{self.taskId}]
			Duration: [{self.duration}],
			GC Time: [{self.gcTime}],
			Executor ID: [{self.executorId}],
		"""
    def __iter__(self):
        return iter([self.taskId, self.duration, self.gcTime, self.executorId])

def writeCSV(fileName, data):
    with open(f"data/{fileName}.csv", "w") as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        writer.writerow(['taskId', 'duration', 'gcTime', 'executorId'])
        writer.writerows(data)
        #for task in data:		
            #writer.writerow(task)
def getRequest(url):
    data = urlopen(url).read().decode("utf-8");
    return json.loads(data)

def getTasksForStage(appId, stage):
    tasks = []
    data = getRequest(f"{base_application_api}/{appId}/stages/{stage}/0/taskList?length=100000")
    for line in data:
        print(line)
        t = Task(line["taskId"], line["duration"], line["taskMetrics"]["jvmGcTime"], line["executorId"])
        tasks.append(t)
    assert len(tasks) > 0, f"No tasks found for app [{appId}] in stage[{stage}]"
    return tasks

def getAppName(appId):
    data = getRequest(f"{base_application_api}/{appId}/environment")
    appName = [t[1] for t in  data['sparkProperties'] if t[0] == 'spark.app.name'][0]
    return appName

def parseAndOutputCsv(appId, stage):
    tasks = getTasksForStage(appId, stage)
    for t in tasks:
        print(t)
    appName = getAppName(appId)
    writeCSV(f"{appName}-s{stage}", tasks)

#base_api = "http://localhost:18080/api/v1"
base_api = "http://node348:18080/api/v1"
base_application_api = f"{base_api}/applications"
app_id = "app-20180713132544-0015" #18min numa
stage = 2

#parseAndOutputCsv(app_id, stage)
#parseAndOutputCsv("app-20180713130750-0014", stage) #18min default

#parseAndOutputCsv("app-20180717120548-0031", 0) #Sparkpi 10000 default
#parseAndOutputCsv("app-20180717121455-0032", 0) #Sparkpi 10000 numa

#parseAndOutputCsv("app-20180809154803-0000", 0)
#parseAndOutputCsv("app-20180809154803-0000", 1)

parseAndOutputCsv("app-20180809161104-0000", 0)
parseAndOutputCsv("app-20180809161104-0000", 1)
parseAndOutputCsv("app-20180809161104-0000", 2)
#parseAndOutputCsv("app-20180801131654-0001", 2)
#parseAndOutputCsv("app-20180801131012-0000", 2)
#parseAndOutputCsv("app-20180727144031-0015", 0)
#parseAndOutputCsv("app-20180727144758-0016", 0)

#parseAndOutputCsv("app-20180727130603-0012", 2)
#parseAndOutputCsv("app-20180727121640-0004", 2)
#parseAndOutputCsv("app-20180727120007-0002", 2)

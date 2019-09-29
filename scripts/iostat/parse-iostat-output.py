import csv
import numpy as np
import sys
from operator import attrgetter
import os
from pathlib import Path
import socket
import pandas as pd

import re
import socket
import selectors
import types
import threading
import struct
import logging

from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-a", "--appName")
parser.add_argument("-f", "--fileNameWithoutExtension")
parser.add_argument("-d", "--deviceId", action='append')

args = parser.parse_args(sys.argv[1:])

appName = args.appName
fileNameWithoutExtension = args.fileNameWithoutExtension
deviceIds = args.deviceId


logDirectory = os.getenv('LOG_HOME', '/local/omranian')
fileName = f'{logDirectory}/parseIoStatOutput.log'

RESULT_HOME = os.getenv('RESULT_HOME', f"{Path.home()}/results")

stageChangedCommand = "STAGE_CHANGED"

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s',
    filename=fileName,
    filemode='w',
    )

nodeId = socket.gethostname()



stage = 0

class IoStatOutput:
    def __init__(self):
        self.appName = "unknown"
        self.time = 0
        self.date = 0
        self.time = 0
        self.user = 0
        self.nice = 0
        self.system = 0
        self.iowait = 0
        self.steal = 0
        self.idle = 0
        self.Device = "unknown"
        self.rrqmPerS = 0
        self.wrqmPerS = 0
        self.rPerS = 0
        self.wPerS = 0
        self.rkBPerS = 0
        self.wkBPerS = 0
        self.avgrq_sz = 0
        self.avgqu_sz = 0
        self.await = 0
        self.r_await = 0
        self.w_await = 0
        self.svctm = 0
        self.util = 0
    



def parseLine(input):
    input = input.rstrip().split('^')
    input = [i for i in input if i]
     
    print(input)
    output = IoStatOutput()
    for item in input:
        item = item.rstrip().split()
        
        if item[0] == "avg-cpu:" or item[0] == "Device:" or item[0] == "md0":
            continue 
        print(item)
        if(item[0].replace('.','',1).isdigit()):
            print("NUMBER!")
            output.user = item[0]
            output.nice = item[1]
            output.system = item[2]
            output.iowait = item[3]
            output.steal = item[4]
            output.idle = item[5]
        elif (item[0] in deviceIds):
            output.rrqmPerS += float(item[1])
            output.wrqmPerS += float(item[2])
            output.rPerS += float(item[3])
            output.wPerS += float(item[4])
            output.rkBPerS += float(item[5])
            output.wkBPerS += float(item[6])
            output.avgrq_sz += float(item[7])
            output.avgqu_sz += float(item[8])
            output.await += float(item[9])
            output.r_await += float(item[10])
            output.w_await += float(item[11])
            output.svctm += float(item[12])
            output.util += float(item[13])
    writeToOutput(output)        
 
    
def writeToOutput(output):
    global fileNameWithoutExtension
    fileName = f"{RESULT_HOME}/{nodeId}#{fileNameWithoutExtension}.iostat"
    file_exists = os.path.isfile(fileName)
    
    with open(fileName, mode='a') as output_file:
#         self.util = 0
        headers = ['nodeId', 'appName', 'stage', 'user', 'nice'
                   , 'system'
                   , 'iowait'
                   , 'steal'
                   , 'idle'
                   , 'rrqmPerS'
                   , 'wrqmPerS'
                   , 'rPerS'
                   , 'wPerS'
                   , 'rkBPerS'
                   , 'wkBPerS'
                   , 'avgrq_sz'
                   , 'avgqu_sz'
                   , 'await'
                   , 'r_await'
                   , 'w_await'
                   , 'svctm'
                   , 'util'
                   ]
        writer = csv.writer(output_file, delimiter=',')

        if not file_exists:
            writer.writerow(headers)  # file doesn't exist yet, write a header
         
        row = [nodeId, appName, stage, output.user, output.nice
                   , output.system
                   , output.iowait
                   , output.steal
                   , output.idle
                   , output.rrqmPerS
                   , output.wrqmPerS
                   , output.rPerS
                   , output.wPerS
                   , output.rkBPerS
                   , output.wkBPerS
                   , output.avgrq_sz
                   , output.avgqu_sz
                   , output.await
                   , output.r_await
                   , output.w_await
                   , output.svctm
                   , output.util]
        logging.info(f'Writing to ouput: {row}')
        writer.writerow(row)
    

sel = selectors.DefaultSelector()

def my_tcp_server():
    HOST = '127.0.0.1'
    PORT = 12005
    
    lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    lsock.bind((HOST, PORT))
    lsock.listen()
    logging.info(f'listening on [{HOST}:{PORT}]')
    lsock.setblocking(False)
    sel.register(lsock, selectors.EVENT_READ, data=None)
    
    while True:
        events = sel.select(timeout=None)
        for key, mask in events:
            if key.data is None:
                accept_wrapper(key.fileobj)
            else:
                service_connection(key, mask)

def accept_wrapper(sock):
    conn, addr = sock.accept()  # Should be ready to read
    logging.info(f'accepted connection from {addr}')
    conn.setblocking(False)
    data = types.SimpleNamespace(addr=addr, inb=b'', outb=b'')
    events = selectors.EVENT_READ | selectors.EVENT_WRITE
    sel.register(conn, events, data=data)

    
def service_connection(key, mask):
    global total_epollWaitTime
    sock = key.fileobj
    data = key.data
    if mask & selectors.EVENT_READ:
        recv_data = sock.recv(1024)  # Should be ready to read
        if recv_data:
            string = recv_data.strip().decode('utf-8')
            logging.info(f'[Recieved from {data.addr}]: {string}')
            if(stageChangedCommand in string):
                newStageId = string.split("^", 1)[1]
                global stage
                stage = int(newStageId)
        else:
            logging.info(f'closing connection to [{data.addr}]')
            sel.unregister(sock)
            sock.close()
                
threading.Thread(target=my_tcp_server).start()         
                
for line in sys.stdin:
    parseLine(line)
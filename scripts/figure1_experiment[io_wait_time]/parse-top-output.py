import csv
import numpy as np
import sys
from operator import attrgetter
import os
from pathlib import Path
import socket


import re
import socket
import selectors
import types
import threading
import struct
import logging

from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-e", "--executorId")
parser.add_argument("-a", "--appName")
parser.add_argument("-f", "--fileNameWithoutExtension")

args = parser.parse_args(sys.argv[1:])

logDirectory = os.getenv('LOG_HOME', '/local/omranian')
fileName = f'{logDirectory}/parseTopOutput.log'

RESULT_HOME = os.getenv('RESULT_HOME', f"{Path.home()}/results")

stageChangedCommand = "STAGE_CHANGED"

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s',
    filename=fileName,
    filemode='w',
#     handlers=[
#         logging.FileHandler(fileName),
#         logging.StreamHandler()
#         ]
    )

executorId = args.executorId
nodeId = socket.gethostname()
appName = args.appName
fileNameWithoutExtension = args.fileNameWithoutExtension

stage = 0

pIdIndex = 0
pIndex = 1
userIndex = 2
prIndex = 3
niIndex = 4
virtIndex = 5
resIndex = 6
shrIndex = 7
sIndex = 8
cpuIndex = 9
memIndex = 10
timeIndex = 11
commandIndex = 11

class TopOutput:
    def __init__(self):
        self.appName = "unknown"
        self.pId = 0
        self.p = 0
        self.user = 0
        self.pr = 0
        self.ni = 0
        self.virt = 0
        self.res = 0
        self.shr = 0
        self.s = 0
        self.cpu = 0
        self.mem = 0
        self.time = 0
        self.command = ""
    
# topOutput = TopOutput    
def parseLine(input):
    input = input.rstrip().split()
    
    if input == []:
        return
    if input[0].isdigit() == False:
        return
    
    logging.info(input)
    
#     global topOutput
    topOutput = TopOutput()
    topOutput.appName = appName
    topOutput.pId = input[pIdIndex]
    topOutput.p = input[pIndex]
    topOutput.user = input[userIndex]
    topOutput.pr = input[prIndex]
    topOutput.ni = input[niIndex]
    topOutput.virt = input[virtIndex]
    topOutput.res = input[resIndex]
    topOutput.soft = input[sIndex]
    topOutput.shr = input[shrIndex]
    topOutput.s = input[sIndex]
    topOutput.cpu = input[cpuIndex]
    topOutput.mem = input[memIndex]
    topOutput.time = input[timeIndex]
    topOutput.command = input[commandIndex]
    
    writeToOutput(topOutput)
    
    
def writeToOutput(obj):
    global fileNameWithoutExtension
    fileName = f"{RESULT_HOME}/{fileNameWithoutExtension}.top"
    file_exists = os.path.isfile(fileName)
    
    with open(fileName, mode='a') as output_file:
        headers = ['nodeId', 'executorId', 'appName', 'stage', 'cpu', 'mem']
        writer = csv.writer(output_file, delimiter=',')

        if not file_exists:
            writer.writerow(headers)  # file doesn't exist yet, write a header
            
        row = [nodeId, executorId, appName, stage, obj.cpu, obj.mem]
        
        writer.writerow(row)
    

sel = selectors.DefaultSelector()

def my_tcp_server():
    HOST = '127.0.0.1'
    PORT = 12002
    
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
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
fileName = f'{logDirectory}/parseMpstatOutput.log'

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

stage = 0

executorId = args.executorId
nodeId = socket.gethostname()
appName = args.appName
fileNameWithoutExtension = args.fileNameWithoutExtension

timeOrAverageIndex = 0
cpuIndex = 2
usrIndex = 3
niceIndex = 4
sysIndex = 5
iowaitIndex = 6
irqIndex = 7
softIndex = 8
stealIndex = 9
guestIndex = 10
gniceIndex = 11
idleIndex = 12

class MpStatOutput:
    def __init__(self):
        mpstatOutput.appName = "unknown"
        mpstatOutput.timeOrAverage = ""
        mpstatOutput.cpu = 0
        mpstatOutput.usr = 0
        mpstatOutput.nice = 0
        mpstatOutput.sys = 0
        mpstatOutput.iowait = 0
        mpstatOutput.irq = 0
        mpstatOutput.soft = 0
        mpstatOutput.steal = 0
        mpstatOutput.guest = 0
        mpstatOutput.gnice = 0
        mpstatOutput.idle = 0
    
mpstatOutput = MpStatOutput    

def parseLine(input):
    input = input.rstrip().split()
    
    if input == []:
        return
    try:
        if input[2] != "all":
            return
    except:
        return
    
    logging.info(input)
    
#     global topOutput
    output = MpStatOutput()
    output.appName = appName
    output.timeOrAverage = ""
    output.cpu = input[cpuIndex]
    output.usr = input[usrIndex]
    output.nice = input[niceIndex]
    output.sys = input[sysIndex]
    output.iowait = input[iowaitIndex]
    output.irq = input[irqIndex]
    output.soft = input[softIndex]
    output.steal = input[stealIndex]
    output.guest = input[guestIndex]
    output.gnice = input[gniceIndex]
    output.idle = input[idleIndex]
    
    writeToOutput(output)
 
sel = selectors.DefaultSelector()

def my_tcp_server():
    HOST = '127.0.0.1'
    PORT = 12004
    
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
    
    
def writeToOutput(mpstatOutput):
    global fileNameWithoutExtension
    fileName = f"{RESULT_HOME}/{fileNameWithoutExtension}.mpstat"
    file_exists = os.path.isfile(fileName)
    
    with open(fileName, mode='a') as output_file:
        headers = ['nodeId', 'executorId', 'appName', 'stage','cpu', 'usr', 'nice', 'sys', 'iowait', 'irq', 'soft', 'steal', 'guest', 'gnice', 'idle']
        writer = csv.writer(output_file, delimiter=',')

        if not file_exists:
            writer.writerow(headers)  # file doesn't exist yet, write a header
         
        global nodeId
        global executorId    
        row = [nodeId, executorId, mpstatOutput.appName, stage, mpstatOutput.cpu, mpstatOutput.usr, mpstatOutput.nice, mpstatOutput.sys, mpstatOutput.iowait, mpstatOutput.irq, mpstatOutput.soft, mpstatOutput.steal, mpstatOutput.guest, mpstatOutput.gnice, mpstatOutput.idle]
        
        writer.writerow(row)

for line in sys.stdin:
    parseLine(line)

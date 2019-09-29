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
parser.add_argument("-f", "--file")

args = parser.parse_args(sys.argv[1:])


filePath = args.file


# fileName = f"{Path.home()}/logs/lseek/parseLseekOutput.log"
fileName = f"/local/omranian/straceFileParserOutput.log"
fileNameMac = f"{Path.home()}/logs/straceFileParser/straceFileParserOutput.log"
# '/local/omranum1an/parseTopOutput.log'


getTotalEpollWaitCommand = "GET_EPOLL_WAIT"
stageChangedCommand = "STAGE_CHANGED"

try:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s',
    filename=fileName,
    filemode='w',
#     handlers=[
#         logging.FileHandler(fileName),
#         logging.StreamHandler()
#         ]
    )
except:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s',
    filename=fileNameMac,
    filemode='w',
    )

nodeId = socket.gethostname()
appName = args.appName
# fileNameWithoutExtension = args.fileNameWithoutExtension

stage = 0



class StraceParserOutput:
    def __init__(self):
        self.appName = "unknown"
        totalTime = 0
        count = 0
        
    


total_epollWaitTime = 0

count = 0

matcher = re.compile(r"<[\d.]+>")
def getTimeFromLine(s):
    m = matcher.search(s)
    if(m == None):
#         logging.info("did not find any time!")
        return

    time = m.group()

    time = float(time[1:-1])

    global total_epollWaitTime
    global count 
    total_epollWaitTime += time
    if (time > 5):
        logging.info(f"Big time: {s}")
    count = count + 1
    


fin = open(filePath)
def updateCurrentStraceValue():
#        
    data = fin.read()
    list = data.splitlines()
    for line in list:
        getTimeFromLine(line)
        

       
 
    
def writeToOutput(output):
    global fileNameWithoutExtension
    fileName = f"{Path.home()}/results/{nodeId}#{appName}.straceFileParser"
    file_exists = os.path.isfile(fileName)
    
    with open(fileName, mode='a') as output_file:
        headers = ['nodeId', 'appName', 'stage', 'totalTime', 'count']

        writer = csv.writer(output_file, delimiter=',')

        if not file_exists:
            writer.writerow(headers)  # file doesn't exist yet, write a header
         

        row = [nodeId, appName, stage, total_epollWaitTime, count]
        logging.info(f'Writing to ouput: {row}')
        writer.writerow(row)
    

sel = selectors.DefaultSelector()

def my_tcp_server():
    HOST = '127.0.0.1'
    PORT = 12006
    
    lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    lsock.bind((HOST, PORT))
    lsock.listen()
    logging.info(f'listeningng on [{HOST}:{PORT}]')
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
#     global total_epollWaitTime
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
                global total_epollWaitTime
                stage = int(newStageId)
                total_epollWaitTime = 0
                fin.read()
            elif(string == getTotalEpollWaitCommand):
                updateCurrentStraceValue()
                logging.info(f'[Going to send totalEpollWaitTime[{total_epollWaitTime}] to {data.addr}]')
#                 total_epollWaitTimeByteArray = bytearray(struct.pack("f", total_epollWaitTime))
                total_epollWaitTimeByteArray = f"{total_epollWaitTime}\r\n".encode('utf_8')
#                 total_epollWaitTimeByteArray.extend("\r\n".encode('utf_8'))
                data.outb = total_epollWaitTimeByteArray
        else:
            logging.info(f'closing connection to [{data.addr}]')
            sel.unregister(sock)
            sock.close()
    if mask & selectors.EVENT_WRITE:
        if data.outb:
            logging.info(f'sending {repr(data.outb)} to [{data.addr}]')
            sent = sock.send(data.outb)  # Should be ready to write
            data.outb = data.outb[sent:]

                
threading.Thread(target=my_tcp_server).start()         
   


# for line in sys.stdin:
#     parseLine(line)
    




    


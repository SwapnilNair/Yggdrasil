import socket
import sys 
from uuid import uuid4
import json
from _thread import *
import threading
import time

lock = threading.Lock()

class Asgardian():

    def __init__(self, ip, port, id, flag):
        self._ip = ip
        self._port = port
        self._id = id
        self._flag = flag

    def createOffsetTable(self,topics,flag):
        if flag:
            f = 0
        else:
            f = -1

        self.topics_table = {}
        for i in topics:
            self.topics_table[i] = {"0":f,"1":f,"2":f}      
            

    def updateOffsetTable(self, topics, topics_table, offset):
        for i in topics:
            topics_table[i] = [offset,offset,offset]


    def BrokerHandler(self, HOST, PORT, message):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((HOST, PORT))
        dataString = json.dumps(self.wrapMessage(message))
        sock.send(dataString.encode('utf-8'))
        conn, addr = sock.accept()
        print(f'Connected to: {addr[0]}:{str(addr[1])}')
        while True:
            data = conn.recv(4096).decode('utf-8')
            res = json.loads(data)
            self.offset_table = res['offset_table']
            self.content = res['messages']
            print(self.content)
        
    
    def subscribe(self, topics_table):
        #sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        HOST = 'localhost'
        PORT_0 = 3500
        PORT_1 = 3501
        PORT_2 = 3502
        broadcastThreadHeimdall0 = threading.Thread(target=self.broadcastHandler, args=(HOST, PORT_0, topics_table))
        broadcastThreadHeimdall1 = threading.Thread(target=self.broadcastHandler, args=(HOST, PORT_1, topics_table))
        broadcastThreadHeimdall2 = threading.Thread(target=self.broadcastHandler, args=(HOST, PORT_2, topics_table))
        broadcastThreadHeimdall0.start()
        broadcastThreadHeimdall1.start()
        broadcastThreadHeimdall2.start()       

    def broadcastHandler(self, HOST, PORT,topics_table, message):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((HOST, PORT))
        dataString = json.dumps(self.wrapMessage(message))
        sock.send(dataString.encode('utf-8'))
        conn, addr = sock.accept()
        print(f'Connected to: {addr[0]}:{str(addr[1])}')
        while True:
            data = conn.recv(4096).decode('utf-8')
            #print(data)
            res = json.loads(data)
            self.offset_table = res['offset_table']
            self.topics_table = self.offset_table
    '''
    def updateOffsetTable(self, topics_table, offset):
        for i in topics_table:
            for j in topics_table[i]:
                topics_table[i][j] = offset
    '''   
    def request(self):      
        HOST = 'localhost'
        PORT_0 = 3500
        PORT_1 = 3501
        PORT_2 = 3502
        reqMessage = 'RequestingMessage'
        broadcastReqHeimdall1 = threading.Thread(target=self.BrokerHandler, args=(HOST, PORT_1, reqMessage))
        broadcastReqHeimdall2 = threading.Thread(target=self.BrokerHandler, args=(HOST, PORT_2, reqMessage))
        broadcastReqHeimdall0 = threading.Thread(target=self.BrokerHandler, args=(HOST, PORT_0, reqMessage))
        broadcastReqHeimdall0.start()
        broadcastReqHeimdall1.start()
        broadcastReqHeimdall2.start() 

 
    def wrapMessage(self, payload):
        return {
                "nodeType" : "asgardian",
                "ip" : self._ip,
                "port" : self._port,
                "nodeID" : str(self._id),
                "flag" : self._flag,
                "message" : payload
                }
            



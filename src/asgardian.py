import json
import socket
import sys
import threading
import time
from _thread import *
from uuid import uuid4


class Asgardian():
    def __init__(self, ip, port, id, flag):
        self._ip = ip
        self._port = port
        self._id = id
        self._flag = flag
        self.offset_table = {}
        self.topics_table = []
        self.offsetLock = threading.Lock()
        # self._metadata = 
        

    def createOffsetTable(self,topics,flag):
        if flag:
            f = 0
        else:
            f = -1

        self.offset_table = {}

        for i in topics:
            self.offset_table[i] = {"0":f,"1":f,"2":f}   
            
        print(self.offset_table)
        
    """def updateOffsetTable(self, topics, topics_table, offset):
        for i in topics:
            topics_table[i] = [offset,offset,offset]"""
    
    def subscribe(self, topics_table):
        #sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.topics_table = topics_table
        self.createOffsetTable(topics_table, self._flag)

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

        while broadcastThreadHeimdall0.is_alive() or broadcastThreadHeimdall1.is_alive() or broadcastThreadHeimdall2.is_alive():
            time.sleep(1)

        print("MESSAGE[ASGARDIAN] : Finished subscribing to topics...")
        self.request()

    # called to subscribe
    def broadcastHandler(self, HOST, PORT, topics_table, message=""):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((HOST, PORT))
            dataString = json.dumps(self.wrapMessage({
                "offset_table": self.offset_table,
                "topics": topics_table
            }))
            sock.send(dataString.encode('utf-8'))

            if not self._flag:
                data = sock.recv(4096).decode('utf-8')
                res = json.loads(data)
                # print("RESSSSSSSSSSSS >", res)
                self.updateOffsetTable(res['payload']['offset_table'])
            print("UPDATED : ", self.offset_table)
        except:
            pass  
    
    def updateOffsetTable(self, offset_table):
        self.offsetLock.acquire(blocking=True)
        for i in offset_table:
            for j in offset_table[i]:
                self.offset_table[i][j] = offset_table[i][j]
        self.offsetLock.release()


    
    # called when message is req
    def BrokerHandler(self, HOST, PORT):
        while True:
            print("MESSAGE[ASGARDIAN] : Polling heimdall {} for messages! (Thread-{})".format(PORT, threading.current_thread().name))
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((HOST, PORT))
                dataString = json.dumps(self.wrapMessage({
                    "query" : 'L',
                    "offset_table": self.offset_table
                }))
                sock.sendall(dataString.encode('utf-8'))
                
                data = sock.recv(8192).decode('utf-8')
                # print(data)
                res = json.loads(data)

                self.updateOffsetTable(res['payload']["offset_table"])
                print("RECEIVED MESSAGE OF LEN : {} from PORT {}".format(len(res['payload']['messages']), PORT))
                time.sleep(3)
            except Exception as err:
                print(err)
                break
                

    def request(self):      
        HOST = 'localhost'
        PORT_0 = 3500
        PORT_1 = 3501
        PORT_2 = 3502
        broadcastReqHeimdall0 = threading.Thread(target=self.BrokerHandler, args=(HOST, PORT_0))
        broadcastReqHeimdall1 = threading.Thread(target=self.BrokerHandler, args=(HOST, PORT_1))
        broadcastReqHeimdall2 = threading.Thread(target=self.BrokerHandler, args=(HOST, PORT_2))
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
            "payload" : payload
        }
            



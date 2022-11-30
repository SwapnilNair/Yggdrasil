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

    def broadcastHandler(self, HOST, PORT, message):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((HOST, PORT))
        dataString = json.dumps(self.wrapMessage({'topics': message}))
        sock.send(dataString.encode('utf-8'))
        sock.close()

    def subscribe(self, topics):
        #sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        HOST = 'localhost'
        PORT_0 = 3500
        PORT_1 = 3501
        PORT_2 = 3502
        broadcastThreadHeimdall0 = threading.Thread(target=self.broadcastHandler, args=(HOST, PORT_0, topics))
        broadcastThreadHeimdall1 = threading.Thread(target=self.broadcastHandler, args=(HOST, PORT_1, topics))
        broadcastThreadHeimdall2 = threading.Thread(target=self.broadcastHandler, args=(HOST, PORT_2, topics))
        broadcastThreadHeimdall0.start()
        broadcastThreadHeimdall1.start()
        broadcastThreadHeimdall2.start()       

    def BrokerHandler(self, HOST, PORT, message):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((HOST, PORT))
        dataString = json.dumps(self.wrapMessage({'message': message}))
        sock.send(dataString.encode('utf-8'))
        conn, addr = sock.accept()
        print(f'Connected to: {addr[0]}:{str(addr[1])}')
        while True:
            data = conn.recv(4096).decode()
            print(data)

            
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
            
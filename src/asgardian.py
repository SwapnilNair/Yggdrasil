import socket
import sys 
from uuid import uuid4
import json
from _thread import *
import threading

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
        
                  
            
    def rcvMessage(self):    
        
        IP = 'localhost'
        PORT = 8000
        broker_addr = (IP,PORT)
        sock.connect(broker_addr)
        print('Broker connected')
        print(sock.recv(4096).decode())
        sock.close()

 
    def wrapMessage(self, message):
        return {"nodeType" : "asgardian", "ip" : self._ip, "port" : self._port, "nodeID" : str(self._id), "flag" : self._flag, "message" : message}
            



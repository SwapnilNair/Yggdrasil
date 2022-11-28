import socket
import sys 
from uuid import uuid4
import json

class Asgardian():

    def __init__(self, ip, port, id):
        self._producerID = uuid4()
        self._ip = ip
        self._port = port
        self._id = id
        self._socketStorage = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def subscribe(self, topics):
        #sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        HOST = 'localhost'
        PORT = 3500
        self._socketStorage.connect((HOST, PORT))
        dataString = json.dumps(self.wrapMessage({'topics':topics}))
        self._socketStorage.send(dataString.encode('utf-8'))
        print("Topics sent to Heimdall")
        #sock.close()             
            
    def rcvMessage(self):    
        
        IP = 'localhost'
        PORT = 8000
        broker_addr = (IP,PORT)
        self._socketStorage.connect(broker_addr)
        print('Broker connected')
        print(self._socketStorage.recv(4096).decode())
        #self._socketStorage.close()

 
    def wrapMessage(self, message):
        return {"nodeType" : "asgardian", "ip" : self._ip, "port" : self._port, "nodeID" : str(self._id), 'message' : message}
            



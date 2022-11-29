import json
import socket
import threading
from uuid import uuid4
from datetime import datetime
import zlib
import base64
import requests


class Norse():

    def __init__(self, port, bufferSize = 100, ip = 'localhost'):
        self._producerID = uuid4()
        self._ip = ip
        self._port = port
        self._bufferSize = bufferSize
        self._incomingMessages = {}
        self._bufferForceFlushTimer = None
        self._brokerHost = 'localhost'
        self._brokerPort = 3500
        self._socketStorage = None
        
    def sendMessage(self, topic):
        #print({'topic': topic, 'messages': self._incomingMessages[topic]})
        currentHeimdallMetadata = self.checkForLeaderHeimdall()
        
        self._socketStorage = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socketStorage.connect((self._brokerHost, self._brokerPort))
        dataString = json.dumps(self.wrapMessage({'topic': topic, 'messages': self._incomingMessages[topic]}))
        self._socketStorage.send(dataString.encode('utf-8'))
        print("data sent to Heimdall")
        self._socketStorage.close()
        
    def produceMessage(self, topic, message):
        if self._bufferForceFlushTimer != None:
            self._bufferForceFlushTimer.cancel()
        if not self._incomingMessages.get(topic):
            self._incomingMessages[topic] = []
        self._incomingMessages[topic].append(message)
        if(len(self._incomingMessages[topic]) == self._bufferSize):
            self.sendMessage(topic)
            #clear incomingMessages[topic]
            self._incomingMessages[topic].clear()
        else:
            self._bufferForceFlushTimer = threading.Timer(5.0, lambda : self.sendMessage(topic))
            self._bufferForceFlushTimer.start()
    
    def compressMessage(self, message):
        #ZIPJSON_KEY = 'base64(zip(o))'
        message['messages'] = base64.b64encode(zlib.compress(json.dumps(message['messages']).encode('utf-8'))).decode('ascii')
        #print(message)
        #print(self.decompressMessage(message, insist = True))
        return message
    
    
    def wrapMessage(self, message):
        return {"nodeType" : "norse", "nodeID": str(self._producerID), 'message':self.compressMessage(message), "timestamp": str(datetime.now())}


    def checkForLeaderHeimdall(self):
        #get request to zookeeper
        URL = "https://localhost:5000/leader"
        r = requests.get(url = URL)
        dataReceived = r.json()
        print(dataReceived) #just to check
        return dataReceived


 

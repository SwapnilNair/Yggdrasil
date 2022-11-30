import base64
import json
import socket
import threading
import time
import zlib
from datetime import datetime
from uuid import uuid4

import requests


class Norse():

    def __init__(self, port, bufferSize = 100, ip = 'localhost'):
        self._producerID = uuid4()
        self._ip = ip
        self._port = port
        self._bufferSize = bufferSize
        self._incomingMessages = {}
        self._bufferForceFlushTimer = None
        self._socketStorage = None
        self._leaderHeimdall = None
        
    def sendMessage(self, topic):
        self._socketStorage = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self._socketStorage.connect((self._leaderHeimdall["heimdallIp"], self._leaderHeimdall["heimdallPort"]))
            dataString = json.dumps(self.wrapMessage({'topic': topic, 'messages': self._incomingMessages[topic]}))
            self._socketStorage.send(dataString.encode('utf-8'))
            print("data sent to Heimdall")
            self._socketStorage.close()
        except Exception as e:
            print('ERROR[NORSE]: {}'.format(str(e)))
            self.checkForLeaderHeimdall()
            self.sendMessage(topic)
            
        
        
    def produceMessage(self, topic, message):
        if self._bufferForceFlushTimer != None:
            self._bufferForceFlushTimer.cancel()
        if not self._incomingMessages.get(topic):
            self._incomingMessages[topic] = []
        self._incomingMessages[topic].append(message)
        if(len(self._incomingMessages[topic]) == self._bufferSize):
            self.sendMessage(topic)
            self._incomingMessages[topic].clear()
        else:
            self._bufferForceFlushTimer = threading.Timer(5.0, lambda : self.sendMessage(topic))
            self._bufferForceFlushTimer.start()
    
    def compressMessage(self, message):
        message['messages'] = base64.b64encode(zlib.compress(json.dumps(message['messages']).encode('utf-8'))).decode('ascii')
        return message
    
    
    def wrapMessage(self, message):
        return {"nodeType" : "norse", "nodeID": str(self._producerID), 'message':self.compressMessage(message), "timestamp": str(datetime.now())}


    def checkForLeaderHeimdall(self):
        URL = "http://localhost:5000/leader"
        """
        {
            "leader": -1
        }
        {
            "leader": {
                "heimdallId" : "",
                "heimdallIp" : "",
                "heimdallPort" : ""
            }
        }
        """
        leader = -1
        while (leader == -1 or leader == "-1"):
            print("MESSAGE[NORSE] : Requesting Odin for leader...")
            r = requests.get(url = URL)
            leader = r.json()['leader']
            print(leader)
            time.sleep(2)

        self._leaderHeimdall = {
            "heimdallIp": leader["ip"],
            "heimdallPort": int(leader["port"])
        }
        print(self._leaderHeimdall)

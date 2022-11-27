from uuid import uuid4
import threading
import socket, json

class Norse():

    def __init__(self, ip, port, bufferSize):
        self._producerID = uuid4()
        self._ip = ip
        self._port = port
        self._bufferSize = bufferSize
        self._incomingMessages = {}
        self._bufferForceFlushTimer = None
        
        
    
    def sendMessage(self, msgSetToSend):
        #print(len(msgSetToSend))
        HOST = 'localhost'
        PORT = 3500
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((HOST, PORT))
        dataString = json.dumps(msgSetToSend)
        s.send(dataString.encode('utf-8'))
        print("data sent to Heimdall")
        s.close()
    

    def produceMessage(self, topic, message):
        if self._bufferForceFlushTimer != None:
            self._bufferForceFlushTimer.cancel()
        if not self._incomingMessages.get(topic):
            self._incomingMessages[topic] = []
        self._incomingMessages[topic].append(message)
        if(len(self._incomingMessages[topic]) == self._bufferSize):
            self.sendMessage(self._incomingMessages[topic])
            #clear incomingMessages[topic]
            self._incomingMessages[topic].clear()
        else:
            self._bufferForceFlushTimer = threading.Timer(5.0, lambda : self.sendMessage(self._incomingMessages[topic]))
            self._bufferForceFlushTimer.start()
            


    def checkHeimdall():
        pass


 

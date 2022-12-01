import json
import threading
from time import sleep


class Partition():
    def __init__(self, partitionId, isReplica, logPath):
        self.partitionId = partitionId
        self.logPath = logPath
        self.brokerId = ""
        self.offset = 0
        """
        consumer : {"ip", "port", "offset", ""}
        """
        self.messagesLock = threading.Lock()
        self.consumers = {}
        self.messages = []
        self.isReplica = isReplica
        self.getMessagesFromLogs()
        self.logLock = threading.Lock()
        self.flushToFile = threading.Thread(
            target=self.flushMessagesToFile,
        )
        self.flushToFile.start()

    def flushMessagesToFile(self):
        while True:
            self.logLock.acquire(blocking=True)
            print("MESSAGE[PARTITION] : Flushing messages to file...")
            f = open(self.logPath, "w+")
            f.write(json.dumps(self.messages))
            f.close()
            self.logLock.release()
            print("MESSAGE[PARTITION] : Flushed messages to file!")
            sleep(5)

    def pushNewMessages(self, messages):
        self.messagesLock.acquire(blocking=True)
        self.messages.extend(messages)
        self.offset = len(self.messages)
        self.messagesLock.release()

    def getMessagesFromLogs(self):
        self.messagesLock.acquire(blocking=True)
        try:
            with open(self.logPath, "r") as f:
                d = json.load(f)
                self.messages.extend(d)
            self.offset = len(self.messages)
        except FileNotFoundError as e:
            self.offset = 0
            
        self.messagesLock.release()

    def __str__(self):
        print(self.messages)
        return "MESSAGE[PARTITION] : Partition {} created in Broker {}".format(self.partitionId, self.brokerId)
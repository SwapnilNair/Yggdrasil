import json
import threading


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

    def pushNewMessages(self, messages):
        self.messagesLock.acquire(blocking=True)
        self.messages.extend(messages)
        self.offset = len(self.messages)
        self.messagesLock.release()

    def getMessagesFromLogs(self):
        self.messagesLock.acquire(blocking=True)
        with open(self.logPath, "r") as f:
            d = json.load(f)
            self.messages.extend(d)
        self.offset = len(self.messages)
        self.messagesLock.release()

    def __str__(self):
        print(self.messages)
        return "MESSAGE[PARTITION] : Partition {} created in Broker {}".format(self.partitionId, self.brokerId)
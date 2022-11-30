import json
import threading


class Partition():
    def __init__(self, partitionId, isReplica, logPath):
        self._partitionId = partitionId
        self._logPath = logPath
        self._brokerId = ""
        self._offset = 0
        """
        consumer : {"ip", "port", "offset", ""}
        """
        self.messagesLock = threading.Lock()
        self.consumers = {}
        self.messages = []
        self._isReplica = isReplica
        self.getMessagesFromLogs()

    def getMessagesFromLogs(self):
        self.messagesLock.acquire(blocking=True)
        with open(self._logPath, "r") as f:
            d = json.load(f)
            self.messages.extend(d)
        self.messagesLock.release()

    def __str__(self):
        print(self.messages)
        return "MESSAGE[PARTITION] : Partition {} created in Broker {}".format(self._partitionId, self._brokerId)
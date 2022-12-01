from datetime import datetime


class Topic():
    def __init__(self, topicName, logPath):
        self.topicName = topicName
        self.partitions = []
        self._logPath = logPath
        self._createdAt = datetime.now()

    def __str__(self):
        return "MESSAGE[TOPIC] : TOPIC {} created @ {}".format(self._topicName, self._createdAt)
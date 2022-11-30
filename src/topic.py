class Topic():
    def __init__(self, topicName):
        self._topicName = topicName
        self._partitions = []
        self._logPath = ""
        self._createdAt = None

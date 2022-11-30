class Partition():
    def __init__(self, partitionId, isReplica):
        self._partitionId = ""
        self._logPath = ""
        self._brokerId = ""
        self._offset = 0
        """
        consumer : {"ip", "port", "offset", ""}
        """
        self.messagesLock = None
        self.consumers = {}
        self.messages = []
        self._isReplica = isReplica

"""

heimdall 0 [leader] => Orders(0), 
heimdall 1 => Orders(1)
heimdall 2 => Orders(2)


Consumer => sub("Orders") => req => all brokers => broker save topic, consumer (ip,port) => 
Consumer 
    => req 
    => broker(0,1,2) [in threads] 
    => broker checks what topics consumer has subscribed to
    => for that broker whichever partition check partition_offset - consumer_partition_offset
    => take messages[consumer_p_offset:partition_offset] 
    => send to consumer
    => inc offset for consumer


3 Brokers [fixed]
3 Partitions per topic [fixed]
3 replication factor [fixed]
if broker dies, let him/her/them/it die,
    zookeeper change leader
    zookeeper send metadata through heartbeat response
    consumer requests alive broker for data

Orders :
    Grocery =>> p0 [b0]
    Dairy =>> p1 [b1]
    Non-Food =>> p2 [b2]

Producer 
    => at init req zookeeper for meta data
    => zookeeper sends leader broker info 

    When broker dies 
        => producer cant send anymore => gets connection refused error
        => producer sends request to zookeeper
        => zookeeper 
            => replies HOGO LO if leader election is currently taking place (keep a flag)
            => replies with new leader if election is complete

    producer => Sends message to new leader

Consumer 


"""
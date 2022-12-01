import base64
import hashlib
import os
import socket
import threading
import zlib
from json import dumps, loads
from pprint import pprint
from socketserver import BaseRequestHandler, ThreadingTCPServer
from time import sleep
from uuid import uuid4

import requests

import errors
import messages
import partition
import topic

BASE_LOG_PATH = "C:/logs/"

class Heimdall():
    """
    """
    def __init__(self, port, ip="localhost", ) -> None:
        # initialize stuff
        self._id = -1
        self._ip = ip
        self._port = int(port)
        self._zookeeperIp = "localhost"
        self._zookeeperPort = 5000
        self._healthEndPoint = "health"
        self._topics = {}
        # self._producers = []
        self._consumers = {}

        # save consumer to topic map
        # save topic to consumer map

        self._heartBeatRequestRetryFailCount = 0

        self._heartbeatMessage = None

        self._metadataHash = None
        self._metadata = None
        self._metadataLock = threading.Lock()

        # replication
        self._nBrokers = 3
        self._nPartitions = 3
        self._nReplication = 3

        # create odin heartbeat process
        self._healthThread = None
        self.initHealthProc()

        # socket server 
        self._threadedSocketServer = None
        self._threadedSocketServerThread = None

    """
        HEARTBEATS -------------------------------------------------------
    """
    
    def initHealthProc(self):
        """
        Initialize the thread to serve the health client
        """
        self._healthThread = threading.Thread(
            target=self.serveHealthClient,
            args=(),
            name="healthThread"
        )
        self._healthThread.start()

    def serveHealthClient(self):
        """
        Serves client to send heartbeats to Zookeeper
        """
        k = 20
        for i in range(k):
            self.sendHeartBeat()
            sleep(1)

    def sendHeartBeat(self):
        """
        Makes a heartbeat request to Zookeeper 
        """
        print(messages.MSG_SENDING_HEARTBEAT.format(self._zookeeperIp, self._zookeeperPort))
        try:
            # make the heartbeat request
            r = requests.post(
                url=f'http://{self._zookeeperIp}:{self._zookeeperPort}/{self._healthEndPoint}',
                json={
                    "heimdallId": str(self._id),
                    "heimdallIp": str(self._ip),
                    "heimdallPort": str(self._port),
                }
            )
            data = r.json()
            # set heimdall id
            if data.get("heimdallId") != None:
                self._id = int(data.get("heimdallId"))
                print("MESSAGE[HEIMDALL] : Assigned ID : {}".format(self._id))
            
            md = data.get("data")
            # parse the metadata and create topics and partitions
            # compare hash to check if metadata was changed
            new_hash = hashlib.sha256(dumps(md).encode("utf-8")).hexdigest()

            if new_hash != self._metadataHash:
                self._metadataHash = new_hash
                self._metadataLock.acquire(blocking=True)
                self.parseMetaData(md)
                self._metadata = md
                self._metadataLock.release()

            # reset fail count
            self._heartBeatRequestRetryFailCount = 0
        except requests.exceptions.ConnectionError as err:
            # if 3 heartbeat requests failed, throw error
            if self._heartBeatRequestRetryFailCount == 3:
                print(errors.ERROR_ODIN_UNAVAILABLE)
            # increment the fail count
            else:
                print(errors.ERROR_ODIN_HEARTBEAT_REQUEST_RETRY)
                self._heartBeatRequestRetryFailCount += 1

    """
        METADATA ---------------------------------------------------------
    """

    def createTopic(self, topicName):
        # path = BASE_LOG_PATH+topicName
        # # create a folder for topic
        # try:
        #     os.mkdir(path)
        # except OSError as error:
        #     print(error)

        # newTopicMetaData = {
        #     "name": topicName,
        #     "logPath": path,
        #     "partitions": []
        # }

        # for i in range(0, 3):
        #     newTopicMetaData["partitions"].append(
        #         {
        #             "partitionId": str(i+1),
        #             "heimdallId": str(i+1),
        #             "isReplica": False,
        #             "logPath": f"{BASE_LOG_PATH}{topicName}/partition_{i+1}.json",
        #             "offset": 0
        #         }
        #     )
        
        # # print(self._metadata)
        # print("MESSAGE[HEIMDALL] : ADDING {}".format(topicName))
        # # update local copy of metadata
        # self._metadata["topics"][topicName] = newTopicMetaData
        # print(self._metadata)

        # self._metadataHash = hashlib.sha256(dumps(self._metadata).encode('utf-8')).hexdigest()

        # new_t = topic.Topic(
        #     topicName=topic,
        #     logPath=path
        # )

        # self._topics[topicName] = new_t

        # make a request to update metadata
        
        r = requests.post(
            url=f'http://{self._zookeeperIp}:{self._zookeeperPort}/updateMetadata',
            json={
                "topic": topicName
            }
        )
        


    def parseMetaData(self, metadata):
        topics = metadata["topics"]
        # create topics
        for t in topics:
            t_ = topic.Topic(topicName=t, logPath=topics[t]["logPath"])
            self._topics[t] = t_
            # create partitions

            # partitions in current broker
            partitions = list(
                filter(lambda p: int(p["heimdallId"]) == self._id, topics[t]["partitions"])
            )
            for part in partitions:
                p = partition.Partition(
                    partitionId=part["partitionId"],
                    isReplica=part["isReplica"],
                    logPath=part["logPath"]
                )
                self._topics[t].partitions.append(p)

        for t in self._topics:
            print(t)
            for p in self._topics[t].partitions:
                print(p)

    """
        PARTITIONING -----------------------------------------------------
    """
    
    def partitionFunction(self, message):
        if not message.get("key"):
            return -1
        if type(message["key"]) is str:
            return ord(message["key"][-1]) % self._nBrokers
        elif type(message["key"]) is int:
            return message["key"] % self._nBrokers
        else:
            return -1

    def wrapMessage(self, payload):
        return {
            "nodeType" : "heimdall", 
            "ip" : self._ip, 
            "port" : self._port,
            "nodeID" : str(self._id),
            "payload" : payload
        }

    def yeetToHeimdall(self, topic, partitioned_messages, broadcast=True):
        # if partition exists in current heimdall, 
        #   acquire lock for partition, write to partition
        for pid in partitioned_messages:
            part = list(filter(lambda p: int(p.partitionId) == int(pid), self._topics[topic].partitions))
            if len(part) > 0:
                selected_part:partition.Partition = part[0]
                print("MESSAGE[HEIMDALL] : MESSAGE TO : ", selected_part, ";;; PID : ", pid, ";;; OFFSET : ", selected_part.offset)
                selected_part.pushNewMessages(partitioned_messages[pid])
                print("MESSAGE[HEIMDALL] : NEW OFFSET : ", selected_part.offset)

        if broadcast:
            # create a socket to send data to other brokers
            for h in self._metadata["heimdalls"]:
                heim = self._metadata["heimdalls"][h]

                # broadcast only to alive heimdalls
                if (heim["ip"] == "" and heim["port"] == "") or (heim["ip"] == self._ip and heim["port"] == self._port) :
                    continue
                else:
                    # make socket for each and yeet it
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    # print(heim["ip"], heim["port"])
                    sock.connect((heim["ip"], int(heim["port"])))
                    dataString = dumps(self.wrapMessage({'topic': topic, 'partitioned_messages': partitioned_messages}))
                    sock.send(dataString.encode('utf-8'))
                    sock.close()

    def partitionIncomingMessages(self, topic, messages):
        # if topic doesnt exist, create topic | update metadata
        if self._topics.get(topic) != None:
            self.createTopic(topic)

        # partition messages into respective partitions
        partitions_buffer = {}
        for m in messages:
            # get partition number for each message
            p = self.partitionFunction(message=m)

            # TODO : handle partition error
            if p == -1:
                p = 0
            
            if not partitions_buffer.get(p+1):
                partitions_buffer[p+1] = []
            
            # add to partition buffer
            partitions_buffer[p+1].append(m)

        # pprint(partitions_buffer)
        # # push partitions to respective heimdall
        self.yeetToHeimdall(topic, partitions_buffer)
    
    """
        SOCKET SERVER ----------------------------------------------------
    """

    def decompressMessage(self, payload, insist = True):
        try:
            assert(payload['messages'])
        except:
            if insist:
                raise RuntimeError("JSON not in expected format")
            else:
                return payload
        try:
            payload['messages'] = zlib.decompress(base64.b64decode(payload['messages']))
        except:
            raise RuntimeError("Could not decode contents")
        
        try:
            payload['messages'] = loads(payload['messages'])
        except:
            raise RuntimeError("Could interpret unzipped contents")
        return payload


    def returnSocketHandler(self):
        heimdallSelf = self
        class socketRequestHandler(BaseRequestHandler):
            """
            Handler for the socket server
            """
            def handle(self):
                data = loads(str(self.request.recv(8192), 'utf-8'))
                print(messages.MSG_DATA_RECEIVED.format(len(data),self.client_address))

                nodeType = data["nodeType"]

                if nodeType == "norse":
                    # If norse sends messages here => THIS HEIMDALL IS THE LEADER
                    # new messages coming in
                    # partition and yeet to respective heimdall
                    # print()
                    heimdallSelf.partitionIncomingMessages(
                        data["payload"]["topic"],
                        heimdallSelf.decompressMessage(data["payload"])["messages"]
                    )
                elif nodeType == "heimdall":
                    # sync partitions
                    payload = data["payload"]
                    # print(payload.keys())
                    heimdallSelf.yeetToHeimdall(payload["topic"], payload["partitioned_messages"], False)

                elif nodeType == "asgardian":
                    # print(data)
                    # print()
                    # requests messages
                    payload = data["payload"]
                    # print(payload)
                    # print(type(payload))
                    ip = data["ip"]
                    port = data["port"]
                    flag = data.get("flag")
                    offset_table = payload.get("offset_table")
                    topics = payload.get("topics")
                    query = payload.get("query")

                    if topics != None:
                        # subscribe asgardian to topics
                        key = f"{ip}:{port}"
                        if heimdallSelf._consumers.get(key) == None:
                            heimdallSelf._consumers[key] = []
                        heimdallSelf._consumers[key].extend(topics)

                        offset_table_updates = {}
                        # send current 
                        if flag == False:
                            # topics : list of strings of topics
                            for t in topics:
                                if t not in heimdallSelf._topics:
                                    heimdallSelf.createTopic(t)

                                # wait
                                while t not in heimdallSelf._topics:
                                    sleep(1)

                                offset_table_updates[t] = {}

                                for p in heimdallSelf._topics[t].partitions:
                                    offset_table_updates[t][p.partitionId] = p.offset

                        print(offset_table_updates)

                        self.request.sendall(
                            dumps(
                                heimdallSelf.wrapMessage({"offset_table":  offset_table_updates})
                            ).encode('utf-8')
                        )



                    elif query != None:
                        # consumer_identifier = f"{ip}:{port}"
                        # consumer = heimdallSelf._consumers[consumer_identifier]

                        messages_buffer = []
                        # get the new messages

                        # {
                        #     "topic": {"0": 0, "1": 0, "2": 0}
                        # }

                        for t in offset_table:
                            topic_class = heimdallSelf._topics[t]

                            for p in topic_class.partitions:
                                consumer_offset_p = int(offset_table[t][str(p.partitionId)])
                                partition_offset = int(p.offset)

                                if (partition_offset > consumer_offset_p):
                                    offset_table[t][str(p.partitionId)] = partition_offset
                                    messages_buffer.extend(p.messages[consumer_offset_p:partition_offset])

                        self.request.sendall(
                            dumps(
                                heimdallSelf.wrapMessage({"messages":  messages_buffer, "offset_table": offset_table})
                            ).encode('utf-8')
                        )
                        

                else:
                    pass
                
        return socketRequestHandler

    def createProducerSocketServer(self):
        """
        Sets up the socket server
        """
        self._threadedSocketServer = ThreadingTCPServer((self._ip, self._port), self.returnSocketHandler())

    def destroyServer(self):
        """
        Destroys the socket server
        """
        print(messages.MSG_STOPPING_SERVER_GRACEFULLY)
        self._threadedSocketServer.shutdown()

    def startServer(self):
        """
        Starts the socket server
        """
        print(messages.MSG_STARTING_SERVER.format(self._threadedSocketServerThread.name))
        self._threadedSocketServer.serve_forever()

    def serve(self):
        """
        Runs Heimdall, sets up the socket server and sets a timer to destroy server [WIP]
        """
        self.createProducerSocketServer()
        self._threadedSocketServerThread = threading.Thread(
            target=self.startServer,
            daemon=True
        )
        self._threadedSocketServerThread.start()

        timer = threading.Timer(interval=25.0, function=self.destroyServer, args=())
        timer.start()
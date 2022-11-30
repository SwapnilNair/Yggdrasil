import base64
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
        self._producers = []
        self._consumers = []

        self._heartBeatRequestRetryFailCount = 0

        self._heartbeatMessage = None

        # self._metadata = self.getMetaData()

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

    def parseMetaData(self, metadata):
        # create topics
        # create partitions
        pass

    def getMetaData(self):
        r = requests.get(f'http://{self._zookeeperIp}:{self._zookeeperPort}/metadata')
        data = r.json()["data"]
        return data

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
        k = 3
        for i in range(k):
            self.sendHeartBeat()
            sleep(3)

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
            # print(r)
            data = r.json()
            # print(data)
            # set heimdall id
            if data.get("heimdallId"):
                self._id = int(data.get("heimdallId"))
                print("MESSAGE[HEIMDALL] : Assigned ID : {}".format(self._id))
            
            # parse the metadata and create topics and partitions

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

    def partitionFunction(self, message):
        if not message.get("key"):
            return -1
        if type(message["key"]) is str:
            return ord(message["key"][-1]) % self._nBrokers
        elif type(message["key"]) is int:
            return message["key"] % self._nBrokers
        else:
            return -1

    def yeetToHeimdall(self, messages):
        # if partition exists in current heimdall, 
        #   acquire lock for partition, write to partition

        # create a socket to send data to other brokers
        pass

    def partitionIncomingMessages(self, topic, messages):
        partitions_buffer = {}
        for m in messages:
            # get partition number for each message
            p = self.partitionFunction(message=m)

            # handle partition error
            if p == -1:
                p = 0
            
            if not partitions_buffer.get(p):
                partitions_buffer[p] = []
            
            # add to partition buffer
            partitions_buffer[p].append(m)

        pprint(partitions_buffer)
        # # push partitions to respective heimdall
        # for k in partitions_buffer.keys():
        #     # find which heimdall the kth partition must go to
        #     t = threading.Thread(self.yeetToHeimdall, (partitions_buffer[k],))
        #     t.start()
    

    def decompressMessage(self, message, insist = True):
        try:
            assert(message['messages'])
        except:
            if insist:
                raise RuntimeError("JSON not in expected format")
            else:
                return message
        try:
            message['messages'] = zlib.decompress(base64.b64decode(message['messages']))
        except:
            raise RuntimeError("Could not decode contents")
        
        try:
            message['messages'] = loads(message['messages'])
        except:
            raise RuntimeError("Could interpret unzipped contents")
        return message
    

    def returnSocketHandler(self):
        heimdallSelf = self
        class socketRequestHandler(BaseRequestHandler):
            """
            Handler for the socket server
            """
            def handle(self):
                data = loads(str(self.request.recv(4096), 'utf-8'))
                print(messages.MSG_DATA_RECEIVED.format(len(data),self.client_address))

                nodeType = data["nodeType"]

                if nodeType == "norse":
                    # If norse sends messages here => THIS HEIMDALL IS THE LEADER
                    # new messages coming in
                    # partition and yeet to respective heimdall
                    # print()
                    heimdallSelf.partitionIncomingMessages(
                        data["message"]["topic"],
                        heimdallSelf.decompressMessage(data["message"])["messages"]
                    )
                    pass
                elif nodeType == "heimdall":
                    # sync partitions
                    pass
                elif nodeType == "asgardian":
                    # requests messages
                    pass
                else:
                    pass

                cur_thread = threading.current_thread()
                response = bytes("{}: {}".format(cur_thread.name, data), 'utf-8')
                self.request.sendall(response)
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

        timer = threading.Timer(interval=10.0, function=self.destroyServer, args=())
        timer.start()
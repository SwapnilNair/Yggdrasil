import threading
from json import dumps, loads
from socketserver import BaseRequestHandler, ThreadingTCPServer
from time import sleep
from uuid import uuid4

import errors
import messages
import requests

#from . import topic


class Heimdall():
    """
    """
    def __init__(self, port, ip="localhost", ) -> None:
        # initialize stuff
        self._id = 0
        self._ip = ip
        self._port = int(port)
        self._zookeeperIp = "localhost"
        self._zookeeperPort = 5000
        self._healthEndPoint = "health"
        self._topics = {}
        self._producers = []
        self._consumers = []

        self._heartBeatRequestRetryFailCount = 0

        self._heartbeatMessage = {
            "heimdallId": str(self._id),
            "heimdallIp": str(self._ip),
            "heimdallPort": str(self._port),
        }

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
        # listen to producer
        # listen to consumer


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
        k = 100
        for i in range(k):
            self.sendHeartBeat()
            sleep(2)

    def sendHeartBeat(self):
        """
        Makes a heartbeat request to Zookeeper 
        """
        print(messages.MSG_SENDING_HEARTBEAT.format(self._zookeeperIp, self._zookeeperPort))
        try:
            # make the heartbeat request
            r = requests.post(
                url=f'http://{self._zookeeperIp}:{self._zookeeperPort}/{self._healthEndPoint}',
                json=self._heartbeatMessage
            )
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
        # create a socket to send data to other brokers
        pass

    def partitionIncomingMessages(self, messages):
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

        # push partitions to respective heimdall
        for k in partitions_buffer.keys():
            # find which heimdall the kth partition must go to
            t = threading.Thread(self.yeetToHeimdall, (partitions_buffer[k],))
            t.start()
    
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

    def createProducerSocketServer(self):
        """
        Sets up the socket server
        """
        self._threadedSocketServer = ThreadingTCPServer((self._ip, self._port), self.socketRequestHandler)

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

        timer = threading.Timer(interval=60.0, function=self.destroyServer, args=())
        timer.start()
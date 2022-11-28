import threading
from json import dumps, loads
from socketserver import BaseRequestHandler, ThreadingTCPServer
from time import sleep
from uuid import uuid4

import errors
import messages
import requests


class Heimdall():
    """
    """
    def __init__(self, port, ip="localhost", ) -> None:
        # initialize stuff
        self._id = uuid4()
        self._ip = ip
        self._port = int(port)
        self._zookeeperIp = "localhost"
        self._zookeeperPort = 5000
        self._healthEndPoint = "health"
        self._topics = []
        self._producers = []
        self._consumers = []

        self._heartBeatRequestRetryFailCount = 0

        self._heartbeatMessage = {
            "heimdallId": str(self._id),
            "heimdallIp": str(self._ip),
            "heimdallPort": str(self._port),
        }

        # replication

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
        k = 5
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

    class socketRequestHandler(BaseRequestHandler):
        def handle(self):
            data = loads(str(self.request.recv(4096), 'utf-8'))
            print(messages.MSG_DATA_RECEIVED.format(len(data),self.client_address))
            cur_thread = threading.current_thread()
            response = bytes("{}: {}".format(cur_thread.name, data), 'utf-8')
            self.request.sendall(response)

    def createProducerSocketServer(self):
        self._threadedSocketServer = ThreadingTCPServer((self._ip, self._port), self.socketRequestHandler)

    def destroyServer(self):
        print(messages.MSG_STOPPING_SERVER_GRACEFULLY)
        self._threadedSocketServer.shutdown()

    def startServer(self):
        print(messages.MSG_STARTING_SERVER.format(self._threadedSocketServerThread.name))
        self._threadedSocketServer.serve_forever()


    def serve(self):
        self.createProducerSocketServer()
        self._threadedSocketServerThread = threading.Thread(
            target=self.startServer,
            daemon=True
        )
        self._threadedSocketServerThread.start()

        timer = threading.Timer(interval=60.0, function=self.destroyServer, args=())
        timer.start()
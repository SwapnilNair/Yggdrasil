from uuid import uuid4
import _thread
from time import sleep
import requests
from json import dumps
import errors
import messages

class Heimdall():
    """
    """
    def __init__(self, port, ip="localhost", ) -> None:
        # initialize stuff
        self._id = uuid4()
        self._ip = ip
        self._port = port
        self._zookeeperIp = "localhost"
        self._zookeeperPort = 5000
        self._healthEndPoint = "health"
        self._topics = []
        self._producers = []
        self._consumers = []

        self._heartBeatRequestRetryFailCount = 0

        self._heartbeatMessage = {
            "heimdallId": str(self._id),
            "heimdallIp": self._ip,
            "heimdallPort": self._port,
        }

        # replication

        # create odin heartbeat process
        self._healthThread = None
        self.initHealthProc()

        # listen to producer
        # listen to consumer
    
    def initHealthProc(self):
        """
        Initialize the thread to serve the health client
        """
        self._healthThread = _thread.start_new_thread(self.serveHealthClient, ())

    def serveHealthClient(self):
        """
        Serves client to send heartbeats to Zookeeper
        """
        k = 10
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
                _thread.exit()
            # increment the fail count
            else:
                print(errors.ERROR_ODIN_HEARTBEAT_REQUEST_RETRY)
                self._heartBeatRequestRetryFailCount += 1

    def serve(self):
        while True:
            pass


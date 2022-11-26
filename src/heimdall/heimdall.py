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

        self._heartbeatMessage = dumps({
            "heimdallId": str(self._id),
            "heimdallIp": self._ip,
            "heimdallPort": self._port,
        })

        # replication

        # create odin heartbeat process
        self._healthThread = None
        self.initHealthProc()

        # listen to producer
        # listen to consumer
    
    def initHealthProc(self):
        self._healthThread = _thread.start_new_thread(self.serveHealthClient, ())

    def serveHealthClient(self):
        k = 10
        for i in range(k):
            self.sendHeartBeat()
            sleep(2)

    def sendHeartBeat(self):
        print(messages.MSG_SENDING_HEARTBEAT)
        try:
            r = requests.request(
                method="POST",
                url=f'http://{self._zookeeperIp}:{self._zookeeperPort}/{self._healthEndPoint}',
                data=self._heartbeatMessage
            )
        except requests.exceptions.ConnectionError as err:
            print(errors.ERROR_ODIN_UNAVAILABLE)
            _thread.exit()

    def serve(self):
        while True:
            pass


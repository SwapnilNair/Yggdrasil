import _thread
import json
import logging
import os
import threading as th

from flask import Flask, request

# logging.basicConfig(filename='eventlog.log', level=logging.INFO)

#data = 'Hello from the other side...'

'''class EndpointAction(object):
    def __init__(self, action):
        self.action = action
        self.response = Response(status=200, headers={})

    def __call__(self, *args):
        self.action()
        return self.response
'''

BASE_LOG_PATH = "C:/logs/"

class Odin():
    '''
        Class variables
        healthapi = Flask application object
        metadata = 
    '''
    _bufferForceFlushTimer = None
    healthapi = None
    metadata = ""
    leader = 0
    

    def __init__(self,name):
        self.heimdallId = 0
        self.heimdallIdLock = th.Lock()

        self.metafile = './data/metadata.json'

        self.healthThreadNo = None
        self.healthapi = Flask(name)
        self.isleaderElectionCurrentlyHappening = True

        self.metadataLock =  th.Lock()

        '''
            Endpoint setups
        '''
        self.addEndpoint(endpoint='/health',endpoint_name='health',handler=self.handleHealthEndpoint)
        self.addEndpointGet(endpoint='/metadata',endpoint_name='metadata',handler=self.metadataEndpoint)
        self.addEndpointGet(endpoint='/leader',endpoint_name='leader',handler=self.leaderInfoEndpoint)
        self.addEndpoint(endpoint='/updateMetadata',endpoint_name='updateMetadata',handler = self.updateMetadata)
        '''
            Timers for brokers
        '''
        self.b1_timer = None
        self.b2_timer = None
        self.b3_timer = None


        with open(self.metafile, 'r') as f:
            self.metadata = json.load(f)
        print(self.metadata)

        # reset
        for i in self.metadata["heimdalls"]:
            self.metadata["heimdalls"][i] = {
                "ip": "",
                "port": ""
            }

        # TODO : cleanup | rebalance partitions!!

        self.leaderElection()

        '''
            Healthapi is the flask application
        '''
    def run(self):
        self.healthapi.run()

        '''
            Function add decorators
        '''
    def addEndpoint(self,endpoint=None,endpoint_name = None,handler=None):
        self.healthapi.add_url_rule(endpoint, endpoint_name,handler,methods=['POST'])

    def addEndpointGet(self,endpoint=None,endpoint_name = None,handler=None):
        self.healthapi.add_url_rule(endpoint, endpoint_name,handler,methods=['GET'])

    '''
        Server threading
    '''
    def serve(self):
        self.healthThreadNo = _thread.start_new_thread(self.run, ())
        while(True):
            pass


    '''
        Endpoints
    '''           

    '''
        Reset Heimdall when it goes damaar
    '''
    def resetHeimdall(self, id):
        self.metadata["heimdalls"][str(id)] = {
            "ip": "",
            "port": ""
        }

    '''
        Heimdall damaar handling 
    '''    
    def handleHeimdallDeath(self,x):
        print("ERROR[ODIN] : Broker " + str(x) + " died")
        # update metadata
        self.resetHeimdall(x)
        self.leaderElection()
        
    '''
        Heartbeats ,id allotment and damaar detection
    '''
    def handleHealthEndpoint(self):   
        rcvd_from = request.json['heimdallId']
        # print(request.json)
        print("MESSAGE[ODIN] : Received Heartbeat from broker " + request.json['heimdallId'] + " at "+ request.json['heimdallIp'] + " at port " + request.json['heimdallPort'])
        #log here
        metadataJSON = {'data':self.metadata}

        # if new broker, add broker info to metadata and send broker its assigned id
        if int(rcvd_from) == -1:
            self.heimdallIdLock.acquire(blocking=True)
            self.heimdallId += 1
            self.metadata['heimdalls'][str(self.heimdallId)]['ip'] = request.json['heimdallIp']
            self.metadata['heimdalls'][str(self.heimdallId)]['port'] = request.json['heimdallPort']
            self.leaderElection()
            metadataJSON = {'data':self.metadata, 'heimdallId': self.heimdallId}
            self.heimdallIdLock.release()
            return json.dumps(metadataJSON)

        if int(rcvd_from) == 1:
            if self.b1_timer != None:
                self.b1_timer.cancel()
                print("stopping timer")
            print("Starting new timer...")
            self.b1_timer = th.Timer(4,self.handleHeimdallDeath,(1,))
            self.b1_timer.start()
            
        if int(rcvd_from) == 2:
            if self.b2_timer != None:
                self.b2_timer.cancel()
            self.b2_timer = th.Timer(4,self.handleHeimdallDeath,(2,))
            self.b2_timer.start()

        if int(rcvd_from) == 3:
            if self.b3_timer != None:
                self.b3_timer.cancel()
            self.b3_timer = th.Timer(4,self.handleHeimdallDeath,(3,))
            self.b3_timer.start()

        return json.dumps(metadataJSON)
    
    '''
        Metadata endpoint 
    '''
    def metadataEndpoint(self):
        #Returned string initially, but I think this is way more convenient for comms
        metadataJSON = json.dumps({'data':self.metadata})
        return metadataJSON

    '''
        Return leader broker id
    '''
    def leaderInfoEndpoint(self):
        if self.isleaderElectionCurrentlyHappening == True:
            return json.dumps({"leader": "-1"})
        leaderInfo = self.metadata['leader']
        return json.dumps({"leader": self.metadata["heimdalls"][leaderInfo]})

    '''
        Round robin leader election
    '''
    def leaderElection(self):
        print("MESSAGE[ODIN] : Conducting leader election")
        self.isleaderElectionCurrentlyHappening = True
        # first heimdall found that is active
        for h,v in self.metadata["heimdalls"].items():
            if v["ip"] != "" and v["port"] != "":
                self.leader = int(h)
                self.metadata["leader"] = h
                self.isleaderElectionCurrentlyHappening = False
                print("MESSAGE[ODIN] : New leader elected : {}".format(self.leader))
                break
            else:
                print("ERROR[ODIN] : No Heimdalls active! Awaiting Heimdall connections!")
                self.isleaderElectionCurrentlyHappening = True
    
    '''
    Update metadata every cycle
    '''

    def updateMetadata(self):
        # newMetadata = request.json["metadata"]
        # self.metadata = newMetadata
        # print("WRITING NEW META DATA TO FILE")
        # with open(self.metafile, "w") as f:
        #     f.write(json.dumps(newMetadata))

        self.metadataLock.acquire(blocking=True)
        topic = request.json["topic"]
        path = BASE_LOG_PATH+topic

        try:
            os.mkdir(path)
        except OSError as error:
            pass

        print("MESSAGE[ODIN] : MAKING TOPIC : {}".format(topic))


        # make topic and append
        newTopicMetaData = {
            "name": topic,
            "logPath": path,
            "partitions": []
        }

        for i in range(0, 3):
            newTopicMetaData["partitions"].append(
                {
                    "partitionId": str(i+1),
                    "heimdallId": str(i+1),
                    "isReplica": False,
                    "logPath": f"{BASE_LOG_PATH}{topic}/partition_{i+1}.json",
                    "offset": 0
                }
            )

        self.metadata["topics"][topic] = newTopicMetaData

        print(self.metadata)

        self.metadataLock.release()

        return '1'
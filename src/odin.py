from flask import Flask ,request  
import _thread,logging
import json
import threading as th 
logging.basicConfig(filename='eventlog.log', level=logging.INFO)

#data = 'Hello from the other side...'

'''class EndpointAction(object):
    def __init__(self, action):
        self.action = action
        self.response = Response(status=200, headers={})

    def __call__(self, *args):
        self.action()
        return self.response
'''

class Odin():
    '''
        Class variables
        healthapi = Flask application object
        metadata = 
    '''
    brokerid = 0
    _bufferForceFlushTimer = None
    healthapi = None
    metadata = ""
    leader = 0
    metafile = open('./data/metadata.json','r')
    metadata = json.load(metafile)

    def __init__(self,name):
        self.healthThreadNo = None
        self.healthapi = Flask(name)
        '''
            Endpoint setups
        '''
        self.addEndpoint(endpoint='/health',endpoint_name='health',handler=self.handleHealthEndpoint)
        self.addEndpointget(endpoint='/metadata',endpoint_name='metadata',handler=self.metadataEndpoint)
        self.addEndpoint(endpoint='/leader',endpoint_name='leader',handler=self.leaderInfoEndpoint)

        self.b1_timer = None
        self.b2_timer = None
        self.b3_timer = None

    def run(self):
        self.healthapi.run()
        '''
            Function add decorators
        '''
    def addEndpoint(self,endpoint=None,endpoint_name = None,handler=None):
        self.healthapi.add_url_rule(endpoint, endpoint_name,handler,methods=['POST'])

    def addEndpointget(self,endpoint=None,endpoint_name = None,handler=None):
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

    def brokerded(self,x):
        print("Broker " + str(x) + "died")
        return 1

    def handleHealthEndpoint(self):   
        rcvd_from = request.json['heimdallId']
        print(request.json)
        print("MESSAGE[ODIN] : Received Heartbeat from broker " +request.json['heimdallId'] " at "+ request.json['heimdallIp'] + " at port " + request.json['heimdallPort'])
        #log here

        # if new broker, add broker info to metadata and send broker its assigned id
        if int(rcvd_from) == -1:
            self.brokerid +=1
            self.metadata['heimdalls'][self.brokerid]['ip'] = request.json['heimdallIp']
            self.metadata['heimdalls'][self.brokerid]['port'] = request.json['heimdallPort']

        if int(rcvd_from) == 0:
            if self.b1_timer != None:
                self.b1_timer.cancel()
                print("stopping timer")
            print("Starting new timer...")
            self.b1_timer = th.Timer(4,self.brokerded,(0,))
            self.b1_timer.start()
            
        if int(rcvd_from) == 1:
            if self.b2_timer != None:
                self.b2_timer.cancel()
            self.b2_timer = th.Timer(4,self.brokerded,(1,))
            self.b2_timer.start()

        if int(rcvd_from) == 2:
            if self.b3_timer != None:
                self.b3_timer.cancel()
            self.b3_timer = th.Timer(4,self.brokerded,(2,))
            self.b2_timer.start()

        metadataJSON = json.dumps({'data':self.metadata,'brokerid':self.})
        return metadataJSON
    


    def metadataEndpoint(self):
        #Returned string initially, but I think this is way more convenient for comms
        metadataJSON = json.dumps({'data':self.metadata})
        return metadataJSON

    def leaderInfoEndpoint(self):
        leaderInfo = self.metadata['leader']
        return leaderInfo

    def leaderelection(self,x):
        self.leader = (x+1)%3
        self.metadata['leader']
        return self.leader


from flask import Flask ,request  
import _thread,logging
import json
#logging.basicConfig(filename='eventlog.log', level=logging.INFO)

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
    healthapi = None
    metadata = ""

    def __init__(self,name):
        self.healthThreadNo = None
        self.healthapi = Flask(name)
        '''
            Endpoint setups
        '''
        self.addEndpoint(endpoint='/health',endpoint_name='health',handler=self.handleHealthEndpoint)
        self.addEndpointget(endpoint='/metadata',endpoint_name='metadata',handler=self.metadataEndpoint)
        self.addEndpoint(endpoint='/leader',endpoint_name='leader',handler=self.leaderInfoEndpoint)

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
    def handleHealthEndpoint(self):
        print("MESSAGE[ODIN] : Received Heartbeat from " + request.json['heimdallIp'] + " at port " + request.json['heimdallPort'])
        return '1'

    def metadataEndpoint(self):
        metafile = open('/home/pes1ug20cs452/Documents/YAK/data/odinMetadata.json','r')
        metadata = json.load(metafile)
        #print("Okay,this get request works")
        return metadata #Returned string initially, but I think this is way more convenient for comms
    
    def leaderInfoEndpoint(self):
        
        return leaderInfo

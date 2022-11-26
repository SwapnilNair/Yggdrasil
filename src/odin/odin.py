from flask import Flask ,request  
import _thread,logging

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
    healthapi = None
    def __init__(self,name):
        self.healthThreadNo = None
        self.healthapi = Flask(name)
        self.addEndpoint(endpoint='/health',endpoint_name='health',handler=self.handleHealthEndpoint)

    def run(self):
        self.healthapi.run()

    def addEndpoint(self,endpoint=None,endpoint_name = None,handler=None):
        self.healthapi.add_url_rule(endpoint, endpoint_name,handler,methods=['POST'])

    def serve(self):
        self.healthThreadNo = _thread.start_new_thread(self.run, ())
        while(True):
            pass

    def handleHealthEndpoint(self):
        print("MESSAGE[ODIN] : Received Heartbeat from " + request.json['heimdallIp'] + " at port " + request.json['heimdallPort'])
        return '1'
  
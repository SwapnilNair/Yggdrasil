import json
import os
import random
import sys
import time

sys.path.append(os.getcwd()+"/src/norse")

import norse

port = sys.argv[1]
ip = sys.argv[2]
bufferSize = int(sys.argv[3])


producer = norse.Norse(port=port, ip = ip, bufferSize = bufferSize)

print(os.getcwd())

with open('./data/dataset.json', "r") as jsonfile:
    time.sleep(random.uniform(3, 6))
    data = json.loads(jsonfile.read())
    #print(len(data))
    for message in data:
        producer.produceMessage(message=message, topic="Orders")
import json
import sys
import os
sys.path.append(os.getcwd()+"/src/norse")

import norse

port = sys.argv[1]
ip = sys.argv[2]
bufferSize = int(sys.argv[3])


producer = norse.Norse(port=port, ip = ip, bufferSize = bufferSize)

with open('C:/Users/kavin/OneDrive/Desktop/sem 5/bd/YAK/BD1_368_409_413_452/data/dataset.json', "r") as jsonfile:
    data = json.loads(jsonfile.read())
    #print(len(data))
    for message in data:
        producer.produceMessage(message=message, topic="Orders")
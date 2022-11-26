import norse
import json
import sys

port = sys.argv[1]

producer = norse.Norse(port=port)

with open("../data/dataset.json", "r") as jsonfile:
    for message in json.loads(jsonfile.read()):
        producer.publish(message=message, topic="Orders")
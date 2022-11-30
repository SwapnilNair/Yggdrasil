import os
import sys

sys.path.append(os.getcwd()+"/src")

import asgardian

ip = sys.argv[2]
port = sys.argv[1]
id = sys.argv[3]
flag = False

try:
    flag = sys.argv[4]
except:
    print("Flag not mentioned")
    

consumer = asgardian.Asgardian(
    ip = ip,
    port=port,
    id = id,
    flag = flag

)

topics = ["Orders", "SomethingElse"]
consumer.subscribe(topics)

while True:
    try:
        for i in consumer.messageQueue:
            print(i)
    except:
        pass
        #consumer.close()
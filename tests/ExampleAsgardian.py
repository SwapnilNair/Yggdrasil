import sys
import os
sys.path.append(os.getcwd()+"/src/asgardian")

import asgardian

ip = sys.argv[2]
port = sys.argv[1]
id = sys.argv[3]
flag = False
'''
try:
    flag = sys.argv[3]
except:
    print("Flag not given")
'''

consumer = asgardian.Asgardian(
    ip = ip,
    port=port,
    id = id
    #flag = flag

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
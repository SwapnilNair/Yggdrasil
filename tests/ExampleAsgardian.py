import os
import sys

sys.path.append(os.getcwd()+"/src")

import asgardian

port = sys.argv[1]
ip = sys.argv[2]
id = sys.argv[3]
flag = False

try:
    flag = sys.argv[4]
except:
    print("Flag not mentioned")

if flag == '--from-beginning':
    flag = True

consumer = asgardian.Asgardian(
    ip = ip,
    port=port,
    id = id,
    flag = flag

)

topics = ["Orders", "SomethingElse"]
consumer.subscribe(topics)

consumer.request()
'''

while True:
    try:
        for i in consumer.messageQueue:
            print(i)
    except:
        pass
        #consumer.close()

'''
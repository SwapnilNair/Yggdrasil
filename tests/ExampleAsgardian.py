import asgardian
import sys

port = sys.argv[1]

consumer = asgardian.Asgardian(
    port=port
)

topics = ["Orders", "SomethingElse"]
consumer.subscribe(topics)

while True:
    try:
        for i in consumer.messageQueue:
            print(i)
    except:
        consumer.close()
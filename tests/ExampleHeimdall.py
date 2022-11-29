import json
import os
import random
import sys
import time

sys.path.append(os.getcwd()+"/src")
port = sys.argv[1]
import heimdall

broker_bhai = heimdall.Heimdall(
    port=port
)

broker_bhai.serve()
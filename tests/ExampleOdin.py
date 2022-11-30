import json
import os
import random
import sys
import time

sys.path.append(os.getcwd()+"/src")
import odin

port = sys.argv[1]

zookepa = odin.Odin(
    name= port
)

zookepa.serve()
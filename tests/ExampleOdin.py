import sys
import os
sys.path.append(os.getcwd()+"/src/odin")
import odin
port = sys.argv[1]

zookepa = odin.Odin(name= port
)

zookepa.serve()
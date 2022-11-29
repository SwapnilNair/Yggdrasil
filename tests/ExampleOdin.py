import sys
import os
sys.path.append("/home/pes1ug20cs452/Documents/YAK/src/odin")
print(sys.path)
import odin
port = sys.argv[1]

zookepa = odin.Odin(name= port
)

zookepa.serve()
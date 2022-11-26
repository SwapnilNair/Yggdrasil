import odin
import sys

port = sys.argv[1]

zookepa = odin.Odin(
    port=port
)

zookepa.serve()
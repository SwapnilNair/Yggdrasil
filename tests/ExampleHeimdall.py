import heimdall
import sys

port = sys.argv[1]

broker_bhai = heimdall.Heimdall(
    port=port
)

broker_bhai.serve()

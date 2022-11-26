import sys
import os
sys.path.append(os.getcwd()+"/src/heimdall")

import heimdall

port = sys.argv[1]

broker_bhai = heimdall.Heimdall(
    port=port
)

broker_bhai.serve()
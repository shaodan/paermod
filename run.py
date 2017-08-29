import time
from time import gmtime, strftime
import sys

j = 0
total = 100
if len(sys.argv)>1:
    try:
        total = int(sys.argv[1])
    except:
        print("int parse failed")

while j<total:
    print(strftime("%Y-%m-%d %H:%M:%S", gmtime()))
    sys.stdout.flush()
    time.sleep(10)
    # sum = 0
    # for i in xrange(10000000):
    #     sum += i
    j += 1

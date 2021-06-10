import json
from pathlib import Path
import sys
import time

#p = Path('./check.json')
path = '../data/check/status.json'
fin = bool(int(sys.argv[1]))
data = json.dumps({'crawl_complete':fin}, indent=4)

while True:
    try:
        with open(path, 'r') as f:
            j = json.loads(json.load(f))
            if j['crawl_complete'] == False:
                print('...crawl job not complete yet.')
            else:
                print('...crawl job complete! Starting extraction.')
                break
                
    except:
        print('...could not read crawl status file.')
    finally:
        time.sleep(5)

try:
    f = open(path, 'w')
except:
    f = open('check.json', 'x')
finally:
    json.dump(data, f)
    f.close()
    
print("After: ", end='')
try:
    with open(path, 'r') as f:
        a = json.loads(json.load(f))
        print(a)
except:
    print('nofile')
    
print(a['crawl_complete'] == False)

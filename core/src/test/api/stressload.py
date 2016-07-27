#!/usr/bin/python

import uuid
import urllib2
import json
import time

total_to_send = 1000
url = "http://localhost:9999/api/job"


for n in range(1, total_to_send):
    job = uuid.uuid4()
    print "Submiting job : ",job

    data = {
        "job_id":str(job),
        "adapterlist":"HelloWorld,PlusBang,HelloWorldPython",
        "seed": {
            "nodes": [ {"status":"new","type":"id","value":"somevaluehere"}],
            "edges": []
        },
        "job_config": {
            "depth": 4,
            "ttl":0, 
            "description":"job description"
        }
    }
    req = urllib2.Request(url)
    req.add_header('Content-Type', 'application/json')
    response = urllib2.urlopen(req, json.dumps(data))
    print response

    # Sleep every 10
    if (n % 10) == 0:
        print "Sleeping"
        time.sleep(1)
    
#curl -H "Content-Type: application/json" -X POST -d '' 

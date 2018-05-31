#!/usr/bin/python

import uuid
import urllib2
import json
import time

total_to_send = 100

url = "http://localhost:9999/api/job"


for n in range(1, total_to_send):

    data = {
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

    # Sleep every 100
    if (n % 100) == 0:
        print "Sleeping"
        time.sleep(1)
    



#!/bin/sh

JOB=' {"seed": { "nodes": [ {"status":"new","type":"id","value":"somevaluehere1"},{"status":"new","type":"id","value":"somevaluehere2"} ],  "edges": []}, "job_config":{"depth": 4, "ttl":0, "description":"job descripion", "adapters": { }}}'

curl -H "Content-Type: application/json" -X POST -d "$JOB"  http://localhost:9999/api/job

#!/bin/sh


JOB_ID="f42c2bf0-3244-11e6-9a54-64006a58e852"


JOB=' {"job_data": { "nodes": [ {"status":"new","type":"id","value":"somevaluehereNEW111"},{"status":"new","type":"id","value":"somevaluehereNEW222"} ],  "edges": []}, "job_config":{"depth": 4, "ttl":0, "description":"job description", "adapters": { "HelloWorld":{}, "PlusBang": {} }}}'


curl -H "Content-Type: application/json" -X POST -d "$JOB"  http://localhost:9999/api/job/$JOB_ID/insert




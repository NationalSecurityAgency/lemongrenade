#!/bin/sh

JOB_ID="f42c2bf0-3244-11e6-9a54-64006a58e852"
JOB='{ "onetimeadapterlist":["plusbang"], "nodes":["2","3","4"], "job_config":{"depth": 4, "ttl":0, "description":"Running hellowworld on adapters nodes 2, 3 and 4"}}'


curl -H "Content-Type: application/json" -X POST -d "$JOB" http://localhost:9999/api/job/$JOB_ID/execute_adapters_on_nodes

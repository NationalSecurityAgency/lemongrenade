#!/bin/sh
JOB_ID="1d8f5542-3c88-11e6-bba7-64006a58e852"

DATA='{ "reason":"PURGED" }'
curl -H "Content-Type: application/json"  -X PUT -d "$DATA"  http://localhost:9999/api/job/$JOB_ID/reset

#curl -H "Content-Type: application/json"  -X PUT  http://localhost:9999/api/job/$JOB_ID/reset






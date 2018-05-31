#!/bin/sh
JOB_ID="bc2a52c0-4ab5-11e6-abb2-000000000000"

DATA='{ "reason":"RESET_REASON", "job": { "jkasdfjkds" :{} }}'

DATA='{ "reason": "PURGED", "jobs": { "fb36d3c0-542f-11e6-9e32-000000000000": {}, "jaksdfas": {} }}'

#curl -H "Content-Type: application/json"  -X PUT -d "$DATA"  http://localhost:9999/api/job/$JOB_ID/reset
curl -H "Content-Type: application/json"  -X PUT -d "$DATA"  http://localhost:9999/api/jobs/reset
#curl -H "Content-Type: application/json"  -X POST -d "$DATA"  http://localhost:5050/reset

echo ""







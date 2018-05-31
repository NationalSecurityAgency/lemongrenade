#!/bin/sh

JOB_ID="ec11cbf4-5f09-11e6-b165-000000000000"

JOB='{  "post_action_job_config": { "nodes": ["7","10","4"], "description":"Running postaction on adapters nodes 2, 3 and 4", "depth": 4, "adapters": { "PostAction": { "HEY":"ONLY POST ACTIONS SHOULD SEE THIS"} }}}'

curl -H "Content-Type: application/json" -X POST -d "$JOB" http://localhost:9999/api/job/$JOB_ID/postaction

#!/bin/sh

JOB_ID="44e7429a-580c-11e6-837b-000000000000"
curl  -H "Content-Type: application/json"  -X put  http://localhost:9999/api/job/$JOB_ID/cancel 


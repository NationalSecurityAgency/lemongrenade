

JOBS='{ "reason":"global_reason", "jobs": {"2e81c3c0-4f80-11e6-a30b-000000000000":{}, "2def522e-4f80-11e6-b4c4-000000000000":{"reason":"nonglobalreason"}}}'

curl  -H "Content-Type: application/json"  -X put  http://localhost:9999/api/jobs/reset -d "$JOBS"

echo ""

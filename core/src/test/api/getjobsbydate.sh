


CREATEDBEFORE="2016-08-10T18:40:32.060Z";
CREATEDAFTER="2016-08-08T18:40:32.060Z";

curl  "http://localhost:9999/api/jobs?created_before=$CREATEDBEFORE&created_after=$CREATEDAFTER" | jq length
echo ""

echo "no AFTER query"
curl  "http://localhost:9999/api/jobs?created_before=$CREATEDBEFORE"  | jq length



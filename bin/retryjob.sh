#!/bin/bash

read -r -d '' USAGE << EOF || true
Usage: $0 <jobid>

Provides a command line way to retry jobs by jobid.
EOF

function usage(){
	echo "$USAGE" >&2
	exit 1
}

# ensure single uuid arg
[[ "$#" = "1" ]] || usage
[[ "$1" =~ ^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$ ]] || usage

curl --silent -H "Content-Type: application/json" -X PUT -d '{}' "http://localhost:9999/api/job/$1/retry"
echo

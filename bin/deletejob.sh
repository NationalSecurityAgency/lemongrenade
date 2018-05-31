#!/bin/bash

read -r -d '' USAGE << EOF
Usage: $0 <job-uuid>

Provides a command line way to delete jobs by uuid.
EOF

function usage(){
	echo "$USAGE" >&2
	exit 1
}

[[ "$#" = "1" ]] || usage

[[ "$1" =~ ^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$ ]] || usage

curl --silent -X DELETE "http://localhost:9999/rest/v2/delete/$1"
echo

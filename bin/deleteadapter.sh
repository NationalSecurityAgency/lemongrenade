#!/bin/bash

read -r -d '' USAGE << EOF || true
Usage: $0 <adapter-uuid>

Provides a command line way to delete adapter by uuid.

Note: this should only be used for offline (non-running adapters)
If you try to delete a running adapter, you could cause bad things
to happen. You should shut down the topologies and run this command
if you want to delete a running adapter.
EOF

function usage(){
	echo "$USAGE" >&2
	exit 1
}

# ensure single uuid arg
[[ "$#" = "1" ]] || usage
[[ "$1" =~ ^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$ ]] || usage

curl --silent -X DELETE "http://localhost:9999/api/adapter/$1"
echo

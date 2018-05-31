#!/bin/bash

read -r -d '' USAGE << EOF
Usage: $0 [-o] [reason] <job_uuids>

Provides a command line way to reset jobs by uuid.
EOF

set -eE
trap 'usage' EXIT

function usage(){
	echo "$USAGE" >&2
	exit 1
}

ids=()
do_flags=()
reason=''
for x in "$@"; do
	if [[ "$x" =~ ^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$ ]]; then
		ids+=("\"${x}\"")
	elif [[ "$x" = "-o" ]]; then
		do_flags+=('"overwrite"')
	elif [[ "$x" = -* ]]; then
		false
	elif [[ "$x" = *+($'\t'|$'\r'|$'\n'|\")* ]]; then
		false
	else
		reason="$x"
		declare -r reason
	fi
done
[[ "${#ids[@]}" != "0" ]]

IFS=','
printf -v json '{"reason":"%s","do":[%s],"ids":[%s]}' "$reason" "${do_flags[*]}" "${ids[*]}"

trap - EXIT
curl --silent -H "Content-Type: application/json" -X POST -d "$json" http://localhost:9999/rest/reset
echo

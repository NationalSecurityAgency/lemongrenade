#!/bin/bash

# kill all topologies
for x in `/opt/storm/bin/storm list | awk '/^-{30,}$/{y=1; next}y{print $1}'`; do
	/opt/storm/bin/storm kill "$x" -w 5 &
done
wait

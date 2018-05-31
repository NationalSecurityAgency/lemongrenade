#!/bin/bash

EXAMPLES="${0%/*}/../examples/examples-latest-jar-with-dependencies.jar"

/opt/storm/bin/storm jar "$EXAMPLES" lemongrenade.examples.adapters.FailAdapter FailAdapter "00000000-0000-0000-0000-000000000006"


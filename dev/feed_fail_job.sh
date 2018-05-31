#!/bin/bash

EXAMPLES="${0%/*}/../examples/examples-latest-jar-with-dependencies.jar"

/opt/storm/bin/storm jar "$EXAMPLES" lemongrenade.examples.test.FeedFailJob

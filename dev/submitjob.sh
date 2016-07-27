#!/usr/bin/env bash
# TODO: make this take a json file input arg (optional)

LGADAPTERJAR="../examples/target/examples-1.0-SNAPSHOT-jar-with-dependencies.jar"
LGCOREJAR="../core/target/core-1.0-SNAPSHOT-jar-with-dependencies.jar"


storm jar $LGCOREJAR lemongrenade.core.SubmitJob --testjob

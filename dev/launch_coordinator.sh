#!/bin/bash

CORE="${0%/*}/../core/core-latest-jar-with-dependencies.jar"

/opt/storm/bin/storm jar "$CORE" lemongrenade.core.coordinator.CoordinatorTopology CoordinatorTopology "00000000-0000-0000-0000-000000000000"

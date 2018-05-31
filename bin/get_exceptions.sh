#!/bin/bash

/opt/storm/bin/storm jar "${0%/*}"/../core/core-latest-jar-with-dependencies.jar lemongrenade.core.util.ExceptionWriter

#!/bin/bash

"${0%/*}"/launch_coordinator.sh &
"${0%/*}"/launch_plusbang_adapter.sh &
"${0%/*}"/launch_hello_adapter.sh &
"${0%/*}"/launch_fail_adapter.sh &

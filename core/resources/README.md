Check /dev for scripts showing how to launch requirements, topologies, and jobs.

Launching LemonGrenade Core:
Requirements:
mongod
zookeeper
rabbitmq
LemonGraph

Local Test:
launch SingleNodeClusterTest or LocalEngineTest to deploy the cluster.
    lemongrenade/examples/src/resources contains topology json configurations
Launch FeedJob to deploy your test job to the cluster

Deployed Test:
For a deployed cluster test a few additional storm services must be launched. These include:
nimbus (1 instance)
ui (1 instance, same box as nimbus)
supervisor (every node/box)
logviewer (1 on every supervisor box)
Check the scripts in /dev for deploying topologies.
Once topologies are deployed, use FeedJob to deploy custom job.

Using LemonGraph:
Install LemonGraph with instructions provided in package.
Kick off LG Server with the following: (-p determines the port, default of 8000 may be used by storm logviewer)
python -m LemonGraph.server -s -p 8001
LemonGraph installation notes available in its README.
LG regex will fail if the value is not present.

===Resource locations===
Place a "certs.json" at lemongrenade/core/src/main/resources containing the same keys as "certs.example.json"
Place ca, private key, and public key at lemongrenade/examples/multilang/resources

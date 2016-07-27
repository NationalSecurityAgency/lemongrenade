#First launch zookeeper and all storm processes
#service zookeeper start &
storm nimbus &
storm supervisor &
storm ui &
storm logviewer &

#Next, launch rabbitmq, redis requirements for lemongrenade
#sudo rabbitmq-server &
#sudo redis-server &

#Create latest jar from source (be in directory with LG's pom file)
#mvn package

# Start the API server
bash start_api_server &

#Last, launch each topology to deploy
bash launch_topologies.sh


#storm jar ../target/lemongrenade-1.0-SNAPSHOT-jar-with-dependencies.jar lemongrenade.test.EndlessFeed &
#storm jar ../target/lemongrenade-1.0-SNAPSHOT-jar-with-dependencies.jar lemongrenade.test.Feed100 &


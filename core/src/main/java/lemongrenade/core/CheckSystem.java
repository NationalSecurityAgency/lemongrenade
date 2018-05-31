package lemongrenade.core;

import com.mongodb.MongoClient;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lemongrenade.core.database.lemongraph.LemonGraph;
import lemongrenade.core.util.LGConstants;
import lemongrenade.core.util.LGProperties;
import org.apache.commons.cli.*;
import org.apache.storm.Config;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/** */
public class CheckSystem {

    /**
     * @return 'true' for success
     * */
    public boolean testStormNimbus() {
        String zookeeperhost = LGProperties.get("zookeeper.hostname");
        int zookeeperport = LGProperties.getInteger("zookeeper.port",2181); //2181

        Map conf = Utils.readStormConfig();
        conf.put(Config.NIMBUS_HOST, zookeeperhost);
        try {
            Nimbus.Client client = NimbusClient.getConfiguredClient(conf).getClient();
            ClusterSummary csummery = client.getClusterInfo();

            // look for nimbus up time
            int uptime = csummery.get_nimbus_uptime_secs();
            System.out.println("***** uptime "+uptime);
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
            return false;
        }
        return true;
    }

    /**
     * @return Incomplete test. Always returns 'false'
     */
    public boolean testZookeeper() {
        return false;
    }

    /**
     * Test connection to LemonGraph
     * @return 'true' if up, 'false' if down.
     */
    public boolean testLemonGraph() {
        LemonGraph lg = new LemonGraph();
        boolean isUp = lg.isApiUp();
        return isUp;
    }


    /**
     * @return 'true' if working, 'false' if not
     * */
    public boolean testMongoDb() {
        String connectString = LGProperties.get("database.mongo.hostname").toString()
                +":"+ LGProperties.get("database.mongo.port").toString();
        try {
            MongoClient mongoClient = new MongoClient(connectString);
            mongoClient.getAddress();
            mongoClient.close();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    /**
     * Test RabbitMQ by trying to connect to COORDINATORCMD queue
     * @return 'true' if connected to queue, 'false' otherwise
     * */
    public boolean testRabbit() {
        ConnectionFactory factory;
        Connection connection;
        Channel channel;
        boolean connectedToQueue = false;
        factory = new ConnectionFactory();
        factory.setHost(LGProperties.get("rabbit.hostname"));
        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.queueDeclare(LGConstants.LEMONGRENADE_COORDINATORCMD, true, false, false, null);
            connectedToQueue = true;
        }
        catch (java.io.IOException e) {
            System.out.println("Unable to connect to rabbitmq "+LGConstants.LEMONGRENADE_COORDINATORCMD+ " "+e.getMessage());
            connectedToQueue = false;
        }
        catch (TimeoutException e) {
            System.out.println("Timeout connecting to rabbitmq "+LGConstants.LEMONGRENADE_COORDINATORCMD+ " "+e.getMessage());
            connectedToQueue = false;
        }
        return connectedToQueue;
    }

    /** TODO: */
    public void printUsage() {
        System.out.println("");
    }

    public static void main(String[] args) throws Exception {
        Options options;
        options = new Options();
        CheckSystem checkSystem = new CheckSystem();
        options.addOption("h", "help", false, "Show help.");
        //options.addOption("j","jobfile",true, "JSON job file to read");
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        ArrayList<String> alist;
        try {
            cmd = parser.parse(options, args);
            if (cmd.hasOption("h")) {
                checkSystem.printUsage();
            }
        } catch (ParseException e) {
            checkSystem.printUsage();
        }

        // Run Tests
        boolean zooKeeperResult  = checkSystem.testZookeeper();
        boolean lemonGraphResult = checkSystem.testLemonGraph();
        boolean rabbitMqResult   = checkSystem.testRabbit();
        boolean mongoResult      = checkSystem.testMongoDb();
        boolean stormNimbusResult= checkSystem.testStormNimbus();

        // Print Results
        System.out.println();
        System.out.println();
        System.out.println("LemonGraph System Check");
        System.out.println("-----------------------");
        System.out.print("     Apache Storm Nimbus..... ");
        printResult(stormNimbusResult);
        System.out.print("     RabbitMQ................ ");
        printResult(rabbitMqResult);
        System.out.print("     Zookeeper .............. ");
        printResult(zooKeeperResult);
        System.out.print("     MongoDB ................ ");
        printResult(mongoResult);
        System.out.print("     LemonGraph ............. ");
        printResult(lemonGraphResult);

        System.out.println("Done.");
    }

    /**
     * @param result The result to be printed.
     * */
    public static void  printResult(boolean result) {
        if (result) {
            System.out.println("["+(char)27 + "[32mUP" + (char)27 + "[0m]");
        } else {
            System.out.println("["+(char)27 + "[31mDOWN" + (char)27 + "[0m]");
        }
    }
}


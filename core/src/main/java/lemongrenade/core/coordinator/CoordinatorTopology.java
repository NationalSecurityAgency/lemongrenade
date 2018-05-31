package lemongrenade.core.coordinator;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.latent.storm.rabbitmq.RabbitMQBolt;
import io.latent.storm.rabbitmq.config.*;
import lemongrenade.core.storm.AdapterSinkScheme;
import lemongrenade.core.storm.CommandSinkScheme;
import lemongrenade.core.storm.RabbitMQSpout;
import lemongrenade.core.templates.LGAdapter;
import lemongrenade.core.util.LGConstants;
import lemongrenade.core.util.LGProperties;
import lemongrenade.core.util.StringScheme;
import lemongrenade.core.util.StringSchemeLGCommand;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class CoordinatorTopology extends LGAdapter {
    private static final Logger log = LoggerFactory.getLogger(CoordinatorTopology.class);
    private BaseRichBolt COMMAND_BOLT = null;
    private StormTopology TOPOLOGY = null;
    public CoordinatorTopology(String id) {
        super(id);
    }
    public Logger getLogger() { return log; }
    public static final HashMap<String, Object> dead_letter = getDeadLetter();
    public static final HashMap<String, Object> queueArgs   = getQueueArgs();

    private static final HashMap<String, Object> getDeadLetter() {
        HashMap<String, Object> dead_letter = new HashMap<String, Object>();
        // Only deadletter the adapter queues
        dead_letter.put("x-dead-letter-exchange", "DeadLetter");
        return dead_letter;
    }

    private static final HashMap<String, Object> getQueueArgs() {
        HashMap<String, Object> args=  new HashMap<String, Object>();
        // NOTE: we no longer dead-letter the coordinator queues because they would only
        //       fail if there's some system level failure (e.g., lemongraph is gone)
        //       In that case, we want to just let the tuple requeue (storm backpressure -
        //       might be useful here.) If lemongraph comes back online, the system should
        //       notice it and start processing the queue immediately.
        //args.put("x-dead-letter-exchange", "DeadLetter");
        args.put("x-max-priority", 10);
        return args;
    }

    public StormTopology getTopology() {
        if(TOPOLOGY != null) {
            return TOPOLOGY;
        }

        //The LGRabbitMQSpout creates the queue/exchange on open if it doesn't already exist
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(LGProperties.get("rabbit.hostname"));
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            //Add Dead letter queue
            channel.exchangeDeclare(LGConstants.DEADLETTER_QUEUE, "fanout", true);
            channel.queueDeclare(LGConstants.DEADLETTER_QUEUE, true, false, false, null);
            channel.queueBind(LGConstants.DEADLETTER_QUEUE, LGConstants.DEADLETTER_QUEUE, "");

            channel.queueDeclare(LGConstants.LEMONGRENADE_COORDINATOR, true, false, false, CoordinatorTopology.queueArgs);
            channel.exchangeDeclare(LGConstants.LEMONGRENADE_COORDINATOR, "fanout");
            channel.queueBind(LGConstants.LEMONGRENADE_COORDINATOR, LGConstants.LEMONGRENADE_COORDINATOR, "");

            //Creates the COORDINATORCMD rabbit queue
            channel.queueDeclare(LGConstants.LEMONGRENADE_COORDINATORCMD, true, false, false, CoordinatorTopology.queueArgs);
            channel.exchangeDeclare(LGConstants.LEMONGRENADE_COORDINATORCMD, "fanout");
            channel.queueBind(LGConstants.LEMONGRENADE_COORDINATORCMD, LGConstants.LEMONGRENADE_COORDINATORCMD, "");

            channel.close();
            connection.close();
        } catch (Exception ex){
            //TODO: Actually handle this when the code is moved
            ex.printStackTrace();
        }

        ConnectionConfig connectionConfig = new ConnectionConfig(LGProperties.get("rabbit.hostname"),
                LGProperties.getInteger("rabbit.port", 5672),
                LGProperties.get("rabbit.user"),
                LGProperties.get("rabbit.password"),
                ConnectionFactory.DEFAULT_VHOST,
                10); // host, port, username, password, virtualHost, heartBeat

        ConsumerConfig spoutConfig = new ConsumerConfigBuilder().connection(connectionConfig)
                .queue(LGConstants.LEMONGRENADE_COORDINATOR)
                .prefetch(LGProperties.getInteger("rabbit.prefetch.messages", 250))
                .requeueOnFail()
                .build();

        ProducerConfig sinkConfig = new ProducerConfigBuilder().connection(connectionConfig).build();

        /* Build CoordinatorCommandSpout */
        ConsumerConfig spoutConfigCommand = new ConsumerConfigBuilder().connection(connectionConfig)
                .queue(LGConstants.LEMONGRENADE_COORDINATORCMD)
                .prefetch(LGProperties.getInteger("rabbit.prefetch.messages", 250))
                .requeueOnFail()
                .build();

        /* ****************************** Build Topology ***************************************** */
        TopologyBuilder builder = new TopologyBuilder();

        /* Add Coordinator to TOPOLOGY */
        builder.setSpout("input", new RabbitMQSpout(new StringScheme()), LGProperties.getInteger("rabbit.spout.threads", 1))
                .addConfigurations(spoutConfig.asMap())
                .setMaxSpoutPending(LGProperties.getInteger("rabbit.prefetch.messages", 250));

        int executors = LGProperties.getInteger("coordinator.threads", getParallelismHint());
        int tasks = getTaskCount();
        int workers = (int) getConfig().getOrDefault("topology.workers", 1);
        log.info("Setting bolt for "+getAdapterName()+". Workers:"+workers+" Executors:" + executors + " Tasks:" + tasks);
        builder.setBolt(LGConstants.LEMONGRENADE_COORDINATOR, new CoordinatorBolt(), executors)
                .setNumTasks(tasks)
                .fieldsGrouping("input",
                new Fields(LGConstants.LG_JOB_ID));
        builder.setBolt("rabbitmq-sink", new RabbitMQBolt(new AdapterSinkScheme()), LGProperties.getInteger("rabbit.sink.threads", 1))
                .addConfigurations(sinkConfig.asMap())
                .shuffleGrouping(LGConstants.LEMONGRENADE_COORDINATOR);

        /* Create the Command Adapter */

        /* Add CoordinatorCommand to TOPOLOGY */
        builder.setSpout("command-input", new RabbitMQSpout(new StringSchemeLGCommand()))
                .addConfigurations(spoutConfigCommand.asMap())
                .setMaxSpoutPending(LGProperties.getInteger("rabbit.prefetch.messages", 250));

        if(COMMAND_BOLT == null)
            COMMAND_BOLT = new CoordinatorCommandBolt();

        builder.setBolt(LGConstants.LEMONGRENADE_COORDINATORCMD, COMMAND_BOLT,
                LGProperties.getInteger("coordinator.command.threads", 10)).fieldsGrouping("command-input", new Fields("job_id"
        ));
        builder.setBolt("cmd-rabbitmq-sink", new RabbitMQBolt(new CommandSinkScheme()))
                .addConfigurations(sinkConfig.asMap())
                .shuffleGrouping(LGConstants.LEMONGRENADE_COORDINATORCMD);

        /* Add heartbeat bolt */
        builder.setBolt("heartbeat", new CoordinatorHeartbeatBolt());
        TOPOLOGY = builder.createTopology();
        return TOPOLOGY;
    }

    @Override public String getAdapterName() {
        return "Coordinator";
    }

    public void close() {
        if(COMMAND_BOLT !=  null)
            COMMAND_BOLT.cleanup();
        COMMAND_BOLT = null;
        TOPOLOGY = null;
    }

    /** */
    @Override public HashMap<String, String> getRequiredAttributes() {
        HashMap<String, String> requirements = new HashMap<String, String>();
        requirements.put("Coordinator", "*");
        return requirements;
    }

    // Not really needed here but we have to define it
    @Override public String getAdapterQuery() {
        return "n(value~/Coordinator/i)";
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("ERROR: Missing adapter ID");
            System.exit(-1);
        }
        CoordinatorTopology adapter = new CoordinatorTopology(args[1]);
        adapter.submitTopology(args);
        adapter.close(); //closes open connections before exiting
    }
}

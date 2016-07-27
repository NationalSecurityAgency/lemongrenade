package lemongrenade.core.coordinator;

import lemongrenade.core.storm.CommandSinkScheme;
import lemongrenade.core.util.StringSchemeLGCommand;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.latent.storm.rabbitmq.RabbitMQBolt;
import lemongrenade.core.templates.LGAdapter;
import lemongrenade.core.storm.RabbitMQSpout;
import io.latent.storm.rabbitmq.config.*;
import lemongrenade.core.util.LGProperties;
import lemongrenade.core.util.LGConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import lemongrenade.core.storm.AdapterSinkScheme;
import lemongrenade.core.util.StringScheme;
import java.util.HashMap;

public class CoordinatorTopology extends LGAdapter {
    private final Logger log = LoggerFactory.getLogger(getClass());
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

        //TODO: Will bomb when deploying... subclass RabbitMQSpout logic for the lemongrenade.core.coordinator
        //TODO: the LGRabbitMQSpout creates the queue/exchange on open if it doesn't already exist
        try{
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

        } catch (Exception ex){
            //TODO: Handle this scenario instead of printing the stack trace
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
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(LGProperties.get("rabbit.hostname"));
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.queueDeclare(LGConstants.LEMONGRENADE_COORDINATORCMD , true, false, false, CoordinatorTopology.queueArgs);
            channel.exchangeDeclare(LGConstants.LEMONGRENADE_COORDINATORCMD, "fanout");
            channel.queueBind(LGConstants.LEMONGRENADE_COORDINATORCMD , LGConstants.LEMONGRENADE_COORDINATORCMD , "");
        } catch (Exception ex){
            //TODO: Need to handle this better
            ex.printStackTrace();
        }
        ConsumerConfig spoutConfigCommand = new ConsumerConfigBuilder().connection(connectionConfig)
                .queue(LGConstants.LEMONGRENADE_COORDINATORCMD)
                .prefetch(LGProperties.getInteger("rabbit.prefetch.messages", 250))
                .requeueOnFail()
                .build();

        /* **** Build Topology **** */
        TopologyBuilder builder = new TopologyBuilder();

        /* Add Coordinator to topology */
        builder.setSpout("input", new RabbitMQSpout(new StringScheme()), LGProperties.getInteger("rabbit.spout.threads", 1))
                .addConfigurations(spoutConfig.asMap())
                .setMaxSpoutPending(LGProperties.getInteger("rabbit.prefetch.messages", 250));
        builder.setBolt(LGConstants.LEMONGRENADE_COORDINATOR, new CoordinatorBolt(), LGProperties.getInteger("coordinator.threads", 90)).fieldsGrouping("input",
                new Fields(LGConstants.LG_JOB_ID));
        builder.setBolt("rabbitmq-sink", new RabbitMQBolt(new AdapterSinkScheme()), LGProperties.getInteger("rabbit.sink.threads", 1))
                .addConfigurations(sinkConfig.asMap())
                .shuffleGrouping(LGConstants.LEMONGRENADE_COORDINATOR);

        /* Create the Command Adapter */

        /* Add CoordinatorCommand to topology */
        builder.setSpout("command-input", new RabbitMQSpout(new StringSchemeLGCommand()))
                .addConfigurations(spoutConfigCommand.asMap())
                .setMaxSpoutPending(LGProperties.getInteger("rabbit.prefetch.messages", 250));
        builder.setBolt(LGConstants.LEMONGRENADE_COORDINATORCMD , new CoordinatorCommandBolt(),
                LGProperties.getInteger("coordinator.command.threads", 10)).fieldsGrouping("command-input", new Fields("job_id"
        ));
        builder.setBolt("cmd-rabbitmq-sink", new RabbitMQBolt(new CommandSinkScheme()))
                .addConfigurations(sinkConfig.asMap())
                .shuffleGrouping(LGConstants.LEMONGRENADE_COORDINATORCMD);

        return builder.createTopology();
    }

    @Override
    public String getAdapterName() {
        return "Coordinator";
    }

    @Override
    public HashMap<String, String> getRequiredAttributes() {
        HashMap<String, String> requirements = new HashMap<String, String>();
        requirements.put("Coordinator", "*");
        return requirements;
    }

    // Not really needed here but we have to define it
    @Override
    public String getAdapterQuery() {
        return "n(value~/Coordinator/i)";
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1 ) {
            System.out.println("ERROR: Missing adapter ID");
            System.exit(-1);
        }
        CoordinatorTopology adapter = new CoordinatorTopology(args[1]);
        adapter.submitTopology(args);
    }
}

package lemongrenade.core.storm;

import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lemongrenade.core.coordinator.CoordinatorTopology;
import lemongrenade.core.util.LGProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;

public class LGRabbitMQSpout extends lemongrenade.core.storm.RabbitMQSpout {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private String adapterName;
    private String adapterId;
    private String queueName;

    public LGRabbitMQSpout(Scheme scheme, String adapterId, String adapterName, String queueName) {
        super(scheme);
        this.adapterName  = adapterName;
        this.adapterId    = adapterId;
        this.queueName    = queueName;
    }

    // open -> activate
    // activate -> deactivate
    // deactivate -> activate|close

    @Override
    public void open(final Map config,
                     final TopologyContext context,
                     final SpoutOutputCollector collector){
        try {
            // Create the queue in rabbit if it doesn't already exist
            ConnectionFactory factory = new ConnectionFactory();
            factory.setRequestedHeartbeat(60);//set the heartbeat timeout to 60 seconds
            factory.setHost(LGProperties.get("rabbit.hostname"));
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.queueDeclare(queueName, true, false, false, CoordinatorTopology.dead_letter);
            channel.exchangeDeclare(queueName, "fanout");
            channel.queueBind(queueName, queueName, "");
            channel.close();
            connection.close();

            // Superclass logic to start consuming from rabbit
            super.open(config, context, collector);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void activate(){
        super.activate();
        // Handle when a spout has been activated out of a deactivated mode.
        log.info("[LGRabbitMQSpout] ACTIVATE: "+adapterId);

    }



    @Override
    public void deactivate(){
        super.deactivate();
        // Handle when a spout has been deactivated.
        log.info("DEACTIVATE: " + adapterId);

    }

    @Override
    public void close(){
        super.close();
        // Handle when a spout is going to be shutdown.
        // *** NOTE: This is only called in a clean shutdown ***
        log.info("SHUTDOWN: " + adapterName+" "+adapterId);

    }
}

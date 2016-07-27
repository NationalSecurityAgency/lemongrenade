package lemongrenade.core.util;

import com.rabbitmq.client.*;
import io.latent.storm.rabbitmq.config.ConnectionConfig;
import lemongrenade.core.models.LGPayload;
import lemongrenade.core.templates.LGAdapter;
import org.apache.storm.Config;
import java.io.IOException;
import java.util.ArrayList;

public class GetDeadLetterMessages {
    public String queueName;
    public Config config = LGAdapter.DEFAULT_CONFIG;
    public String rabbitmq_host = (String) config.get("rabbit.hostname");
    public String rabbitmq_user = (String) config.get("rabbit.user");
    public String rabbitmq_pass = (String) config.get("rabbit.pass");
    public ConnectionFactory factory;
    public Connection connection;
    public Channel channel;
    public String consumerTag;
    public QueueingConsumer consumer;
    public ConnectionConfig connectionConfig = new ConnectionConfig(rabbitmq_host, 5672,
            rabbitmq_user, rabbitmq_pass, ConnectionFactory.DEFAULT_VHOST, 10);// password, virtualHost, heartbeat)

    public GetDeadLetterMessages() throws Exception {
        this("DeadLetter");
    }

    public GetDeadLetterMessages(String queueName) throws Exception {
        this.queueName = queueName;
        factory = new ConnectionFactory();
        factory.setHost(rabbitmq_host);
        connection = factory.newConnection();
        channel = connection.createChannel();

        //Add Dead letter queue
        channel.exchangeDeclare(queueName, "fanout", true);
        channel.queueDeclare(queueName, true, false, false, null);
        channel.queueBind(queueName, queueName, "");
        consumer = new QueueingConsumer(channel);
    }

    public void rePublish(GetResponse response) {
        Envelope envelope = response.getEnvelope();
        try {
            channel.basicPublish(envelope.getExchange(), envelope.getRoutingKey(), response.getProps(), response.getBody());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void printBody(byte[] body) {
        try {
            LGPayload payload = LGPayload.deserialize(body);
            System.out.println(payload.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void printResponse(GetResponse response) {
        byte[] body = response.getBody();
        printBody(body);
    }

    public GetResponse get() {
        boolean autoAck = false;
        GetResponse response = null;
        try {
            response = channel.basicGet(queueName, autoAck);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (response == null) {
            // No message retrieved.
        } else {
            AMQP.BasicProperties props = response.getProps();
            byte[] body = response.getBody();
            long deliveryTag = response.getEnvelope().getDeliveryTag();
            try {
                channel.basicNack(deliveryTag, false, false);
                //channel.basicAck(deliveryTag, false); // acknowledge receipt of the message
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return response;
    }

    public void processErrors(int maxResponses) {
        ArrayList<GetResponse> responses = new ArrayList<>();
        GetResponse response = null;

        //Get responses up to maxResponses off of DeadLetter queue
        do{
            response = this.get();
            if(response != null)
                responses.add(response);
        } while(response != null && responses.size() < maxResponses);


        //Print then republish all response to DeadLetter
        for (GetResponse getResponse : responses) {
            printResponse(getResponse);
            rePublish(getResponse);
        }
    }

    public static void main(String[] args) throws Exception {
        GetDeadLetterMessages DLM = new GetDeadLetterMessages();
        DLM.processErrors(100); //print and republish up to 100 errors from DeadLetter queue
        System.out.println("Done");
        System.exit(1); //closes consumer
    }
}

package lemongrenade.core.storm;

import com.rabbitmq.client.*;
import io.latent.storm.rabbitmq.Declarator;
import io.latent.storm.rabbitmq.ErrorReporter;
import io.latent.storm.rabbitmq.Message;
import io.latent.storm.rabbitmq.config.ConnectionConfig;
import lemongrenade.core.models.LGPayload;
import lemongrenade.core.util.LGConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Note; This file is forked from an Open Source Repository on GitHub
 *
 * Modifications were made to support dead-lettering with RabbitMQ
 *
 * ORIGINAL SOURCE REPOSITORY:
 * https://github.com/ppat/storm-rabbitmq
 *
 * ORIGINAL LICENSE (MIT):
 * https://github.com/ppat/storm-rabbitmq/blob/master/LICENSE
 */
public class RabbitMQConsumer implements Serializable {
    public static final long MS_WAIT_FOR_MESSAGE = 1L;

    public HashMap<Long, Object> messages;
    private final ConnectionFactory connectionFactory;
    private final static int MAX_RETRIES = 3;
    private final Address[] highAvailabilityHosts;
    private final int prefetchCount;
    private final String queueName;
    private final boolean requeueOnFail;
    private final Declarator declarator;
    private final ErrorReporter reporter;
    private final Logger logger;
    private Connection connection;
    private Channel channel;
    private QueueingConsumer consumer;
    private String consumerTag;

    public RabbitMQConsumer(ConnectionConfig connectionConfig,
                            int prefetchCount,
                            String queueName,
                            boolean requeueOnFail,
                            Declarator declarator,
                            ErrorReporter errorReporter) {
        this.messages = new HashMap<Long, Object>();
        this.connectionFactory = connectionConfig.asConnectionFactory();
        this.highAvailabilityHosts = connectionConfig.getHighAvailabilityHosts().toAddresses();
        this.prefetchCount = prefetchCount;
        this.queueName = queueName;
        this.requeueOnFail = requeueOnFail;
        this.declarator = declarator;

        this.reporter = errorReporter;
        this.logger = LoggerFactory.getLogger(RabbitMQConsumer.class);
    }

    public Message nextMessage() {
        reinitIfNecessary();
        if (consumerTag == null || consumer == null)
            return Message.NONE;
        try {
            QueueingConsumer.Delivery message = consumer.nextDelivery(MS_WAIT_FOR_MESSAGE);
            return Message.forDelivery(message);
        } catch (ShutdownSignalException sse) {
            reset();
            logger.error("shutdown signal received while attempting to get next message", sse);
            reporter.reportError(sse);
            return Message.NONE;
        } catch (InterruptedException ie) {
      /* nothing to do. timed out waiting for message */
            logger.debug("interrupted while waiting for message", ie);
            return Message.NONE;
        } catch (ConsumerCancelledException cce) {
      /* if the queue on the broker was deleted or node in the cluster containing the queue failed */
            reset();
            logger.error("consumer got cancelled while attempting to get next message", cce);
            reporter.reportError(cce);
            return Message.NONE;
        }
    }

    public void ack(Long msgId) {
        reinitIfNecessary();
        messages.remove(msgId);
        try {
            channel.basicAck(msgId, false);
        } catch (ShutdownSignalException sse) {
            reset();
            logger.error("shutdown signal received while attempting to ack message", sse);
            reporter.reportError(sse);
        } catch (Exception e) {
            logger.error("could not ack for msgId: " + msgId, e);
            reporter.reportError(e);
        }
    }

    //Increment times-sent custom header. Shows the amount of times this message has been sent.
    private HashMap<String, Object> getIncrementedHeader(Message.DeliveredMessage message) {
        Map<String, Object> headers;
        headers = message.getHeaders();
        if(headers == null || headers.size() == 0)
            headers = new HashMap<String, Object>();
        if(!headers.containsKey("times-sent")) {
            headers.put("times-sent", 1);
        }
        else {
            int times = (int) headers.get("times-sent");
            headers.put("times-sent", times+1);
        }
        return (HashMap<String, Object>) headers;
    }

    /**
     *  Fails are handled by allowing a message to be resent MAX_RETRIES. This counter is kept in the header
     *  in "times-sent". If this message fails more than MAX_RETRIES, we 'dead-letter' this message, which
     *  means we send the message to the 'dead-letter' queue where it will sit forever until it's cleared out
     *  However, we must also send the payload back to the coordinator with a status of ERROR (and with a
     *  possible error message) so that the coordinator can set the task and job status to ERROR. Otherwise, the
     *  end user never knows about the problem and the job will appear to never finish.
     *
     * @param msgId
     */
    public void fail(Long msgId) {//retry failures 2 times
        try {
            Message.DeliveredMessage message = (Message.DeliveredMessage) messages.get(msgId);
            HashMap<String, Object> headers = getIncrementedHeader(message);
            int times_sent = (int) headers.get("times-sent");
            if(times_sent  < this.MAX_RETRIES) {
                message = (Message.DeliveredMessage) messages.get(msgId);
                AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                        .contentType(message.getContentType())
                        .contentEncoding(message.getContentEncoding())
                        .headers(headers)
                        .priority(LGConstants.QUEUE_PRIORITY_ADAPTER_RESPONSE)
                        .build();
                channel.basicPublish(message.getExchange(),message.getRoutingKey(), properties, message.getBody());
                ack(msgId);
            } else {
                // First send a message to the coordinator

                byte[] body = message.getBody();
                try {
                    // TODO: why no log here?
                    System.out.println("*** Sending task FAILED payload back to coordinator for processing");
                    LGPayload payload = LGPayload.deserialize(body);
                    String taskId = payload.getTaskId();
                    System.out.println("*** FAILED TaskId:"+taskId);
                    payload.setPayloadType(LGConstants.LG_PAYLOAD_TYPE_ADAPTERRESPONSE_FAILURE);
                    try {
                        channel.basicPublish("", LGConstants.LEMONGRENADE_COORDINATOR
                                , new AMQP.BasicProperties.Builder().priority(LGConstants.QUEUE_PRIORITY_ADAPTER_RESPONSE).build()
                                , payload.toByteArray());
                    } catch (IOException e) {
                        System.out.println("UNABLE TO PUBLISH DEADLETTER FAILURE NOTICE to coordinator queue");
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
                deadLetter(msgId); //kill the old message regardless. It will be remade/resent if maxDeliveries isn't met.
            }
        }
        catch (Exception e) {
            System.out.println("Error: Could not retrieve message! for msgID "+msgId.toString()+".");
        }
        messages.remove(msgId);
    }


    public void failWithRedelivery(Long msgId) {
        reinitIfNecessary();
        try {
            channel.basicReject(msgId, true);
        } catch (ShutdownSignalException sse) {
            reset();
            logger.error("shutdown signal received while attempting to fail with redelivery", sse);
            reporter.reportError(sse);
        } catch (Exception e) {
            logger.error("could not fail with redelivery for msgId: " + msgId, e);
            reporter.reportError(e);
        }
    }

    public void deadLetter(Long msgId) {
        reinitIfNecessary();
        try {
            channel.basicReject(msgId, false);
        } catch (ShutdownSignalException sse) {
            reset();
            logger.error("shutdown signal received while attempting to fail with no redelivery", sse);
            reporter.reportError(sse);
        } catch (Exception e) {
            logger.error("could not fail with dead-lettering (when configured) for msgId: " + msgId, e);
            reporter.reportError(e);
        }
    }

    public void open() {
        try {
            connection = createConnection();
            channel = connection.createChannel();
            if (prefetchCount > 0) {
                logger.info("setting basic.qos / prefetch count to " + prefetchCount + " for " + queueName);
                channel.basicQos(prefetchCount);
            }
            // run any declaration prior to queue consumption
            declarator.execute(channel);

            consumer = new QueueingConsumer(channel);
            consumerTag = channel.basicConsume(queueName, isAutoAcking(), consumer);
        } catch (Exception e) {
            reset();
            logger.error("could not open listener on queue " + queueName);
            reporter.reportError(e);
        }
    }

    protected boolean isAutoAcking() {
        return false;
    }

    public void close() {
        try {
            if (channel != null && channel.isOpen()) {
                if (consumerTag != null) channel.basicCancel(consumerTag);
                channel.close();
            }
        } catch (Exception e) {
            logger.debug("error closing channel and/or cancelling consumer", e);
        }
        try {
            logger.info("closing connection to rabbitmq: " + connection);
            connection.close();
        } catch (Exception e) {
            logger.debug("error closing connection", e);
        }
        consumer = null;
        consumerTag = null;
        channel = null;
        connection = null;
    }

    private void reset() {
        consumerTag = null;
    }

    private void reinitIfNecessary() {
        if (consumerTag == null || consumer == null) {
            close();
            open();
        }
    }

    private Connection createConnection() throws IOException {
        Connection connection = null;
        try {
            connection = highAvailabilityHosts == null || highAvailabilityHosts.length == 0
                    ? connectionFactory.newConnection()
                    : connectionFactory.newConnection(highAvailabilityHosts);
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
        connection.addShutdownListener(new ShutdownListener() {
            @Override
            public void shutdownCompleted(ShutdownSignalException cause) {
                logger.error("shutdown signal received", cause);
                reporter.reportError(cause);
                reset();
            }
        });
        logger.info("connected to rabbitmq: " + connection + " for " + queueName);
        return connection;
    }
}



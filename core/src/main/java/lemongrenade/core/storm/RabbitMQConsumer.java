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
 * An abstraction on RabbitMQ client API to encapsulate interaction with RabbitMQ and de-couple Storm API from RabbitMQ API.
 *
 * @author peter@latent.io
 */
public class RabbitMQConsumer implements Serializable {
    private final Logger log = LoggerFactory.getLogger(getClass());
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
            logger.error("Shutdown signal received while attempting to get next message.", sse);
            reporter.reportError(sse);
            return Message.NONE;
        } catch (InterruptedException ie) {
      /* nothing to do. timed out waiting for message */
            logger.debug("Interrupted while waiting for message.", ie);
            return Message.NONE;
        } catch (ConsumerCancelledException cce) {
      /* if the queue on the broker was deleted or node in the cluster containing the queue failed */
            reset();
            logger.error("Consumer got cancelled while attempting to get next message.", cce);
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
            logger.error("Shutdown signal received while attempting to ack message.", sse);
            reporter.reportError(sse);
        } catch (Exception e) {
            logger.error("Could not ack for msgId: " + msgId, e);
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
     */
    public void fail(Long msgId) {//retry failures 2 times
        try {
            Message.DeliveredMessage message = (Message.DeliveredMessage) messages.get(msgId);
            HashMap<String, Object> headers = getIncrementedHeader(message);
            int times_sent = (int) headers.get("times-sent");
            if(times_sent  < this.MAX_RETRIES) { //retry the message when it fails 2 times
                message = (Message.DeliveredMessage) messages.get(msgId);
                byte[] body = message.getBody();
                LGPayload payload = LGPayload.deserialize(body);
                String taskId = payload.getTaskId();
                String jobId = payload.getJobId();
                log.info("*** Task failed, but max retries not met. Retrying ("+(times_sent+1)+"/"+this.MAX_RETRIES+"). " +
                        "RETRYING TaskId:"+taskId+" JobId:"+jobId);
                AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                        .contentType(message.getContentType())
                        .contentEncoding(message.getContentEncoding())
                        .headers(headers)
                        .priority(LGConstants.QUEUE_PRIORITY_ADAPTER_RESPONSE)
                        .build();
                channel.basicPublish(message.getExchange(),message.getRoutingKey(), properties, message.getBody());
                ack(msgId);
            } else {
                //First send a message to the coordinator
                byte[] body = message.getBody();
                try {
                    LGPayload payload = LGPayload.deserialize(body);
                    String taskId = payload.getTaskId();
                    String jobId = payload.getJobId();
                    log.info("*** Sending task FAILED payload back to coordinator for processing. " +
                            "FAILED TaskId:"+taskId+" JobId:"+jobId);
                    payload.setPayloadType(LGConstants.LG_PAYLOAD_TYPE_ADAPTERRESPONSE_FAILURE);
                    channel.basicPublish("", LGConstants.LEMONGRENADE_COORDINATOR
                                , new AMQP.BasicProperties.Builder().priority(LGConstants.QUEUE_PRIORITY_ADAPTER_RESPONSE).build()
                                , payload.toByteArray());
                } catch (Exception e) {
                    log.error("Failed to publish failure response to coordinator.");
                    e.printStackTrace();
                }
                deadLetter(msgId); //kill the old message regardless. It will be remade/resent if maxDeliveries isn't met.
            }
        } catch (Exception e) {
            log.info("Error: Could not retrieve message! for msgID " + msgId.toString() + ".");
            e.printStackTrace();
        }
        messages.remove(msgId);
    }

    public void failWithRedelivery(Long msgId) {
        reinitIfNecessary();
        try {
            channel.basicReject(msgId, true);
        } catch (ShutdownSignalException sse) {
            reset();
            logger.error("Shutdown signal received while attempting to fail with redelivery.", sse);
            reporter.reportError(sse);
        } catch (Exception e) {
            logger.error("Could not fail with redelivery for msgId: " + msgId, e);
            reporter.reportError(e);
        }
    }

    public void deadLetter(Long msgId) {
        reinitIfNecessary();
        try {
            channel.basicReject(msgId, false);
        } catch (ShutdownSignalException sse) {
            reset();
            logger.error("Shutdown signal received while attempting to fail with no redelivery.", sse);
            reporter.reportError(sse);
        } catch (Exception e) {
            logger.error("Could not fail with dead-lettering (when configured) for msgId: " + msgId, e);
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
            logger.error("Could not open listener on queue:" + queueName +".");
            e.printStackTrace();
            reset();
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
            logger.debug("Error closing channel and/or cancelling consumer.", e);
            e.printStackTrace();
        }
        try {
            if(connection != null && connection.isOpen()) {
                logger.info("Closing connection to RabbitMQ: " + connection);
                connection.close();
            }
        } catch (Exception e) {
            logger.debug("Error closing connection.", e);
            e.printStackTrace();
        }
        try {
            consumer.getChannel().close();
        }
        catch(Exception e) {
            log.error("Error closing RabbitMQ consumer.");
            e.printStackTrace();
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

    private Connection createConnection() throws IOException, TimeoutException {
        Connection connection = highAvailabilityHosts == null || highAvailabilityHosts.length == 0
                ? connectionFactory.newConnection()
                : connectionFactory.newConnection(highAvailabilityHosts);

        connection.addShutdownListener(cause -> {
            logger.error("Shutdown signal received", cause);
            reporter.reportError(cause);
            reset();
        });

        logger.info("Connected to rabbitmq: " + connection + " for " + queueName);
        return connection;
    }
}



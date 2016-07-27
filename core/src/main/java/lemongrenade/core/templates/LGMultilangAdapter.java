package lemongrenade.core.templates;

import lemongrenade.core.coordinator.AdapterManager;
import lemongrenade.core.coordinator.JobManager;
import lemongrenade.core.models.LGJob;
import lemongrenade.core.models.LGPayload;
import lemongrenade.core.util.ExceptionWriter;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import com.rabbitmq.client.ConnectionFactory;
import io.latent.storm.rabbitmq.RabbitMQBolt;
import io.latent.storm.rabbitmq.config.*;
import lemongrenade.core.storm.LGRabbitMQSpout;
import lemongrenade.core.storm.LGShellBolt;
import lemongrenade.core.util.LGConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import lemongrenade.core.util.LGProperties;
import lemongrenade.core.storm.MultilangCoordinatorSinkScheme;
import lemongrenade.core.util.StringScheme;
import java.util.Map;

public abstract class LGMultilangAdapter extends LGAdapter{

    private transient AdapterManager am;
    public LGMultilangAdapter(String id) {
        super(id);
        am = new AdapterManager();
    }
    private static final Logger log = LoggerFactory.getLogger(LGMultilangAdapter.class);

    public StormTopology getTopology() {

        ConnectionConfig connectionConfig = new ConnectionConfig(LGProperties.get("rabbit.hostname"),
                LGProperties.getInteger("rabbit.port", 5672),
                LGProperties.get("rabbit.user"),
                LGProperties.get("rabbit.password"),
                ConnectionFactory.DEFAULT_VHOST,
                10); // host, port, username, password, virtualHost, heartBeat

        ConsumerConfig spoutConfig = new ConsumerConfigBuilder().connection(connectionConfig)
                .queue(getQueueName())
                .prefetch(LGProperties.getInteger("rabbit.prefetch.messages", 250))
                .requeueOnFail()
                .build();

        ProducerConfig sinkConfig = new ProducerConfigBuilder().connection(connectionConfig).build();

        TopologyBuilder builder = new TopologyBuilder();

        // Populate Adapter Data Structures
        am.buildAdapterDataStructure( getAdapterId(),  getAdapterName(), getRequiredAttributes()
                , getAdapterQuery(),getAdapterDepth());

        builder.setSpout("input", new LGRabbitMQSpout(
                        new StringScheme(), getAdapterId(), getAdapterName(), getQueueName()),
                                LGProperties.getInteger("rabbit.spout.threads", 1))
                .addConfigurations(spoutConfig.asMap())
                .setMaxSpoutPending(LGProperties.getInteger("rabbit.prefetch.messages", 250));


        /**
         * Heartbeat bolt
         *
         */
        builder.setBolt("heartbeat", new BaseBasicBolt() {
            private transient Jedis jedis;

            @Override
            public void prepare(final Map config,
                                final TopologyContext context) {
                // Open redis connection; shouldn't be done in the constructor
                jedis = new Jedis(LGProperties.get("redis.hostname").toString());
            }

            @Override
            public Map<String, Object> getComponentConfiguration() {
                Config conf = new Config();
                int heartBeatTime = LGProperties.getInteger("adapter_heartbeat",20);
                log.info("Heartbeat tick time = "+heartBeatTime);
                conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, heartBeatTime);
                return conf;
            }

            protected boolean isTickTuple(Tuple tuple) {
                return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                       && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
            }

            @Override
            public void execute(Tuple tuple, BasicOutputCollector collector) {
                try {
                    if (isTickTuple(tuple)) {
                        // Heartbeat key is queuename:hb
                        String hbKey = getAdapterId()+ ":hb";
                        log.info("Received Heartbeat "+getAdapterName()+" "+getAdapterId());
                        jedis.hset(LGConstants.LGADAPTERSDATA, hbKey, Long.toString(System.currentTimeMillis()));
                    }
                } catch (Exception e) {
                    log.error("Bolt execute error: " + e.getMessage());
                }
            }

            // This is a dummy bolt - so no outputfields to declare
            public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}
        });

        builder.setBolt(getAdapterName(), new LGShellBolt(getAdapterType(), getScriptName()){
            private JobManager jobManager;
            private AdapterManager adapterManager;
            private OutputCollector oc;

            public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
                jobManager = new JobManager();
                adapterManager = new AdapterManager();
                oc = outputCollector;
                super.prepare(map,topologyContext, outputCollector);
            }

            @Override
            public void execute(Tuple input){
                LGPayload payload = (LGPayload) input.getValueByField(LGConstants.LG_PAYLOAD);

                // Check job state before allowing Adapter to do any processing.
                LGJob lg ;
                try {
                    lg = jobManager.getJob(payload.getJobId());
                }
                catch(org.mongodb.morphia.mapping.MappingException e) {
                    log.error(" Unable to map jobId " + payload.getJobId() + "   error:" + e.getMessage());
                    oc.reportError(e);
                    oc.fail(input);
                    return;
                }
                if (lg == null) {
                    log.error("Adapter received invalid job. "+payload.getJobId());
                    oc.ack(input);
                    return;
                }
                if (lg.getStatus() == LGJob.STATUS_STOPPED) {
                    log.info("Adapter received task for job, but job is STOPPED");
                    oc.ack(input);
                    return;
                }
                // If Job has expired, stop processing
                if (jobManager.hasJobExpired(lg)) {
                    oc.ack(input);
                    return;
                }

                adapterManager.setCurrentTaskId(getAdapterId(), payload.getTaskId());

                try {
                    super.execute(input);
                } catch (Exception e) {
                    // It's critical that we have the try/catch wrapper around the process block, because if the
                    // AdapterWriter does something that causes a problem, (nullPointer, JSONObject.get("badkey") we
                    // can not expect them to do the correct error handling. So, we gracefully catch it here and
                    // fail the tuple
                    log.error("Adapter threw an exception: Failing Tuple: " + e.getMessage());
                    //e.printStackTrace();//error handling here...
                    try {
                        ExceptionWriter writer = new ExceptionWriter();
                        String taskId = payload.getTaskId();
                        String requests = payload.getRequestNodes().toString();
                        writer.publishException(taskId, requests, e);
                    } catch (Exception e2) {
                        e2.printStackTrace();
                    }
                    oc.reportError(e);
                    oc.fail(input);
                }
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
                outputFieldsDeclarer.declare(new Fields(LGConstants.LG_JOB_ID, LGConstants.LG_PAYLOAD));
            }

            @Override
            public Map<String, Object> getComponentConfiguration() {
                return null;
            }

        }, getParallelismHint()).shuffleGrouping("input");

        builder.setBolt("output", new RabbitMQBolt(new MultilangCoordinatorSinkScheme()), LGProperties.getInteger("rabbit.sink.threads", 1))
                .addConfigurations(sinkConfig.asMap())
                .shuffleGrouping(getAdapterName());

        return builder.createTopology();
    }

    public abstract LGShellBolt.SupportedLanguages getAdapterType();
    public abstract String getScriptName();
}

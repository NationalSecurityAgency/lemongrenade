package lemongrenade.core.templates;

import com.rabbitmq.client.ConnectionFactory;
import io.latent.storm.rabbitmq.RabbitMQBolt;
import io.latent.storm.rabbitmq.config.*;
import lemongrenade.core.coordinator.AdapterManager;
import lemongrenade.core.coordinator.JobManager;
import lemongrenade.core.models.LGJob;
import lemongrenade.core.models.LGPayload;
import lemongrenade.core.storm.CoordinatorSinkScheme;
import lemongrenade.core.storm.LGRabbitMQSpout;
import lemongrenade.core.util.ExceptionWriter;
import lemongrenade.core.util.LGConstants;
import lemongrenade.core.util.LGProperties;
import lemongrenade.core.util.StringScheme;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import java.util.Iterator;
import java.util.Map;

public abstract class LGJavaAdapter extends LGAdapter {
    private static final Logger log = LoggerFactory.getLogger(LGJavaAdapter.class);
    private transient AdapterManager am;

    public LGJavaAdapter(String id) {
        super(id);
        am = new AdapterManager();
    }

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

        builder.setSpout("input", new LGRabbitMQSpout(new StringScheme(), getAdapterId()
                                , getAdapterName(), getQueueName())
                , LGProperties.getInteger("rabbit.spout.threads", 1))
                .addConfigurations(spoutConfig.asMap())
                .setMaxSpoutPending(LGProperties.getInteger("rabbit.prefetch.messages", 250));

        /**
         * Heartbeat bolt
         */
        builder.setBolt("heartbeat", new BaseBasicBolt() {
            private transient Jedis jedis;

            @Override
            public void prepare(final Map config, final TopologyContext context) {
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


        builder.setBolt(getAdapterName(), new BaseRichBolt() {
            private JobManager jobManager;
            private AdapterManager adapterManager;
            private OutputCollector oc;

            public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
                jobManager = new JobManager();
                adapterManager = new AdapterManager();
                oc = outputCollector;
            }

            public void execute(Tuple tuple) {
                LGPayload payload = (LGPayload) tuple.getValueByField(LGConstants.LG_PAYLOAD);
                LGJob lg;
                try {
                    lg = jobManager.getJob(payload.getJobId());
                } catch (org.mongodb.morphia.mapping.MappingException e) {
                    log.error(" Unable to map jobId " + payload.getJobId() + "   error:" + e.getMessage());
                    oc.reportError(e);
                    oc.fail(tuple);
                    return;
                }
                if (lg == null) {
                    log.error("Adapter received invalid job. " + payload.getJobId());
                    oc.ack(tuple);
                    return;
                }
                if (lg.getStatus() == LGJob.STATUS_STOPPED) {
                    log.info("Adapter received task for job, but job is STOPPED. Ignoring");
                    oc.ack(tuple);
                    return;
                }

                // Check TTL (If Job has expired, stop processing
                if (jobManager.hasJobExpired(lg)) {
                    oc.ack(tuple);
                    return;
                }

                adapterManager.setCurrentTaskId(getAdapterId(), payload.getTaskId());

                try {
                    process(payload, new LGCallback() {
                        @Override
                        public void emit(LGPayload resp) {
                            String job_id = resp.getJobId();
                            String task_id = resp.getTaskId();
                            log.info("***EMITTING [Adapter" + getAdapterName() + "] job_id:" + job_id + " task_id:" + task_id);
                            Values values = new Values(resp.getJobId(), resp);
                            oc.emit(tuple, values);
                            oc.ack(tuple);
                        }

                        @Override
                        public void fail(Exception ex) {
                            String job_id = payload.getJobId();
                            String task_id = payload.getTaskId();
                            log.info("***FAILING [Adapter" + getAdapterName() + "]. job_id:"+job_id +" task_id:"+task_id);
                            ex.printStackTrace();//error handling here...
                            try {
                                ExceptionWriter writer = new ExceptionWriter();
                                String requests = payload.getRequestNodes().toString();
                                writer.publishException(task_id, requests, ex);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            oc.reportError(ex);
                            oc.fail(tuple);
                        }
                    });
                }
                catch (Exception e) {
                    // It's critical that we have the try/catch wrapper around the process block, because if the
                    // AdapterWriter does something that causes a problem, (nullPointer, JSONObject.get("badkey") we
                    // can not expect them to do the correct error handling. So, we gracefully catch it here and
                    // fail the tuple
                    log.error("Adapter threw an exception: Failing Tuple: "+e.getMessage());
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
                    oc.fail(tuple);
                }
            }

            public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
                outputFieldsDeclarer.declare(new Fields(LGConstants.LG_JOB_ID, LGConstants.LG_PAYLOAD));
            }
        }, getParallelismHint()).shuffleGrouping("input");

        builder.setBolt("output", new RabbitMQBolt(new CoordinatorSinkScheme()), LGProperties.getInteger("rabbit.sink.threads", 1))
                .addConfigurations(sinkConfig.asMap())
                .shuffleGrouping(getAdapterName());

        return builder.createTopology();
    }

    public interface LGCallback {
        void emit(LGPayload resp);
        void fail(Exception ex);
    }

    public JSONObject getJobConfig(LGPayload payload) {
        JSONObject generic_job_config = payload.getJobConfig();
        JSONObject new_job_config = new JSONObject(payload.getJobConfig().toString());//create a deep copy of job_config
        if(generic_job_config.has("adapters")) {
            JSONObject adapters = new JSONObject(generic_job_config.get("adapters").toString());
            new_job_config.remove("adapters");
            if(adapters.has(this.getAdapterName())) {//adapters section has entries for this adapter
                JSONObject unique = adapters.getJSONObject(this.getAdapterName());
                Iterator<?> keys = unique.keys();
                while(keys.hasNext()) {
                    String key = keys.next().toString();
                    new_job_config.put(key, unique.get(key));
                }
            }
        }
        return new_job_config;
    }

    public abstract void process(LGPayload input, LGCallback callback);
}

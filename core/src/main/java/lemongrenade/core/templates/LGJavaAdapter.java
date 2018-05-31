package lemongrenade.core.templates;

import com.rabbitmq.client.ConnectionFactory;
import io.latent.storm.rabbitmq.RabbitMQBolt;
import io.latent.storm.rabbitmq.config.*;
import lemongrenade.core.coordinator.AdapterManager;
import lemongrenade.core.coordinator.JobManager;
import lemongrenade.core.models.LGJob;
import lemongrenade.core.models.LGJobError;
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
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Iterator;
import java.util.Map;

public abstract class LGJavaAdapter extends LGAdapter {
    private static final Logger log = LoggerFactory.getLogger(LGJavaAdapter.class);
    private StormTopology TOPOLOGY = null;
    private BaseRichBolt ADAPTER_BOLT = null;
    private BaseBasicBolt HEARTBEAT_BOLT = null;
    private RabbitMQBolt RABBIT_BOLT = null;

    public LGJavaAdapter(String id) {super(id);}

    public StormTopology getTopology() {
        if(TOPOLOGY != null) {
            return TOPOLOGY;
        }

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
        int maxNodesPerTask = 0;
        if (getConfig().containsKey("max_nodes_per_task")) {
            String str = getConfig().get("max_nodes_per_task").toString();
            try  {
                if ((str != null) && (!str.equals(""))) {
                    maxNodesPerTask = Integer.parseInt(str);
                }
            }
            catch (NumberFormatException e)  {
                maxNodesPerTask = 0;
            }
        }

        AdapterManager.buildAdapterDataStructure(getAdapterId(), getAdapterName(),
                getAdapterQuery(), getAdapterDepth(), getRequiredAttributes(), maxNodesPerTask, this.getAuxInfo());

        builder.setSpout("input", new LGRabbitMQSpout(new StringScheme(), getAdapterId()
                                , getAdapterName(), getQueueName())
                , LGProperties.getInteger("rabbit.spout.threads", 1))
                .addConfigurations(spoutConfig.asMap())
                .setMaxSpoutPending(LGProperties.getInteger("rabbit.prefetch.messages", 250));

        /**
         * Heartbeat bolt
         */
        if(HEARTBEAT_BOLT == null) {
            HEARTBEAT_BOLT = createHeartbeatBolt();
        }
        builder.setBolt("heartbeat", HEARTBEAT_BOLT);


        if(ADAPTER_BOLT == null) {
            ADAPTER_BOLT = createAdapterBolt();
        }

        int executors = getParallelismHint();
        int tasks = getTaskCount();
        int workers = (int) getConfig().getOrDefault("topology.workers", 1);
        log.info("Setting bolt for "+getAdapterName()+". Workers:"+workers+" Executors:"+executors+" Tasks:"+tasks);
        builder.setBolt(getAdapterName(), ADAPTER_BOLT, executors)//set the amount of threads for this adapter
            .shuffleGrouping("input")
            .setNumTasks(tasks)//sets the total number of tasks
        ;

        if(RABBIT_BOLT == null) {
            RABBIT_BOLT = createRabbitBolt();
        }
        builder.setBolt("output", RABBIT_BOLT, LGProperties.getInteger("rabbit.sink.threads", 1))
                .addConfigurations(sinkConfig.asMap())
                .shuffleGrouping(getAdapterName());

        TOPOLOGY = builder.createTopology();
        return TOPOLOGY;
    } //end getTopology

    public RabbitMQBolt createRabbitBolt() {
        RabbitMQBolt bolt = new RabbitMQBolt(new CoordinatorSinkScheme());
        return bolt;
    }

    public BaseBasicBolt createHeartbeatBolt() {
        BaseBasicBolt bolt = new BaseBasicBolt() {

            public void cleanup() {
                super.cleanup();
                AdapterManager.close();
            }

            @Override public void prepare(final Map config, final TopologyContext context) {}

            @Override public Map<String, Object> getComponentConfiguration() {
                Config conf = new Config();
                int heartBeatTime = LGProperties.getInteger("adapter_heartbeat", 20);
                log.info("Heartbeat tick time = " + heartBeatTime);
                conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, heartBeatTime);
                return conf;
            }

            protected boolean isTickTuple(Tuple tuple) {
                return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                        && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
            }

            @Override public void execute(Tuple tuple, BasicOutputCollector collector) {
                try {
                    if (isTickTuple(tuple)) {
                        log.info("Received Heartbeat " + getAdapterName() + " " + getAdapterId());
                        AdapterManager.setHeartBeat(getAdapterId(), System.currentTimeMillis());

                    }
                } catch (Exception e) {
                    log.error("Bolt execute error: " + e.getMessage());
                }
            }

            // This is a dummy bolt - so no outputfields to declare
            public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            }
        };
        return bolt;
    }

    public BaseRichBolt createAdapterBolt() {
        BaseRichBolt bolt = new BaseRichBolt() {
            private OutputCollector oc;
            private transient JobManager JOB_MANAGER;
            private transient AdapterManager AM;

            public void cleanup() {
                super.cleanup();
                if(JOB_MANAGER != null) {
                    JOB_MANAGER.close();
                }
                if(AM != null) {
                    AM.close();
                }
            }

            public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
                oc = outputCollector;
                JOB_MANAGER = new JobManager();
                AM = new AdapterManager();
            }

            public void execute(Tuple tuple) {
                LGPayload payload = (LGPayload) tuple.getValueByField(LGConstants.LG_PAYLOAD);
                LGJob job;
                String jobId = payload.getJobId();

                try {
                    job = JOB_MANAGER.getJob(jobId);
                } catch (org.mongodb.morphia.mapping.MappingException e) {
                    log.error(" Unable to map jobId " + payload.getJobId() + "   error:" + e.getMessage());
                    oc.reportError(e);
                    oc.fail(tuple);
                    return;
                }
                if (job == null) { //We probably received a request for a deleted job. We will ack this request, but do nothing.
                    log.error("Adapter received task:"+payload.getTaskId()+" for job:"+payload.getJobId()+", but job is invalid.");
                    JOB_MANAGER.updateTaskToDropped(job, payload.getTaskId());
                    oc.ack(tuple);
                    return;
                }
                if (job.getStatus() == LGJob.STATUS_STOPPED) {
                    log.info("Adapter received task:"+payload.getTaskId()+" for job:"+payload.getJobId()+", but job is STOPPED. Dropping task.");
                    JOB_MANAGER.updateTaskToDropped(job, payload.getTaskId());
                    oc.ack(tuple);
                    return;
                }

                // Check TTL (If Job has expired, stop processing
                if (JOB_MANAGER.hasJobExpired(job)) {
                    log.info("Adapter received task:"+payload.getTaskId()+" for job:"+payload.getJobId()+", but job has expired.");
                    JOB_MANAGER.updateTaskToDropped(job, payload.getTaskId());
                    oc.ack(tuple);
                    return;
                }

                AM.setCurrentTaskId(getAdapterId(), payload.getTaskId());

                try {
                    process(payload, new LGCallback() {
                        @Override
                        public void emit(LGPayload resp) {
                            //String job_id = resp.getJobId();
                            //String task_id = resp.getTaskId();
                            //log.info("***EMITTING [Adapter" + getAdapterName() + "] job_id:" + job_id + " task_id:" + task_id);
                            Values values = new Values(resp.getJobId(), resp);
                            oc.emit(tuple, values);
                            oc.ack(tuple);
                        }

                        @Override
                        public void fail(Exception ex) {
                            String job_id = payload.getJobId();
                            String task_id = payload.getTaskId();
                            String errorMsg = getAdapterName() + ": " + ex.getMessage();
                            log.info("***FAILING [Adapter" + getAdapterName() + "]. job_id:" + job_id + " task_id:" + task_id
                                    +" for error:"+ex.getMessage());
                            ex.printStackTrace();//error handling here...
                            try {
                                ExceptionWriter writer = new ExceptionWriter();
                                String requests = payload.getRequestNodes().toString();
                                writer.publishException(task_id, requests, ex);
                                writer.channel.close();
                                writer.connection.close();
                            } catch (Exception e) {
                                log.error("Failure while writing adapter failure exception. job_id:"+job_id +"task_id:"+task_id);
                                e.printStackTrace();
                            }
                            LGJobError error = new LGJobError(job_id, task_id, getAdapterName(), getAdapterId(), errorMsg);
                            log.info("Adding error for job_id:"+job_id+" task_id:"+task_id+" error:"+errorMsg);
                            try {
                                JOB_MANAGER.updateErrorsForTask(job, task_id, error);
                            }
                            catch(Exception e) {//This can happen if the job was deleted
                                log.error("Unable to add error for job_id:"+job_id+" task_id:"+task_id+".");
                            }
                            oc.reportError(ex);
                            oc.fail(tuple);
                        }
                    });
                }
                catch (Exception e) {
                    log.error("Adapter threw an exception: Failing Tuple: "+e.getMessage());
                    e.printStackTrace();
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
        };//end of bolt declaration
        return bolt;
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

    //Cleans up and closes bolts and topology
    public void close() {
        if(ADAPTER_BOLT != null) {
            ADAPTER_BOLT.cleanup();
            ADAPTER_BOLT = null;
        }
        if(HEARTBEAT_BOLT != null) {
            HEARTBEAT_BOLT.cleanup();
            HEARTBEAT_BOLT = null;
        }
        if(RABBIT_BOLT != null) {
            RABBIT_BOLT.cleanup();
            RABBIT_BOLT = null;
        }
        TOPOLOGY = null;
    }

    public abstract void process(LGPayload input, LGCallback callback);
}

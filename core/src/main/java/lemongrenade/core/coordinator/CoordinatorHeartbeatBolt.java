package lemongrenade.core.coordinator;

import lemongrenade.core.SubmitToRabbitMQ;
import lemongrenade.core.database.lemongraph.LemonGraph;
import lemongrenade.core.database.mongo.MongoDBStore;
import lemongrenade.core.models.LGAdapterModel;
import lemongrenade.core.models.LGJob;
import lemongrenade.core.util.LGProperties;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Coordinator Heartbeat Bolt performs extra actions compared to adapter level version.
 *
 * The coordinator takes advantage of this interval to run a few cleanup tasks.
 * First, it looks at all jobs that are currently PROCESSING and/or ERROR state. If
 * there are no current active tasks for the job and the last task execute was over
 * MAX_JOB_IDLE_SECONDS, we consider this job to be FINISHED or FINISHED_WITH_ERRORS
 *
 * Secondly, it looks at all reporting adapters. If any adapter is less than MAX_ADAPTER_HB_TIME,
 * we set those adapter to OFFLINE status.
 *
 */
public class CoordinatorHeartbeatBolt extends BaseBasicBolt {
    private final Logger log = LoggerFactory.getLogger(getClass());
    final static int MAX_ADAPTER_HEARTBEAT_TIME = LGProperties.getInteger("max_adapter_heartbeat_time", 300);
    transient private JobManager JOB_MANAGER;
    transient private AdapterManager ADAPTER_MANAGER;
    transient private SubmitToRabbitMQ SUBMIT_TO_RABBIT;
    transient private LemonGraph LEMONGRAPH;
    transient private Long LAST_RESET;
    private boolean checkRunning;

    public void close() {
        try {
            JOB_MANAGER.close();
            ADAPTER_MANAGER.close();
            SUBMIT_TO_RABBIT.close();
            LEMONGRAPH.close();
        }
        catch(Exception e) {
            log.error("Failed to close connections.");
            e.printStackTrace();
        }
    }

    //Override LAST_RESET for testing
    public void setLastReset(Long reset) {
        LAST_RESET = reset;
    }

    @Override public void prepare(final Map config, final TopologyContext context) {
        checkRunning = false;
        JOB_MANAGER = new JobManager();
        ADAPTER_MANAGER = new AdapterManager();
        SUBMIT_TO_RABBIT =  new SubmitToRabbitMQ();
        LEMONGRAPH = new LemonGraph();
        log.info("Performing startup reset/expire for jobs.");
        resetJobs();//reset required jobs on startup
        expireJobs();//delete required jobs on startup
        LAST_RESET = System.currentTimeMillis();
    }

    //Reset all jobs past reset date
    public void resetJobs() {
        Set<String> ids = MongoDBStore.getResetJobs();
        if(ids.size() == 0) {
            log.info("No jobs ready for reset.");
            return;
        }
        log.info("Resetting LEMONGRENADE jobs. IDs:"+ids.toString());
        Iterator iterator = ids.iterator();
        String reason = "RESET date reached.";
        while(iterator.hasNext()) {
            try {
                String jobId = iterator.next().toString();
                SUBMIT_TO_RABBIT.sendReset(jobId, reason);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    //Delete all expired jobs
    public void expireJobs() {
        Set<String> ids = MongoDBStore.getExpiredJobs();
        if(ids.size() == 0) {
            log.info("No jobs ready for expiration.");
            return;
        }
        log.info("Deleting LEMONGRENADE jobs. IDs:" + ids.toString());
        Iterator iterator = ids.iterator();
        while(iterator.hasNext()) {
            String jobId = iterator.next().toString();
            try {
                LEMONGRAPH.deleteGraph(jobId);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
            try {
                JOB_MANAGER.deleteDBValues(jobId);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
            try {
                MongoDBStore.deleteJob(jobId);
                MongoDBStore.deleteTasksByJob(jobId);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        int heartBeatTime = LGProperties.getInteger("coordinator_heartbeat", 30);
        log.info("Coordinator Heartbeat tick time = " + heartBeatTime);
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
                log.info("Received Heartbeat Coordinator");
                checkForResetExpire();
                if (!checkRunning) {
                    checkAdapterHearbeats();
                    checkForFinishedJobs();
                }
            }
        } catch (Exception e) {
            log.error("Bolt execute error: " + e.getMessage());
        }
    }

    /** */
    private void checkAdapterHearbeats() {
        List<LGAdapterModel> adapters = ADAPTER_MANAGER.getAll();
        for(LGAdapterModel a: adapters) {
            long diffTime = (System.currentTimeMillis() - a.getLastHeartBeat())/ LGJob.SECOND;
            if (diffTime > MAX_ADAPTER_HEARTBEAT_TIME) {
                log.warn("Adapter "+a.getUniqueName()+" heartbeat exceeded max time "
                        +diffTime+"/"+MAX_ADAPTER_HEARTBEAT_TIME);
                ADAPTER_MANAGER.setStatus(a.getId(), LGAdapterModel.STATUS_OFFLINE);
                continue;
            } else {
                // Check DEGRADED? has errors certain percentage in last X minutes
                if (a.getStatus() != LGAdapterModel.STATUS_ONLINE) {
                    ADAPTER_MANAGER.setStatus(a.getId(), LGAdapterModel.STATUS_ONLINE);
                }
            }
        }
    }

    public boolean checkForResetExpire() {
        long msSinceLastReset = (System.currentTimeMillis()-LAST_RESET);
        if(msSinceLastReset >= LGJob.DAY) {
            LAST_RESET = System.currentTimeMillis();
            log.info("Over 24 hours since last reset/expire.");
            resetJobs();
            expireJobs();
            return true;//returns 'true' if jobs were reset/expired
        }
        return false;//returns 'false' if no reset/expire occured
    }

    /** */
    private void checkForFinishedJobs() {
        checkRunning = true;
        int maxJobIdleTime       = LGProperties.getInteger("max_job_idle_seconds",30);//Only use if max_job_idle_seconds > 0
        int maxJobRunTimeSeconds = LGProperties.getInteger("max_job_run_time_seconds",0);//Only use max_job_run_time if > 0

        // Get list of all jobs PROCESSING jobs
        List<LGJob> jobs = JOB_MANAGER.getAllProcessing();
        for(LGJob job : jobs) {
            long secondsSinceLastTask = (System.currentTimeMillis() - job.getLastTaskTime())/LGJob.SECOND;
            long currentRunningTimeSeconds = job.calculateCurrentRunningTimeSeconds();
            if ((maxJobIdleTime > 0 && job.getActiveTaskCount() == 0) && (secondsSinceLastTask > maxJobIdleTime)) {
                log.info("Setting job:"+job.getJobId()+" with no active tasks to FINISHED.");
                JOB_MANAGER.updateJobIfFinished(job);
            }
            else if (maxJobRunTimeSeconds > 0 && currentRunningTimeSeconds > maxJobRunTimeSeconds) {
                log.warn("Job has exceed system maximum run time limit! Setting status to FINISHED.");
                JOB_MANAGER.setJobFinished(job);
            }
        }
        checkRunning = false;
    }

    // This is a dummy bolt - so no outputfields to declare
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}
}

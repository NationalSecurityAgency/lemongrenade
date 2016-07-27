package lemongrenade.core.coordinator;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lemongrenade.core.database.lemongraph.InvalidGraphException;
import lemongrenade.core.database.lemongraph.LemonGraph;
import lemongrenade.core.models.LGCommand;
import lemongrenade.core.models.LGJob;
import lemongrenade.core.models.LGJobHistory;
import lemongrenade.core.models.LGPayload;
import lemongrenade.core.util.LGConstants;
import lemongrenade.core.util.LGProperties;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Commands supported or will be supported: NEW,ADD,STOP, CHANGE_PRIORITY
 */
public class CoordinatorCommandBolt extends BaseRichBolt {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private OutputCollector oc;
    private int boltId;
    Channel channel;
    private JobManager jobManager;
    private LemonGraph lemonGraph;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        oc = outputCollector;
        boltId = topologyContext.getThisTaskId();
        jobManager = new JobManager();
        lemonGraph = new LemonGraph();

        // Setup communications with coordinatorSpout via coordinator rabbitmq
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(LGProperties.get("rabbit.hostname"));
        try {
            Connection connection = factory.newConnection();
            channel = connection.createChannel();
            channel.queueDeclare(LGConstants.LEMONGRENADE_COORDINATOR, true, false, false, CoordinatorTopology.queueArgs);
        }
        catch (Exception e) {
            log.error("Caught exception trying to setup rabbitmq communication");
        }
    }

    /**
     * Helper Method for execute()
     * @return boolean if we should ack the tuple or not
     */
    private boolean handleNewCommand(LGCommand incomingCmd, Tuple tuple) {
        String jobId = incomingCmd.getJobId();
        boolean jobAlreadyExists = false;
        if (jobManager.doesJobExist(jobId)) {
            // Does job already exist, if so - error out IF STATUS is NOT NEW
            // Status == NEW means it's been added by the SubmitJob class and is awaiting processing
            if (jobManager.getJob(jobId).getStatus() != LGJob.STATUS_NEW) {
                log.warn("Received NEW job id " + jobId + " but it already exists in database and is NOT in NEW status.");
                // We return true because we want to ACK the tuple
                return true;
            }
            jobAlreadyExists = true;
        }

        // New Jobs are always *seeds*
        LGPayload lgp;
        LGJob     job;
        lgp = incomingCmd.getSeedPayload();
        lgp.setPayloadType(LGConstants.LG_PAYLOAD_TYPE_COMMAND);
        if (jobAlreadyExists) {
            job = jobManager.getJob(jobId);
        } else {
            job = new LGJob(jobId, incomingCmd.getAdapterList(), lgp.getJobConfig());
            jobManager.addJob(job);
        }

        // If we were given no approved adapters for this job, we ignore it
        if (incomingCmd.getAdapterList().size() <=0 ) {
            log.error("Incoming job has no approved adapter list");
            LGJobHistory history = new LGJobHistory(LGJobHistory.LGHISTORY_TYPE_COMMAND, "error", "",
                    "No valid adapters for job",System.currentTimeMillis(), System.currentTimeMillis(), 0,0,0,0);
            jobManager.updateHistoryForJob(job.getJobId(),history);
            jobManager.setStatus(job,LGJob.STATUS_ERROR);
            return true;
        }

        // Send job to the coordinator for processing
        try {
            channel.basicPublish("", LGConstants.LEMONGRENADE_COORDINATOR
                    , new AMQP.BasicProperties.Builder().priority(LGConstants.QUEUE_PRIORITY_NEW_JOB).build()
                    , lgp.toByteArray());
            //log.info("Sending incoming payload to " + LGConstants.LEMONGRENADE_COORDINATOR.toString());
        } catch (IOException e) {
            log.error("Unable to publish job to coordinator queue !" + e.getMessage());
            // Return false will cause a retry because we will oc.fail() the tuple
            return false;
        }
        return true;
    }

    /**
     * Helper Method for execute()
     * @return true/false depending on if we want to ack or fail the tuple
     */
    private boolean handleExecuteOnNodesCommand(LGCommand incomingCmd, Tuple tuple) {
        String jobId = incomingCmd.getJobId();
        // If we were given no approved adapters for this job, we ignore it
        if (!jobManager.doesJobExist(jobId)) {
            log.warn("Received ExecuteOnNodes job id "+jobId+" but does not exist in database.");
            // We return true because we want to ACK the tuple
            return true;
        }
        // New Jobs are always *seeds*
        LGPayload lgp = incomingCmd.getSeedPayload();
        LGJob job = jobManager.getJob(jobId);
        jobManager.setStatus(job,LGJob.STATUS_PROCESSING);

        // Store incoming command in history - endtime is unknown
        LGJobHistory lgHistory = new LGJobHistory(LGJobHistory.LGHISTORY_TYPE_COMMAND,  "ExecuteOnNodes", "",
                "ExecuteOnNodes command received",System.currentTimeMillis(), 0, 0, 0, 0,0);
        jobManager.updateHistoryForJob(jobId, lgHistory);

        try {
            channel.basicPublish("", LGConstants.LEMONGRENADE_COORDINATOR
                    , new AMQP.BasicProperties.Builder().priority(LGConstants.QUEUE_PRIORITY_NEW_JOB).build()
                    , lgp.toByteArray());
            log.info("Sending incoming EXECUTE ON ADAPTERS payload to " + LGConstants.LEMONGRENADE_COORDINATOR);
        } catch (IOException e) {
            log.error("Unable to publish job to coordinator queue !" + e.getMessage());
            // Return false will cause a retry because we will oc.fail() the tuple
            return false;
        }
        return true;
    }

    /**
     * Helper Method for execute()
     * @return true/false depending on if we want to ack or fail the tuple
     */
    private boolean handleReset(LGCommand incomingCmd, Tuple tuple) {
        String jobId = incomingCmd.getJobId();
        if (!jobManager.doesJobExist(jobId)) {
            log.warn("Received ExecuteOnNodes job id "+jobId+" but does not exist in database.");
            // We return true because we want to ACK the tuple
            return true;
        }

        LGJob job = jobManager.getJob(jobId);
        LGPayload lgp = incomingCmd.getSeedPayload();
        JSONObject jconfig = lgp.getJobConfig();
        String resetReason = "";
        if (jconfig.has(LGConstants.LG_RESET_REASON)) {
            resetReason = jconfig.getString(LGConstants.LG_RESET_REASON);
        }

        // Check status to make sure it's 'resetable' (we allow reset to run again on already reset jobs)
        if (  (job.getStatus() == LGJob.STATUS_FINISHED)
                ||(job.getStatus() == LGJob.STATUS_FINISHED_WITH_ERRORS)
                ||(job.getStatus() == LGJob.STATUS_STOPPED)
                ||(job.getStatus() == LGJob.STATUS_RESET))
        {
            log.info("Job ["+jobId+"] accepted for reset.");
        } else {
            log.error("Job ["+jobId+"] can't be reset with state :"+job.getStatusString(job.getStatus()));
            return false;
        }

        LGJobHistory lgHistory = new LGJobHistory(LGJobHistory.LGHISTORY_TYPE_COMMAND, "Reset", "",
                "Reset command received reason:"+resetReason,System.currentTimeMillis(), 0, 0, 0, 0, 0);
        jobManager.updateHistoryForJob(jobId, lgHistory);

        // Delete Graph from LemonGraph
        try {
            lemonGraph.deleteGraph(jobId);
        }
        catch (InvalidGraphException e) {
            log.error("Unable to delete graph from LEMONGraph for job id ["+jobId+"] Reset failed.");
            return false;
        }

        // We only set status to RESET if the delete from lemongraph was successful
        if (!jobManager.setStatus(job, LGJob.STATUS_RESET, resetReason)) {
            log.error("Unable to set state to RESET for job ["+jobId+"]");
        }
        return true;
    }

    /**
     *
     * @return true/false if tuple should be ack'd or not
     */
    private boolean handleAddCommand(LGCommand incomingCmd, Tuple tuple) {
        String jobId = incomingCmd.getJobId();

        // If we were given no approved adapters for this job, we ignore it
        if (incomingCmd.getAdapterList().size() <=0 ) {
            log.error("Incoming job has no approved adapter list, ignoring job request.");
            // We return true because we want to ACK the tuple
            return true;
        }
        else if (!jobManager.doesJobExist(jobId)) {
            // Does job already exist, if so - error out
            log.warn("Received ADD job id "+jobId+" but job does NOT Exist");
            // We return true because we want to ACK the tuple
            return true;
        }
        // New Job would already be labeled seed or not by the submitting user
        LGPayload lgp = incomingCmd.getSeedPayload();
        LGJob job = new LGJob(jobId, incomingCmd.getAdapterList(), lgp.getJobConfig());
        LGJobHistory lgHistory = new LGJobHistory(LGJobHistory.LGHISTORY_TYPE_COMMAND, "Add", "",
                "Add command received",System.currentTimeMillis(), 0, 0, 0 , 0, 0);
        jobManager.updateHistoryForJob(jobId, lgHistory);

        jobManager.setStatus(job, LGJob.STATUS_PROCESSING);
        try {
            channel.basicPublish("", LGConstants.LEMONGRENADE_COORDINATOR
                    , new AMQP.BasicProperties.Builder().priority(LGConstants.QUEUE_PRIORITY_NEW_JOB).build()
                    , lgp.toByteArray());
            log.info("Sending add to job "+jobId+" payload to " + LGConstants.LEMONGRENADE_COORDINATOR);
        } catch (IOException e) {
            log.error("Unable to publish job to coordinator queue !" + e.getMessage());
            // Return false will cause a retry because we will oc.fail() the tuple
            return false;
        }
        return true;
    }

    /**
     * Helper Method for execute()
     */
    private void handleStopCommand(LGCommand incomingCmd, Tuple tuple) {
        String jobId = incomingCmd.getJobId();
        if (!jobManager.doesJobExist(jobId)) {
            // Does job already exist, if so - error out
            log.error("Received STOP jobid "+jobId+" but it does not exist.");
        } else {
            // Update Database to set state to STOPPED, adapters and coordinators will handle everything accordingly.
            LGJob lj = jobManager.getJob(jobId);
            if (lj != null) {
                jobManager.setStatus(lj,LGJob.STATUS_STOPPED);
                log.info("Setting job id ["+jobId+"] to STOPPED");
            }
            LGJobHistory lgHistory = new LGJobHistory(LGJobHistory.LGHISTORY_TYPE_COMMAND,  "Stop", "",
                    "Stop command received", System.currentTimeMillis(), 0, 0, 0, 0, 0);
            jobManager.updateHistoryForJob(jobId, lgHistory);
        }
    }

    public void execute(Tuple tuple) {
        LGCommand incomingCmd = (LGCommand) tuple.getValueByField(LGConstants.LG_COMMAND);
        log.info("EXECUTE Job Command:     "+incomingCmd.toString());

        boolean val = false;
        switch(incomingCmd.getCmd()) {
            case LGCommand.COMMAND_TYPE_NEW:
                val = handleNewCommand(incomingCmd, tuple);
                if (!val) {
                    oc.fail(tuple);
                }
                break;
            case LGCommand.COMMAND_TYPE_STOP:
                handleStopCommand(incomingCmd, tuple);
                break;
            case LGCommand.COMMAND_TYPE_ADD:
                val = handleAddCommand(incomingCmd, tuple);
                if (!val) {
                    oc.fail(tuple);
                }
                break;
            case LGCommand.COMMAND_TYPE_EXECUTE_ON_NODES:
                val = handleExecuteOnNodesCommand(incomingCmd, tuple);
                if (!val) {
                    oc.fail(tuple);
                }
                break;
            case LGCommand.COMMAND_TYPE_RESET:
                val = handleReset(incomingCmd, tuple);
                if (!val) {
                    oc.fail(tuple);
                }
                break;
            default:
                log.error("Invalid Job Command received from queue "+incomingCmd.getCmdString());
        }

        // Always ack the tuple
        oc.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(LGConstants.LG_JOB_ID, LGConstants.LG_COMMAND, "destination"));
    }

}

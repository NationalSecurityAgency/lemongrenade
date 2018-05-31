package lemongrenade.core.coordinator;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lemongrenade.core.database.lemongraph.InvalidGraphException;
import lemongrenade.core.database.lemongraph.LemonGraph;
import lemongrenade.core.models.*;
import lemongrenade.core.util.LGConstants;
import lemongrenade.core.util.LGProperties;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Commands supported or will be supported: NEW,ADD,STOP,RESET,CHANGE_PRIORITY
 */
public class CoordinatorCommandBolt extends CoordinatorBolt {

    CoordinatorCommandBolt() {
        super();
        init();
    }

    //Fields that won't be serialized need to be marked transient
    transient Channel channel = null;
    transient Connection connection = null;
    transient ConnectionFactory factory = null;

    public void init() {
        if(factory == null) {
            factory = new ConnectionFactory();
            factory.setHost(LGProperties.get("rabbit.hostname"));
            openConnection();
        }
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        oc = outputCollector;
        boltId = topologyContext.getThisTaskId();
        // Setup communications with coordinatorSpout via coordinator rabbitmq
        init();
    }

    //Attempts to open a RabbitMQ connection to COORDINATOR channel, if one isn't already open
    public void openConnection() {
        if(connection == null || !connection.isOpen() || channel == null || !channel.isOpen() ) {
            try {
                log.info("Opening RabbitMQ connection and COORDINATOR channel.");
                connection = factory.newConnection();
                channel = connection.createChannel();
                channel.queueDeclare(LGConstants.LEMONGRENADE_COORDINATOR, true, false, false, CoordinatorTopology.queueArgs);
            } catch (java.io.IOException e) {
                log.error("Unable to connect to rabbitmq " + LGConstants.LEMONGRENADE_COORDINATOR + " " + e.getMessage());
            } catch (TimeoutException e) {
                log.error("Timeout connecting to rabbitmq " + LGConstants.LEMONGRENADE_COORDINATOR + " " + e.getMessage());
            }
        } else {
            log.debug("Connection and Channel are open.");
        }
    }

    //closes connections possibly left open
    public void cleanup() {
        super.cleanup();
        if(channel != null && channel.isOpen()) {
            try {
                channel.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            } catch(com.rabbitmq.client.AlreadyClosedException e) {
                log.info(LGConstants.LEMONGRENADE_COORDINATOR+" channel already closed.");
            }
        }
        if(connection != null && connection.isOpen()) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch(com.rabbitmq.client.AlreadyClosedException e) {
                log.info(LGConstants.LEMONGRENADE_COORDINATOR+" connection already closed.");
            }
        }
        log.info("Closing connection to LemonGraph.");
        LemonGraph.close();
    }

    /** For JUNIT Testing purposes only!*/
    public void testSetup() {
        boltId = 1;
        init();
    }

    /**
     * Helper Method for execute()
     * @return boolean if we should ack the tuple or not
     */
    private boolean handleNewCommand(LGCommand incomingCmd, Tuple tuple) {
        String jobId = incomingCmd.getJobId();
        boolean jobAlreadyExists = false;
        if (JobManager.doesJobExist(jobId)) {
            // Does job already exist, if so - error out IF STATUS is NOT NEW
            // Status == NEW means it's been added by the SubmitToRabbitMQ class and is awaiting processing
            if (JobManager.getJob(jobId).getStatus() != LGJob.STATUS_NEW) {
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
            job = JobManager.getJob(jobId);
        } else {
            job = new LGJob(jobId, incomingCmd.getAdapterList(), lgp.getJobConfig());
            JobManager.addJob(job);
        }

        // If we were given no approved adapters for this job, we ignore it
        if (incomingCmd.getAdapterList().size() <=0 ) {
            LGJobHistory history = new LGJobHistory(LGJobHistory.LGHISTORY_TYPE_COMMAND, "error", "",
                    "No valid adapters for job",System.currentTimeMillis(), System.currentTimeMillis(), 0,0,0,0);
            JobManager.updateJobHistory(job, history);
            job.setEndTime(System.currentTimeMillis());
            LGJobError error = new LGJobError(jobId, "No Task", "Job Error", "No Adapter", "Incoming job has no approved adapter list.");
            JobManager.updateErrorsForJob(job, error);
            JobManager.setStatus(job,LGJob.STATUS_FINISHED_WITH_ERRORS);
            return true;
        }

        // Send job to the coordinator for processing
        try {
            openConnection();
            channel.basicPublish("", LGConstants.LEMONGRENADE_COORDINATOR
                    , new AMQP.BasicProperties.Builder().priority(LGConstants.QUEUE_PRIORITY_NEW_JOB).build()
                    , lgp.toByteArray());
            //log.info("Sending incoming payload to " + LGConstants.LEMONGRENADE_COORDINATOR.toString());
        } catch (IOException e) {
            log.error("Unable to publish job to coordinator queue !" + e.getMessage());
            return false;// Return false will cause a retry because we will oc.fail() the tuple
        }
        return true;
    }

    /**
     * Helper Method for execute()
     * @return true/false depending on if we want to ack or fail the tuple
     */
    private boolean handlePostActionCommand(LGCommand incomingCmd, Tuple tuple) {
        String jobId = incomingCmd.getJobId();

        // If we were given no approved adapters for this job, we ignore it
        if (incomingCmd.getAdapterList().size() <=0 ) {
            log.error("Incoming job has no approved adapter list, ignoring job request.");
            // We return true because we want to ACK the tuple
            return true;
        }

        if (!JobManager.doesJobExist(jobId)) {
            log.warn("Received ExecuteOnNodes job id "+jobId+" but does not exist in database.");
            // We return true because we want to ACK the tuple
            return true;
        }
        // New Jobs are always *seeds*
        LGPayload lgp = incomingCmd.getSeedPayload();
        LGJob job = JobManager.getJob(jobId);
        JobManager.setStatus(job, LGJob.STATUS_PROCESSING);

        // Store incoming command in history - endtime is unknown
        LGJobHistory lgHistory = new LGJobHistory(LGJobHistory.LGHISTORY_TYPE_COMMAND,  "PostAction", "",
                "PostAction command received ",System.currentTimeMillis(), 0, 0, 0, 0,0);
        JobManager.updateJobHistory(job, lgHistory);

        try {
            openConnection();
            channel.basicPublish("", LGConstants.LEMONGRENADE_COORDINATOR
                    , new AMQP.BasicProperties.Builder().priority(LGConstants.QUEUE_PRIORITY_NEW_JOB).build()
                    , lgp.toByteArray());
            log.info("Sending incoming Post Action payload to " + LGConstants.LEMONGRENADE_COORDINATOR);
        } catch (IOException e) {
            log.error("Unable to publish job to coordinator queue !" + e.getMessage());
            // Return false will cause a retry because we will oc.fail() the tuple
            return false;
        }
        return true;
    }

    /**
     * Helper Method for execute()
     * @param incomingCmd LGCommand
     * @param tuple Tuple, unused param
     * @return true/false depending on if we want to ack or fail the tuple
     */
    protected boolean handleReset(LGCommand incomingCmd, Tuple tuple) {
        String jobId = incomingCmd.getJobId();
        if (!JobManager.doesJobExist(jobId)) {
            log.warn("Received Reset job id "+jobId+" but does not exist in database.");
            // We return true because we want to ACK the tuple
            return true;
        }

        LGJob job = JobManager.getJob(jobId);
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
        JobManager.updateJobHistory(job, lgHistory);

        // Delete Graph from LemonGraph
        try {
            LemonGraph.resetGraph(jobId);
        }
        catch (InvalidGraphException e) {
            log.error("Unable to reset graph from LEMONGraph for job id ["+jobId+"] Reset failed.");
        }
        log.info("Lemongraph reset called");
        // We only set status to RESET if the delete from lemongraph was successful
        if (!JobManager.setStatus(job, LGJob.STATUS_RESET, resetReason, true)) {
            log.error("Unable to set state to RESET for job ["+jobId+"]");
        }
        return true;
    }

    /**
     * Helper Method for execute()
     * @param incomingCmd LGCommand
     * @param tuple Tuple, unused param.
     * @return true/false depending on if we want to ack or fail the tuple
     */
    protected boolean handleRetryFailedTasks(LGCommand incomingCmd, Tuple tuple) {
        String jobId = incomingCmd.getJobId();
        if (!JobManager.doesJobExist(jobId)) {
            log.warn("Received RETRY_FAILED_TASKS job id "+jobId+" but does not exist in database.");
            // We return true because we want to ACK the tuple
            return true;
        }

        LGJob job = JobManager.getJob(jobId);
        LGPayload lgp = incomingCmd.getSeedPayload();
        JSONObject jconfig = lgp.getJobConfig();

        // Only set doTaskList to true if we were given specific tasks to retry, otherwise, retry them all.
        boolean doTaskList = false;
        JSONArray approvedTasksJson = new JSONArray();
        List<String> approvedTasks = new ArrayList<String>();
        if (jconfig.has("tasks")) {

            approvedTasksJson = jconfig.getJSONArray("tasks");
            if (approvedTasksJson.length() > 0) {
                doTaskList = true;
                for (int i=0; i<approvedTasksJson.length(); i++) {
                    approvedTasks.add(approvedTasksJson.getString(i));
                }
            }
        }

        // Check status to make sure it's NOT PROCESSING
        if (job.getStatus() == LGJob.STATUS_PROCESSING) {
            log.error("Job ["+jobId+"] can't RETRY with state :"+job.getStatusString(job.getStatus()));
            return true;
        }

        // Log this command to the job's history, so we know what happened.
        LGJobHistory lgHistory = new LGJobHistory(LGJobHistory.LGHISTORY_TYPE_COMMAND, "RETRY", "",
                "Retrying failed tasks",System.currentTimeMillis(), 0, 0, 0, 0, 0);
        JobManager.updateJobHistory(job, lgHistory);

        // Count all Failed Tasks for job
        Map<String,LGTask> taskMap = job.getTaskMap();
        HashMap<String,LGTask> failedTaskList = new HashMap<String,LGTask>();
        if (taskMap == null) {
            log.error("Job ["+jobId+"] has no tasks to retry");
            return true;
        }
        int failed = 0;
        for(Map.Entry<String,LGTask> entry : taskMap.entrySet()) {
            LGTask t = entry.getValue();
            if (t.getStatus() == LGTask.TASK_STATUS_FAILED) {
                failed++;
                if (doTaskList) {
                    if (approvedTasks.contains(t.getTaskId())) {
                        failedTaskList.put(t.getTaskId(), t);
                    }
                } else {
                    failedTaskList.put(t.getTaskId(), t);
                }
            }
        }
        // if we didn't find any failed tasks , don't bother
        if (failed <= 0) {
            log.error("Job ["+jobId+"] has no FAILED tasks to retry");
            return true;
        }

        // Set to PROCESSING
        if (!JobManager.setStatus(job, LGJob.STATUS_PROCESSING)) {
            log.error("Unable to set state to PROCESSING for job ["+jobId+"]");
        }

        // RESUBMIT failed tasks back into the Coordinator Queue
        // This is tricky, because we have to ask lemongraph for the data at that tasks point in time and
        // that's the data we are going to resubmit.
        for(Map.Entry<String,LGTask> entry : failedTaskList.entrySet()) {
            LGTask oldTask = entry.getValue();
            log.info("Resubmitting FAILED task ["+oldTask.getTaskId()+"] to adapter "+oldTask.getAdapterName());

            JobManager.updateJobTaskStatus(job, oldTask.getTaskId(), LGTask.TASK_STATUS_REPLAYED);
            JobManager.removeErrorsForJobByTaskId(job, oldTask.getTaskId());
            job = JobManager.getJob(jobId);

            // Query LemonGraph and ask for the data at that point in time (data for original graph query)
            String adapterId = AdapterManager.findBestAdapterByAdapterName(oldTask.getAdapterName());
            HashMap<String, String> adapterQueryMap = new HashMap<String, String>();
            if (adapterId.equals("")) {
                log.warn("Unknown or disabled adapter requested ["+oldTask.getAdapterName()+"] inside jobid ["+jobId+"] ignoring request.");
            } else {
                String adapterGraphQuery = AdapterManager.getGraphQueryForAdapter(adapterId, job, true);
                if (!adapterGraphQuery.equals("")) {
                    adapterQueryMap.put(adapterId, adapterGraphQuery);
                }
            }

            int maxNodes = LGProperties.getInteger("max_nodes_per_task",0);

            // Send all Queries to LemonGraph
            JSONObject resultdata = LemonGraph.queryBasedOnPatterns(jobId, adapterQueryMap, oldTask.getCurrenGraphId(), oldTask.getMaxGraphId());
            // Parse LemonGraphResult and generated new tasks as needed
            HashMap<String, JSONArray> resultMap = LemonGraph.parseLemonGraphResult(resultdata);
            for (Map.Entry<String, JSONArray> entry2 : resultMap.entrySet()) {
                String    query = entry2.getKey();
                JSONArray nodes = entry2.getValue();
                // Helper will possibly split results into smaller tasks if a payload is too big
                int tasksGenerated = handleLemonGraphProcessingBatchHelper(job, "", adapterQueryMap, query, nodes, maxNodes, oldTask.getCurrenGraphId(), oldTask.getMaxGraphId(), oldTask.getNodePageIndex());

            }
           // metrics.numberOfNewTasksGenerated = numberOfNewTasksGenerated;

        }
        return true;
    }

    /**
     *
     * @return true/false if tuple should be ack'd or not
     */
    private boolean handleAddCommand(LGCommand incomingCmd) {
        String jobId = incomingCmd.getJobId();

        // If we were given no approved adapters for this job, we ignore it
        if (incomingCmd.getAdapterList().size() <= 0 ) {
            log.error("Incoming job has no approved adapter list, ignoring job request.");
            // We return true because we want to ACK the tuple
            return true;
        }
        else if (!JobManager.doesJobExist(jobId)) {
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
        JobManager.updateJobHistory(job, lgHistory);
        JobManager.setStatus(job, LGJob.STATUS_PROCESSING);
        try {
            openConnection();
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
     * @param incomingCmd LGCommand
     * @param tuple Tuple, unused param.
     */
    protected void handleStopCommand(LGCommand incomingCmd, Tuple tuple) {
        String jobId = incomingCmd.getJobId();
        if (!JobManager.doesJobExist(jobId)) {// Does job already exist, if so - error out
            log.error("Received STOP jobid "+jobId+" but it does not exist.");
        } else {// Update Database to set state to STOPPED, adapters and coordinators will handle everything accordingly.
            LGJob job = JobManager.getJob(jobId);
            if (job != null) {
                JobManager.setStatus(job,LGJob.STATUS_STOPPED);
                JobManager.setEndTime(job);
                log.info("Setting job id ["+jobId+"] to STOPPED");
            }
            LGJobHistory lgHistory = new LGJobHistory(LGJobHistory.LGHISTORY_TYPE_COMMAND,  "Stop", "",
                    "Stop command received", System.currentTimeMillis(), 0, 0, 0, 0, 0);
            JobManager.updateJobHistory(job, lgHistory);
        }
    }

    /**
     * @param tuple Tuple containing LGCommand
     */
    public void execute(Tuple tuple) {
        try {
            LGCommand incomingCmd = (LGCommand) tuple.getValueByField(LGConstants.LG_COMMAND);
            log.info("EXECUTE Job Command:     " + incomingCmd.toString());
            boolean val = false;
            switch (incomingCmd.getCmd()) {
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
                    val = handleAddCommand(incomingCmd);
                    if (!val) {
                        oc.fail(tuple);
                    }
                    break;
                case LGCommand.COMMAND_TYPE_POST_ACTION:
                    val = handlePostActionCommand(incomingCmd, tuple);
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
                case LGCommand.COMMAND_TYPE_RETRY_FAILED_TASKS:
                    val = handleRetryFailedTasks(incomingCmd, tuple);
                    if (!val) {
                        oc.fail(tuple);
                    }
                    break;
                default:
                    log.error("Invalid Job Command received from queue " + incomingCmd.getCmdString());
            }
            // Always ack the tuple
            oc.ack(tuple);
        } catch (Exception e) {
            log.error("Error processing tuple");
            e.printStackTrace();
            oc.fail(tuple);
        }//end of catch
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(LGConstants.LG_JOB_ID, LGConstants.LG_COMMAND, "destination"));
    }

}

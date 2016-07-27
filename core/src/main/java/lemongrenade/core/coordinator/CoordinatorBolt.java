package lemongrenade.core.coordinator;

import lemongrenade.core.database.lemongraph.InvalidGraphException;
import lemongrenade.core.database.lemongraph.LemonGraph;
import lemongrenade.core.database.lemongraph.LemonGraphObject;
import lemongrenade.core.database.lemongraph.LemonGraphResponse;
import lemongrenade.core.database.mongo.MongoDBStore;
import lemongrenade.core.models.LGJob;
import lemongrenade.core.models.LGJobError;
import lemongrenade.core.models.LGPayload;
import lemongrenade.core.models.LGTask;
import lemongrenade.core.templates.LGAdapter;
import lemongrenade.core.util.JSONUtils;
import lemongrenade.core.util.LGConstants;
import lemongrenade.core.util.LGProperties;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

public class CoordinatorBolt extends BaseRichBolt {
    public final static int GRAPH_STORE_LEMONGRAPH = 1;  // use LEMONGRAPH for adapter tasks assignments
    public final static int GRAPH_STORE_INTERNAL   = 2;  // Use internal diffs to figure out adapter tasksFerr

    private final Logger log = LoggerFactory.getLogger(getClass());
    private int graphStoreMode;
    private OutputCollector oc;
    private int boltId;
    private Map<String, JSONObject> mockDatabase;
    private Map<String, Integer> outboundTasks;
    private JobManager jobManager;
    private AdapterManager adapterManager;
    private transient MongoDBStore db;
    private transient LemonGraph lemonGraph;

    // Maximum number of jobs we will send into STORM at once from this lemongrenade.core.coordinator
    private final static int MAX_PROCESSING_JOBS_PER_COORDINATOR = 1000;   // TODO: props file?

    /**
     * Reads the "coordinator.graphstore" LGProperities line and returns the correct int value.
     * Defaults to INTERNAL if not set properly.
     *
     * @return
     */
    private int getGraphStore() {

        String graphStoreStr = LGProperties.get("coordinator.graphstore").toString();
        if (graphStoreStr.equalsIgnoreCase("internal")) {
            return GRAPH_STORE_INTERNAL;
        }
        else if (graphStoreStr.equalsIgnoreCase("lemongraph")) {
            return GRAPH_STORE_LEMONGRAPH;
        }
        else if (graphStoreStr.equalsIgnoreCase("neo4j")) {
            log.warn("NEO4j not supported yet. Defaulting to Internal graph storage.");
            return GRAPH_STORE_INTERNAL;
        }

        log.warn("Invalid coordinator.graphstore setting in props file :"+graphStoreStr+" Defaulting to Internal");
        // default to internal mode
        return GRAPH_STORE_INTERNAL;
    }

    public int getMaxProcessingJobs() {
        return MAX_PROCESSING_JOBS_PER_COORDINATOR;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        Config config = LGAdapter.DEFAULT_CONFIG;
        oc = outputCollector;
        jobManager = new JobManager();
        adapterManager = new AdapterManager();
        boltId = topologyContext.getThisTaskId();
        mockDatabase = new HashMap<>();
        outboundTasks = new HashMap<>();
        graphStoreMode = getGraphStore();
        db = new MongoDBStore();
        // TODO: Check to see if lemongraph is even configured before attempting  to connect
        lemonGraph = new LemonGraph();
    }

    /** Helper internal class that maintains the metrics return values */
    private class MetricData {
        protected int graphChanges = 0;
        protected int maxGraphId = 0;
        protected int numberOfNewTasksGenerated = 0;
        protected int currentGraphId = 0;
        public MetricData(int graphChanges, int maxGraphId, int numberOfNewTasksGenerated, int currentGraphId) {
            this.graphChanges = graphChanges;
            this.maxGraphId   = maxGraphId;
            this.numberOfNewTasksGenerated = numberOfNewTasksGenerated;
            this.currentGraphId = currentGraphId;
        }
    }

    /** Helper method for execute()
     * Uses LemonGraph to store and process data
     *
     * @param tuple incoming raw data from storm
     * @param job   job we are processing
     * @param taskId current task for job
     * @return MetricData The amount of graph changes that occurred during the postgraph to LemonGraph
     */
    private MetricData handleLemonGraphProcessingBatch(Tuple tuple, LGJob job, LGPayload payload, String taskId)
            throws JobFailureException, Exception
    {
        int numberOfNewTasksGenerated = 0;

        String jobId = payload.getJobId();
        LemonGraphObject lgo = lemonGraph.buildLemonGraphFromPayloadNodesandEdges(payload.getResponseNodes()
                , payload.getResponseEdges());
        if (payload.getPayloadType().equals(LGConstants.LG_PAYLOAD_TYPE_COMMAND)) {
            lgo.setSeed(true);
        }
        LemonGraphResponse lgr = null;
        try {
            lgr = lemonGraph.postToGraph(jobId, lgo);
            if (lgr.getResponseCode() == 404) {
                // LemonGraph doesn't know about our GRAPH
                throw new JobFailureException("LemonGraph doesn't know about graph :"+jobId+ " task:"+taskId);
            }
        } catch (InvalidGraphException e) {
            log.error("Unable to communicate with Lemongraph and post graph data!!!");
            // TODO?  HOW SHOULD WE HANDLE THIS?
            throw new Exception("Error trying to post new data to graph for job:"+jobId+" task:"
                    + taskId+" "+e.getMessage());
        }

        // If LemonGraph errors out, we need to handle that and not try to query LemonGraph any further on this cycle.
        if (!lgr.getSuccess()) {
            log.error("Error posting data to LemonGraph, aborting job:"+jobId+" Task:"+taskId);
            throw new JobFailureException("Error communicating with LemonGraph job:"+jobId+ " task:"+taskId);
        }

        // Calculate queryId  (maxId - #_of_updates) + 1
        int currentId = (lgr.getMaxId() - lgr.getUpdateCount()) + 1;

        // Store metrics
        MetricData metrics = new MetricData(lgr.getUpdateCount(),lgr.getMaxId(),0, currentId);

        // Query lemongraph for each adapter that we are interested in
        List<String> adapterList = job.getApprovedAdapterList();
        HashMap<String, String> adapterQueryMap = new HashMap<String, String>();
        for (String a : adapterList) {
            String adapterId = adapterManager.findBestAdapterByAdapterName(a);
            String adapterGraphQuery = adapterManager.getGraphQueryForAdapter(a, adapterId, job, true);
            if (!adapterGraphQuery.equals("")) {
                adapterQueryMap.put(adapterId, adapterGraphQuery);
            }
        }

        int maxNodes = LGProperties.getInteger("max_nodes_per_task",0);

        // Send all Queries to LemonGraph
        JSONObject resultdata = lemonGraph.queryBasedOnPatterns(jobId, adapterQueryMap, currentId);

        // Parse LemonGraphResult and generated new tasks as needed
        HashMap<String, JSONArray> resultMap = lemonGraph.parseLemonGraphResult(resultdata);
        for (Map.Entry<String, JSONArray> entry : resultMap.entrySet()) {
            String    query = entry.getKey();
            JSONArray nodes = entry.getValue();
            // Helper will possibly split results into smaller tasks if a payload is too big
            int tasksGenerated = handleLemonGraphProcessingBatchHelper(job, adapterQueryMap, query, nodes, maxNodes);
            numberOfNewTasksGenerated += tasksGenerated;
        }
        metrics.numberOfNewTasksGenerated = numberOfNewTasksGenerated;

        return metrics;
    }

    /**
     * If a response from LemonGraph is over a certain size, we can and will split the result into smaller
     * chucks (multiple tasks)
     *
     * @param job            LGjob that we are processing
     * @param adapterQueryMap  The hash of adapters and queries that we sent to
     * @param query            The query result from Lemongraph
     * @param nodes            The nodes from Lemongraph that matched 'query'
     * @param maxNodesPerTask  The maximum amount of nodes to send to a task before splitting into smaller tasks
     *                         <1 is no split
     * @return int number of tasks we created
     */
    private int handleLemonGraphProcessingBatchHelper(LGJob job, HashMap<String, String> adapterQueryMap
            , String query, JSONArray nodes, int maxNodesPerTask)
    {
        int numberOfNewTasksGenerated = 0;
        JSONArray splitNodes = JSONUtils.splitJsonArray(maxNodesPerTask, nodes);
        for (Map.Entry<String, String> entry2 : adapterQueryMap.entrySet()) {
            String adapterId = entry2.getKey();
            Object adapterQuery = entry2.getValue();
            if (adapterQuery.equals(query)) {
                for (int n=0; n < splitNodes.length(); n++) {
                    JSONArray taskNodes = splitNodes.getJSONArray(n);
                    LGTask newTask = new LGTask(job, adapterId, adapterManager.getAdapterNameById(adapterId),taskNodes.length());
                    LGPayload payload = new LGPayload(job.getJobId(), newTask.getTaskId(), job.getJobConfigAsJSON());
                    payload.addRequestNodes(taskNodes);
                    sendTaskToAdapter(job, newTask, payload);
                    numberOfNewTasksGenerated++;
                }
            }
        }
        return numberOfNewTasksGenerated;
    }

    /**
     * handles internal processing/datastore  (uses mongo to store and process graph data instead of LemonGraph)
     *
     * @return - The number of new tasks that this processing cycle spawned
     * */
    private int handleInternalProcessing(Tuple tuple, LGJob job, LGPayload payload) {
        Map<String, Boolean> dedup = new HashMap<>();
        String job_id = payload.getJobId();
        int numberOfNewTasksGenerated = 0;

        // First iterate over the edges and extract the nodes
        // Add them to the nodes we want to process
        for (JSONArray edge : payload.getResponseEdges()){
            payload.addResponseNode(edge.getJSONObject(0)); //src
            payload.addResponseNode(edge.getJSONObject(2)); //dst
        }

        // Process all of the nodes (including ones on the edge)
        for (JSONObject currVersion : payload.getResponseNodes()) {
            String mockDbKey = currVersion.get("type") + "" + currVersion.get("value");
            if (!dedup.containsKey(mockDbKey)) {
                dedup.put(mockDbKey, true);
                JSONObject cachedVersion = db.hasNode(job_id, mockDbKey) ?
                        db.getNodeByKey(job_id, mockDbKey) : new JSONObject();
                JSONObject diff = JSONUtils.diff(cachedVersion, currVersion);
                JSONObject updatedVersion = JSONUtils.apply(cachedVersion, diff);
                db.saveNode(job_id, mockDbKey, updatedVersion);
                List<String> uniqueAdapterList = adapterManager.buildUniqueAdapterListBasedOnRequiredKeys(diff);
                if (uniqueAdapterList.size() > 0) {
                    log.info("Built new adapter list for job [ " + job.getJobId() + "]  adapterlist: " + uniqueAdapterList.toString());
                    List<LGTask> newTasks = buildListOfTasksBasedOnListOfAdapters(job, uniqueAdapterList, updatedVersion.length());
                    if (newTasks.size() > 0) {
                        log.info("Built new tasks list for job [" + job.getJobId() + "] taskList: " + newTasks.toString());
                        for (LGTask t : newTasks) {
                            LGPayload newPayload = new LGPayload(payload.getJobId(), payload.getTaskId(), payload.getJobConfig());
                            newPayload.addRequestNode(updatedVersion);
                            sendTaskToAdapter(job, t, newPayload);
                            numberOfNewTasksGenerated++;
                        }
                    }
                }
            }
        }

        // Now that all the nodes exist, process the edges
        for (JSONArray edge : payload.getResponseEdges()) {
            JSONObject edgeInfo = edge.getJSONObject(1);
//            String dbKey = edgeInfo.get("type") + "" +
//                    edgeInfo.get("value");
            String srcKey = tuple.getStringByField("job_id") + "" +
                    edge.getJSONObject(0).get("type") + "" +
                    edge.getJSONObject(0).get("value");
            String dstKey = tuple.getStringByField("job_id") + "" +
                    edge.getJSONObject(2).get("type") + "" +
                    edge.getJSONObject(2).get("value");
            db.saveEdge(job_id, UUID.randomUUID().toString(), srcKey, dstKey, edgeInfo);
        }

        return numberOfNewTasksGenerated;
    }

    private void  handleLgCommand(String cmd, LGJob job, LGPayload payload) {
        String jobId = job.getJobId();
        JSONObject cmdConfig = payload.getJobConfig();
        JSONObject cmdData   = new JSONObject();
        log.info("EXECUTE LG_INTERNAL_COMMAND: "+cmd+" job_id: " + jobId+ " coord_id: " + boltId + " data:"
                + cmdConfig.toString());

        if (cmdConfig.has(LGConstants.LG_INTERNAL_DATA)) {
            cmdData = new JSONObject(cmdConfig.getString(LGConstants.LG_INTERNAL_DATA));
        } else {
            log.error("LG_COMMAND missing LG_INTERNAL_DATA in payload.jobConfig! Aborting Command.");
            return;
        }

        if (graphStoreMode == GRAPH_STORE_INTERNAL) {
            log.info("Internal graph mode does not support LG_INTERNAL_COMMANDS");
        }
        else if (graphStoreMode == GRAPH_STORE_LEMONGRAPH) {
            if (cmd.equalsIgnoreCase(LGConstants.LG_INTERNAL_OP_EXECUTE_ON_ADAPTERS )) {
                handleLgCommandExecuteOnAdapters(job, cmdData);
            }
        }
    }

    private void handleLgCommandExecuteOnAdapters(LGJob job, JSONObject cmdData) {
        log.info("Processing LG_OP ExecuteOnAdapters");
        String jobId = job.getJobId();
        JSONArray adapters = new JSONArray();
        JSONArray nodes    = new JSONArray();

        if (cmdData.has("nodes")) {
            nodes = cmdData.getJSONArray("nodes");
        } else {
            log.error("LG_OP Command "+LGConstants.LG_INTERNAL_OP_EXECUTE_ON_ADAPTERS
                    + "Missing required nodes element. aborting.");
            return;
        }
        List<String> nodeListStrings = new ArrayList<String>();
        for(int i=0; i<nodes.length(); i++) {
            nodeListStrings.add(nodes.getString(i));
        }

        if (cmdData.has("onetimeadapterlist")) {
            adapters = cmdData.getJSONArray("onetimeadapterlist");
        } else {
            log.error("LG_OP Command "+LGConstants.LG_INTERNAL_OP_EXECUTE_ON_ADAPTERS
                    + "Missing required onetimeadapterlist element. aborting.");
            return;
        }

        // First thing we do is strip out the lg_internal data from the job, otherwise we could enter a loop
        JSONObject jc = job.getJobConfigAsJSON();
        if (jc != null) {
            jc.remove(LGConstants.LG_INTERNAL_DATA);
            jc.remove(LGConstants.LG_INTERNAL_OP);
            jobManager.updateJobConfig(job, jc);
        }

        // For every adapterQuery, we are going to append ,1(ID=[1,4,12])
        StringBuilder nodelist = new StringBuilder();
        nodelist.append(",1(ID=[");
        boolean isFirst = true;
        for(String nodeid: nodeListStrings) {
            if (!isFirst) {
                nodelist.append(",");
            } else {
                isFirst = false;
            }
            nodelist.append(nodeid);
        }
        nodelist.append("])");

        // Build adapter queries
        HashMap<String, String> adapterQueryMap = new HashMap<>();
        for (int i=0; i < adapters.length(); i++) {
            String a = adapters.getString(i);
            String adapterId = adapterManager.findBestAdapterByAdapterName(a);
            // Build the query from adapter, leave off the depth
            String adapterGraphQuery = adapterManager.getGraphQueryForAdapter(a, adapterId, job, false);
            if (!adapterGraphQuery.equals("")) {
                // Append node list to adapterQuery
                adapterGraphQuery += nodelist.toString();
                adapterQueryMap.put(adapterId, adapterGraphQuery);
            }
        }

        // Send all tmp command to LemonGraph
        JSONObject resultdata = lemonGraph.queryBasedOnPatterns(jobId, adapterQueryMap);
        HashMap<String, JSONArray> resultMap = lemonGraph.parseLemonGraphResult(resultdata);
        for (Map.Entry<String, JSONArray> entry : resultMap.entrySet()) {
            String query = entry.getKey();
            JSONArray resultNodes = entry.getValue();

            // Now send Payload to all adapters that have requested this query
            for (Map.Entry<String, String> entry2 : adapterQueryMap.entrySet()) {
                String adapterId = entry2.getKey();
                Object adapterQuery = entry2.getValue();
                if (adapterQuery.equals(query)) {
                    // Build Payload based off returned resultNodes in query
                    // TODO: Right now, we are assuming only nodes are returned in queries
                    LGPayload newPayload = new LGPayload(job.getJobId(), "", job.getJobConfigAsJSON());
                    for (int i = 0; i < resultNodes.length(); i++) {
                        JSONObject node = resultNodes.getJSONObject(i);
                        newPayload.addRequestNode(node);
                    }
                    LGTask newTask = new LGTask(job, adapterId, adapterManager.getAdapterNameById(adapterId),resultNodes.length());
                    newPayload.setTaskId(newTask.getTaskId());
                    sendTaskToAdapter(job, newTask, newPayload);
                }
            }
        }
    }


    /**
     * If we want to assure that jobs end up on the exact same lemongrenade.core.coordinator instance,
     * look up "fieldsGrouping"
     *
     * Fields grouping: The stream is partitioned by the fields specified in the grouping.
     * For example, if the stream is grouped by the "user-id" field, tuples with the same "user-id"
     * will always go to the same task, but tuples with different "user-id"'s may go to different tasks.
     *
     * Should probably look at the diff between the input/output to determine which new lemongrenade.adapters to
     * trigger instead of utilizing the _adapters parameter updateTaskBasedOnStatus.
     */
    public void execute(Tuple tuple) {
        boolean errorProcessingTuple = false;
        int numberOfNewTasksGenerated = 0;
        MetricData md;

        String job_id = tuple.getStringByField(LGConstants.LG_JOB_ID);
        //log.info("EXECUTE job_id: " + job_id + " coord_id: " + boltId);
        LGPayload payload = (LGPayload) tuple.getValueByField(LGConstants.LG_PAYLOAD);
        String tmpTaskId = "unknown";
        if (payload != null) {
            tmpTaskId = payload.getTaskId();
        }
        log.info("EXECUTE job_id: " + job_id + " task_id:"+tmpTaskId+" coord_id:" + boltId);

        // Does job exist, if not, error out
        if (!jobManager.doesJobExist(payload.getJobId())) {
            log.error("Unable to find Job [" + payload.getJobId() + "] in CoordinatorBolt");
            // TODO: How should we handle this condition
            oc.ack(tuple);
            return;
        }

        // Set Job Information
        LGJob job = jobManager.getJob(payload.getJobId());
        jobManager.setStatus(job,LGJob.STATUS_PROCESSING);
        job.setCoordinatorId(boltId);

        // Look for payloads that are marked as internal_processing commands (handled differently than normal job
        // submissions and adapter_results.
        // jobs with "lg_internal_op" in the job_config are processed here
        if(payload.getJobConfig().has(LGConstants.LG_INTERNAL_OP)) {
            String cmd = payload.getJobConfig().getString(LGConstants.LG_INTERNAL_OP);
            log.info("Processing lg_internal_op :"+cmd);
            handleLgCommand(cmd, job, payload);
            oc.ack(tuple);
            return;
        }

        // Store the INCOMING taskId temporarily because we reuse it later
        String taskId = payload.getTaskId();

        // Strip task Id of out the payload because the task is 'done'
        payload.delTaskId();

        // If we get a failure response, we handle that differently. Usually means the adapter dead-lettered
        if (payload.getPayloadType().equalsIgnoreCase(LGConstants.LG_PAYLOAD_TYPE_ADAPTERRESPONSE_FAILURE)) {
            String jobId = job.getJobId();
            LGTask task = jobManager.getTask(taskId);
            String adapterName = task.getAdapterName();
            String adapterId   = task.getAdapterId();
            LGJobError error = new LGJobError(jobId, taskId, adapterName, adapterId,
                    "Failure payload received from "+adapterName);
            jobManager.updateErrorsForJob(jobId, error);
            log.warn("Received adapter failed message for job_id:"+job.getJobId()+" task_id:"+taskId);
            jobManager.updateHistoryAdapterFailedForJob(job.getJobId(), taskId);
            updateTaskToFailed(job.getJobId(), taskId);
        } else {
            // Process incoming data from adapters
            if (graphStoreMode == GRAPH_STORE_INTERNAL) {
                log.info("Processing data with internal graph store.");
                numberOfNewTasksGenerated = handleInternalProcessing(tuple, job, payload);
                // TODO: have Internal return a metrics value
                int metrics    = 0;
                int maxGraphId = 0;
                int currentId  = 0;
                updateJobHistory(job.getJobId(), taskId, metrics, maxGraphId, numberOfNewTasksGenerated, currentId);
                updateTaskToCompleted(job.getJobId(), taskId);
            } else if (graphStoreMode == GRAPH_STORE_LEMONGRAPH) {
                try {
                    // Send the taskID along for storing metrics
                    md = handleLemonGraphProcessingBatch(tuple, job, payload, taskId);
                    updateJobHistory(job.getJobId(), taskId, md.graphChanges, md.maxGraphId,
                            md.numberOfNewTasksGenerated, md.currentGraphId);
                    jobManager.updateGraphActivity(job.getJobId(), md.currentGraphId);
                    updateTaskToCompleted(job.getJobId(), taskId);
                } catch (JobFailureException e) {
                    // There was an error processing the incoming tuple
                    String jobId = job.getJobId();
                    LGTask task = jobManager.getTask(taskId);
                    log.error("Fatal Job Failure  job:" + job.getJobId() + "task:"+taskId+ " " + e.getMessage());

                    jobManager.setStatus(job, LGJob.STATUS_ERROR);
                    String adapterName = task.getAdapterName();
                    String adapterId   = task.getAdapterId();
                    // (Don't set errorProcessingTuple ... we want to ack the tuple in this situation
                    LGJobError error = new LGJobError(jobId, taskId, adapterName, adapterId,
                            "Failure payload received from "+adapterName+"  We will retry task.");
                    jobManager.updateErrorsForJob(jobId, error);

                } catch (Exception e) {
                    // There was an error processing the incoming tuple
                    log.error("Problem handing incoming tuple for job :" + job.getJobId() + " " + e.toString());
                    String jobId = job.getJobId();
                    LGTask task = jobManager.getTask(taskId);
                    String adapterName = task.getAdapterName();
                    String adapterId   = task.getAdapterId();
                    log.error("Fatal Job Failure  job:" + job.getJobId() + "task:"+taskId+ " " + e.toString());
                    errorProcessingTuple = true;
                }
            }
        }

        // Look to see if we have any remaining tasks for that job, if not it's 'finished'
        if (errorProcessingTuple) {
            log.error("Failing tuple. job:"+job.getJobId()+" task:"+taskId);
            oc.fail(tuple);
        } else {
            jobManager.checkIfJobFinished(job.getJobId());
            oc.ack(tuple);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(LGConstants.LG_JOB_ID, LGConstants.LG_PAYLOAD, "destination"));
    }

    /**
     * update History based on the results of this Task
     * @param jobId
     * @param taskId
     * @param graphChanges
     * @param maxGraphId
     * @param currentId
     * @param numberOfNewTasksCreated  Number of new tasks that were created as a result of this adapter result
     */
    private void updateJobHistory(String jobId, String taskId, int graphChanges, int maxGraphId
            , int numberOfNewTasksCreated, int currentId) {
        if ((taskId == null) || (taskId.equals(""))){
            return;  // Most likely a seed job.?
        }
        jobManager.updateHistoryAdapterForJob(jobId, taskId, graphChanges, maxGraphId
                , numberOfNewTasksCreated, currentId);
    }

    /**
     * Update the task status to COMPLETE
     */
    private void updateTaskToCompleted(String jobId, String taskId) {
        if ((taskId == null) || (taskId.equals(""))){
            return;  // Most likely a seed job.?
        }
        jobManager.updateJobTaskStatus(jobId,taskId, LGTask.TASK_STATUS_COMPLETE);
    }

    /**
     * Update the task status to FAILED
     */
    private void updateTaskToFailed(String jobId, String taskId) {
        if ((taskId == null) || (taskId.equals(""))){
            return;  // Most likely a seed job.?
        }
        jobManager.updateJobTaskStatus(jobId,taskId,LGTask.TASK_STATUS_FAILED);
    }

    /**
     * Sends given task and payload to the adapter via adapter queues
     */
    private void sendTaskToAdapter(LGJob job, LGTask task, LGPayload lgp) {
        log.info("Sending Task:"+task.getTaskId()
                 + " TO Adapter:"+adapterManager.getAdapterNameById(task.getAdapterId())+" ["+task.getAdapterId() +"]"
                 + " Job:" + job.getJobId());
        jobManager.addTaskToJob(job,task);
        lgp.setTaskId(task.getTaskId());
        adapterManager.incrementTaskCount(task.getAdapterId());
        jobManager.setStatus(job,LGJob.STATUS_PROCESSING);  // There's no fear of race condition here
        oc.emit(new Values(job.getJobId(), lgp, adapterManager.getAdapterQueueNameById(task.getAdapterId())));
    }

    /**
     * Given the job and list of uniqueAdapter List, returns a list of tasks to process
     * Note: this is only used by the internal processing code (not used for LemonGraph)
     * @param job The incoming job
     * @param uniqueAdapterList  a list of adapters with duplicates removed
     * @return List<LGTask>
     */
    private List<LGTask> buildListOfTasksBasedOnListOfAdapters(LGJob job, List<String> uniqueAdapterList, int nodeSize) {
        List<LGTask> newTasks = new ArrayList<>();
        for (String adapterId : uniqueAdapterList) {
            // Look to first see if this job is on the approved lemongrenade.adapters list
            String adapterName = adapterManager.getAdapterNameById(adapterId);

            if (job.isAdapterApproved(adapterName.toLowerCase())) {
                LGTask task = new LGTask(job, adapterId, adapterName, nodeSize);
                log.info("Built tasks for Task:" + task.getTaskId()
                         + " Adapter:" + adapterManager.getAdapterNameById(adapterId)
                         + "[" + adapterId + "] Job:" + job.getJobId());
                newTasks.add(task);
            } else {
                log.info("Skipping adapter for this job. Adapter not on approved list: "+adapterName);
            }
        }
        return newTasks;
    }
}

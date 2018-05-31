package lemongrenade.core.coordinator;

import lemongrenade.core.database.lemongraph.InvalidGraphException;
import lemongrenade.core.database.lemongraph.LemonGraph;
import lemongrenade.core.database.lemongraph.LemonGraphResponse;
import lemongrenade.core.database.mongo.MongoDBStore;
import lemongrenade.core.models.*;
import lemongrenade.core.util.JSONUtils;
import lemongrenade.core.util.LGConstants;
import lemongrenade.core.util.LGProperties;
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
    private final static int DEFAULT_MAX_GRAPH_SIZE = 50000; //default value to use if no props file config is found
    private final static int DEFAULT_MAX_PROCESSING_JOBS_PER_COORDINATOR = 1000; //default value
    static protected final Logger log = LoggerFactory.getLogger(CoordinatorBolt.class);
    private int graphStoreMode;
    protected OutputCollector oc;
    protected int boltId;

    // Maximum number of jobs we will send into STORM at once from this lemongrenade.core.coordinator
    private final static int MAX_PROCESSING_JOBS_PER_COORDINATOR = LGProperties.getInteger(
            "max_processing_jobs_per_coordinator", DEFAULT_MAX_PROCESSING_JOBS_PER_COORDINATOR);
    //Maximum number of activities that a graph can have before we issue a stop commnand
    private final static int MAX_GRAPH_SIZE = LGProperties.getInteger("max_graph_size",DEFAULT_MAX_GRAPH_SIZE);

    protected void close() {
        try {
            AdapterManager.close();
            JobManager.close();
            LemonGraph.close();
        }
        catch(Exception e) {
            log.error("Failed to close connections.");
            e.printStackTrace();
        }
    }

    /**
     * Reads the "coordinator.graphstore" LGProperties line and returns the correct int value.
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

    public int getMaxProcessingJobs() {return MAX_PROCESSING_JOBS_PER_COORDINATOR;}

    /** Helper internal class that maintains the metrics return values */
    public class MetricData {
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

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        oc = outputCollector;
        boltId = topologyContext.getThisTaskId();
        graphStoreMode = getGraphStore();
    }

    public void execute(Tuple tuple) {
        String job_id = null;
        try {
            LGPayload payload = (LGPayload) tuple.getValueByField(LGConstants.LG_PAYLOAD);
            job_id = payload.getJobId();
            execute(payload);
            log.info("Acknowledging tuple. job:" + job_id + " task:" + payload.getTaskId());// Success, so ack the tuple
        }
        catch(Exception e) {
            log.error("Error caught while processing tuple for jobId:" + job_id + ". Error:" + e.getMessage());
            e.printStackTrace();
        }
        oc.ack(tuple);
    }

    public void execute(LGPayload payload) {
        String job_id = payload.getJobId();
        String tmpTaskId = "unknown";
        if (payload != null) {
            tmpTaskId = payload.getTaskId();
        }
        log.info("EXECUTE job_id: " + job_id + " task_id:" + tmpTaskId + " coord_id:" + boltId);

        // Does job exist, if not, error out
        if (!JobManager.doesJobExist(job_id)) {
            log.error("Unable to find Job [" + job_id + "] in CoordinatorBolt");
            return;
        }

        // Set Job Information
        LGJob job = JobManager.getJob(job_id); //Get job from Mongo
        job.setCoordinatorId(boltId);

        // Look for payloads that are marked as internal_processing commands (handled differently than normal job
        // submissions and adapter_results.
        // jobs with "lg_internal_op" in the job_config are processed here
        if (payload.getJobConfig().has(LGConstants.LG_INTERNAL_OP)) {
            String cmd = payload.getJobConfig().getString(LGConstants.LG_INTERNAL_OP);
            log.info("Processing lg_internal_op :" + cmd);
            handleLgCommand(cmd, job, payload);
            return;
        }

        String taskId = payload.getTaskId();// Store the INCOMING taskId temporarily because we reuse it later
        payload.delTaskId();// Strip task Id of out the payload because the task is 'done'

        //Determine to use LEMONGRAPH or other graph and perform processing
        Boolean errorProcessingTuple = graphProcessing(job, payload, taskId);

        if (errorProcessingTuple) {
            log.error("Error occurred processing tuple. job:" + job_id + " task:" + taskId);
        }
    }

    //All graph processing is done here. The proper graph processing calls are made from here
    public Boolean graphProcessing(LGJob job, LGPayload payload, String taskId) {
        String jobId = job.getJobId();
        Integer newTasksGenerated = 0;
        Boolean errorProcessingTuple = false;

        //If job has stopped, we shouldn't do any more processing. This assures a job can be deleted.
        if(!(job.getStatus() == job.STATUS_STOPPED || maxGraphActivity(job, taskId))) { //job hasn't stopped, continue
            // If we get a failure response, we handle that differently. Usually means the adapter dead-lettered
            if (payload.getPayloadType().equalsIgnoreCase(LGConstants.LG_PAYLOAD_TYPE_ADAPTERRESPONSE_FAILURE)) {
                log.warn("Received adapter failed message for job_id:" + jobId + " task_id:" + taskId);
                List<LGJobError> taskErrors = job.getTaskErrors(taskId);
                try {
                    LGJobError error;
                    if (taskErrors.size() > 0) {
                        error = taskErrors.get(taskErrors.size() - 1); //grab the last Error for this task to report (others not important)
                    } else {
                        log.info("topology.message.timeout.secs reached for job:" + jobId + " taskId:" + taskId + ".");
                        String msg = "Failed to receive a response.";
                        LGTask task = JobManager.getTask(taskId);
                        error = new LGJobError(job.getJobId(), taskId, task.getAdapterName(), task.getAdapterId(), msg);
                    }
                    JobManager.updateErrorsForJob(job, error); //make only 1 of the 3 errors visible to the user
                    JobManager.updateJobHistoryFailed(jobId, taskId);
                    JobManager.updateTaskToFailed(job, taskId);
                } catch (Exception e) {
                    log.error("Couldn't update job:" + jobId + " task:" + taskId + " with errors.");
                    e.printStackTrace();
                    JobManager.updateTaskToFailed(job, taskId);
                }
            } //end of adapter response failure processing
            else {
                // Process incoming data from adapters
                if (graphStoreMode == GRAPH_STORE_INTERNAL) {
                    log.info("Processing data with internal graph store.");
                    // TODO: have Internal return a metrics value
                    int graphChanges = 0;
                    int maxGraphId = 0;
                    newTasksGenerated = handleInternalProcessing(job, payload);
                    int currentId = 0;
                    MetricData md = new MetricData(graphChanges, maxGraphId, newTasksGenerated, currentId);
                    JobManager.updateJobHistorySuccess(job, taskId, md);
                    JobManager.updateTaskToCompleted(job, taskId);
                } else if (graphStoreMode == GRAPH_STORE_LEMONGRAPH) {
                    try {
                        MetricData md = lemongraphProcessing(job, payload, taskId);
                        newTasksGenerated = md.numberOfNewTasksGenerated;
                    } catch (Exception e) {
                        errorProcessingTuple = true;
                    }
                }//end of else, lemongraphProcessing
            }//end of success response processing
        }//end of STOPPED check
        else {
            JobManager.updateTaskToDropped(job, taskId);//drop the task for STOPPED job
        }

        if(newTasksGenerated == 0) {
            JobManager.updateJobIfFinished(job);//sets job to finished if active task count is 0
        }
        return errorProcessingTuple;
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
     * @param job LGJob
     * @param payload LGPayload
     * @param taskId Adapter task ID
     * @throws Exception Throws an exception for job failure.
     * @return MetricData
     */
    public MetricData lemongraphProcessing(LGJob job, LGPayload payload, String taskId) throws Exception {
        String jobId = job.getJobId();
        try {
            MetricData md = handleLemonGraphProcessingBatch(job, payload, taskId);
            JobManager.updateGraphActivity(job, md.maxGraphId);
            JobManager.updateJobHistorySuccess(job, taskId, md);
            JobManager.updateTaskToCompleted(job, taskId);
            return md;
        } catch (Exception e) {// There was an error processing the incoming tuple
            log.error("Problem handling incoming tuple for job :" + jobId + " " + e.toString());
            LGTask task = JobManager.getTask(taskId);
            String adapterName = "";
            String adapterId   = "";
            if (task != null) {
                adapterName = task.getAdapterName();
                adapterId = task.getAdapterId();
            }
            log.error("Fatal Job Failure  job:" + jobId + "task:" + taskId + " " + " adapterName:" + adapterName + adapterId + " " + e.toString());
            JobManager.updateTaskToFailed(job, taskId);
            throw e;
        }
    }

    /** Helper method for execute()
     * Uses LemonGraph to store and process data
     *
     * @param job   job we are processing
     * @param taskId current task for job
     * @return MetricData The amount of graph changes that occurred during the postgraph to LemonGraph
     */
    private MetricData handleLemonGraphProcessingBatch(LGJob job, LGPayload payload, String taskId) throws Exception {
        int numberOfNewTasksGenerated = 0;
        LemonGraphResponse lgr;
        String jobId = payload.getJobId();
        try {
            //Add new items to Lemongraph
            lgr = LemonGraph.postToGraph(payload);
            if (lgr.getResponseCode() == 404) {
                // LemonGraph doesn't know about our GRAPH
                throw new JobFailureException("LemonGraph doesn't know about graph :"+jobId+ " task:"+taskId);
            }
        } catch (InvalidGraphException e) {
            log.error("Unable to communicate with Lemongraph and post graph data!");
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
        int maxGraphId = lgr.getMaxId();

        // Store metrics
        MetricData metrics = new MetricData(lgr.getUpdateCount(),maxGraphId,0, currentId);

        // Query lemongraph for each adapter that we are interested in
        if(!(job.getStatus() == job.STATUS_STOPPED || maxGraphActivity(job, taskId))) { //job hasn't stopped, continue
            HashMap<String, String> adapterQueryMap = getAdapterQueryMap(job);
            int maxNodes = LGProperties.getInteger("max_nodes_per_task", 0);

            // Send all Queries to LemonGraph
            JSONObject resultdata = LemonGraph.queryBasedOnPatterns(jobId, adapterQueryMap, currentId);

            // Parse LemonGraphResult and generate new tasks as needed
            HashMap<String, JSONArray> resultMap = LemonGraph.parseLemonGraphResult(resultdata);
            Set<Map.Entry<String, JSONArray>> entrySet = resultMap.entrySet();
            for (Map.Entry<String, JSONArray> entry : entrySet) {
                String query = entry.getKey();
                JSONArray nodes = entry.getValue();
                int tasksGenerated = handleLemonGraphProcessingBatchHelper(job, taskId, adapterQueryMap, query, nodes, maxNodes, currentId, maxGraphId, null);
                numberOfNewTasksGenerated += tasksGenerated;
            }
            metrics.numberOfNewTasksGenerated = numberOfNewTasksGenerated;
        }
        else {
            log.info("job:" + job.getJobId() + " has STOPPED. Dropping new tasks.");
        }
        return metrics;
    }

    //Takes an LGJob and returns a map of adapter IDs to lemongraph queries
    public static HashMap getAdapterQueryMap(LGJob job) {
        Set<String> approvedAdapterNames = job.getApprovedAdapterSet();
        HashMap<String, String> adapterQueryMap = new HashMap<>();
        Set<String> approvedAdapterIDs = getAdapterIDs(approvedAdapterNames);
        for (String adapterId : approvedAdapterIDs) {
            String adapterGraphQuery = AdapterManager.getGraphQueryForAdapter(adapterId, job, true);
            adapterQueryMap.put(adapterId, adapterGraphQuery);
        }
        return adapterQueryMap;
    }

    //Translates a set of adapter names into a set of adapter IDs
    public static Set<String> getAdapterIDs(Set<String> adapterNames) {
        Set<String> adapterIDs = new HashSet();
        Iterator iterator = adapterNames.iterator();
        while(iterator.hasNext()) {
            String name = (String) iterator.next();
            String adapterId = AdapterManager.findBestAdapterByAdapterName(name);
            if (adapterId.equals("")) {
                log.warn("Unknown or disabled adapter requested [" + name + "]. Ignoring request.");
            }
            else {
                adapterIDs.add(adapterId);
            }
        }
        return adapterIDs;
    }

    /**
     * If a response from LemonGraph is over a certain size, we can and will split the result into smaller
     * chunks (multiple tasks)
     *
     * @param job              LGjob that we are processing
     * @param currentTaskId    id of the task that we are currently processing.
     * @param adapterQueryMap  hash of adapters and queries that we sent to
     * @param query            query result from Lemongraph
     * @param nodes            nodes from Lemongraph that matched 'query'
     * @param maxNodesPerTask   maximum amount of nodes to send to a task before splitting into smaller tasks
     *                         less than 1 is no split
     * @param currentGraphId   used for replay/retry - leave 0 if not needed for task
     * @param maxGraphId        used for replay/retry - leave 0 if not needed for task
     * @param nodeIndex Integer for node index
     * @return int number of tasks we created
     */
    protected int handleLemonGraphProcessingBatchHelper(LGJob job, String currentTaskId,  HashMap<String, String> adapterQueryMap
            , String query, JSONArray nodes, int maxNodesPerTask, int currentGraphId, int maxGraphId, Integer nodeIndex) {
        int numberOfNewTasksGenerated = 0;
        if(!(job.getStatus() == job.STATUS_STOPPED || maxGraphActivity(job, currentTaskId))) { //job hasn't stopped, continue
            JSONArray splitNodes = JSONUtils.splitJsonArray(maxNodesPerTask, nodes); //split nodes into arrays <= maxNodesPerTask
            for (Map.Entry<String, String> entry2 : adapterQueryMap.entrySet()) {
                String adapterId = entry2.getKey();
                Object adapterQuery = entry2.getValue();
                LGAdapterModel a = AdapterManager.getAdapterById(adapterId);
                if (adapterQuery.equals(query)) {
                    // If adapter has an override for maxNodesPerTask (>0) we use that instead
                    // Which makes our lives much more difficult. So, deal with the first (ideal) case where an adapter
                    // DOES NOT have a maxNodesPerTask setting (==0)
                    // Very important that you don't split when there's only 1 node - we allow the adapter to override
                    // this because there's some adapters that can only handle one node at at time
                    if ((a.getMaxNodesPerTask() == 0) || (nodes.length() == 1)) {
                        for (int n = 0; n < splitNodes.length(); n++) {
                            if (nodeIndex == null || n == nodeIndex) {
                                JSONArray taskNodes = splitNodes.getJSONArray(n);
                                LGTask newTask = new LGTask(job, adapterId, a.getName(), taskNodes.length(), currentTaskId, currentGraphId, maxGraphId, n);
                                LGPayload payload = new LGPayload(job.getJobId(), newTask.getTaskId(), job.getJobConfigAsJSON());
                                payload.addRequestNodes(taskNodes);
                                sendTaskToAdapter(job, newTask, payload);
                                numberOfNewTasksGenerated++;
                            }
                        }
                    }
                    else {
                        // Nothing is easy, first log a warning
                        log.warn("Adapter " + a.getName() + " has an individual max_nodes_per_task setting, parsing tasks");
                        int max_nodes = a.getMaxNodesPerTask();
                        JSONArray splitNodesForAdapterSetting = JSONUtils.splitJsonArray(max_nodes, nodes);
                        for (int n = 0; n < splitNodesForAdapterSetting.length(); n++) {
                            if (nodeIndex == null || n == nodeIndex) {
                                JSONArray taskNodes = splitNodesForAdapterSetting.getJSONArray(n);
                                LGTask newTask = new LGTask(job, adapterId, a.getName(), taskNodes.length(), currentTaskId, currentGraphId, maxGraphId, n);
                                LGPayload payload = new LGPayload(job.getJobId(), newTask.getTaskId(), job.getJobConfigAsJSON());
                                payload.addRequestNodes(taskNodes);
                                sendTaskToAdapter(job, newTask, payload);
                                numberOfNewTasksGenerated++;
                            }
                        }
                    }
                }
            }
        }
        else {
            log.info("job:"+job.getJobId()+" has STOPPED. Dropping new tasks.");
        }
        return numberOfNewTasksGenerated;
    }

    /**
     * handles internal processing/datastore  (uses mongo to store and process graph data instead of LemonGraph)
     *
     * @return - The number of new tasks that this processing cycle spawned
     * */
    private int handleInternalProcessing(LGJob job, LGPayload payload) {
        Map<String, Boolean> dedup = new HashMap<>();
        String job_id = payload.getJobId();
        int numberOfNewTasksGenerated = 0;
        String taskId = payload.getTaskId();

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
                JSONObject cachedVersion = MongoDBStore.hasNode(job_id, mockDbKey) ?
                        MongoDBStore.getNodeByKey(job_id, mockDbKey) : new JSONObject();
                JSONObject diff = JSONUtils.diff(cachedVersion, currVersion);
                JSONObject updatedVersion = JSONUtils.apply(cachedVersion, diff);
                MongoDBStore.saveNode(job_id, mockDbKey, updatedVersion);
                List<String> uniqueAdapterList = AdapterManager.buildUniqueAdapterListBasedOnRequiredKeys(diff);

                if (uniqueAdapterList.size() > 0) {
                    log.info("Built new adapter list for job [ " + job.getJobId() + "]  adapterlist: " + uniqueAdapterList.toString());
                    List<LGTask> newTasks = buildListOfTasksBasedOnListOfAdapters(job, taskId, uniqueAdapterList, updatedVersion.length(),0,0);
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
            String srcKey = job_id + "" +
                    edge.getJSONObject(0).get("type") + "" +
                    edge.getJSONObject(0).get("value");
            String dstKey = job_id + "" +
                    edge.getJSONObject(2).get("type") + "" +
                    edge.getJSONObject(2).get("value");
            MongoDBStore.saveEdge(job_id, UUID.randomUUID().toString(), srcKey, dstKey, edgeInfo);
        }
        return numberOfNewTasksGenerated;
    }//end handleInternalProcessing

    private void handleLgCommand(String cmd, LGJob job, LGPayload payload) {
        String jobId = job.getJobId();
        JSONObject cmdConfig = payload.getJobConfig();
        JSONObject cmdData;
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
                handleLgCommandPostAction(job, cmdData);
            }
        }
    }

    private void handleLgCommandPostAction(LGJob job, JSONObject cmdData) {
        log.info("Processing LG_OP PostAction:"+cmdData.toString());
        String jobId = job.getJobId();
        ArrayList<String> adapters;
        JSONArray nodes;
        JSONObject jobConfig;

        //Set a new start time if the job isn't already processing. This prevents calculating bad runtime durations.
        if (job.getActiveTaskCount() == 0) {
            JobManager.setStartTime(job);
        }

        List<String> nodeListStrings = new ArrayList<String>();

        try {
            if (!cmdData.has("post_action_job_config")) {
                log.error("LG_OP Command "+LGConstants.LG_INTERNAL_OP_EXECUTE_ON_ADAPTERS
                        + "Missing required post_action_job_config aborting.");
                return;
            }
            jobConfig = cmdData.getJSONObject("post_action_job_config");

            if (jobConfig.has("nodes")) {
                nodes = jobConfig.getJSONArray("nodes");
            } else {
                log.error("LG_OP Command "+LGConstants.LG_INTERNAL_OP_EXECUTE_ON_ADAPTERS
                        + "Missing required nodes element. aborting.");
                return;
            }

            for (int i = 0; i < nodes.length(); i++) {
                nodeListStrings.add(nodes.get(i).toString());//get item and convert to String
            }
        }
        catch(Exception e) {
            e.printStackTrace();
            log.error("Error processing postaction nodes. " + e.getLocalizedMessage());
            log.error("Error:"+e.toString());
            return;
        }

        try {
            adapters = AdapterManager.parseAdaptersListFromJobConfig(jobConfig);
        }
        catch (Exception e) {
            log.error("LG_OP Command "+LGConstants.LG_INTERNAL_OP_EXECUTE_ON_ADAPTERS
                    + "Error parsing adapter list in post_action_job_config "+e.toString());
            return;
        }

        // First thing we do is strip out the lg_internal data from the job, otherwise we could enter a loop
        jobConfig.remove(LGConstants.LG_INTERNAL_DATA);
        jobConfig.remove(LGConstants.LG_INTERNAL_OP);

        if(!jobConfig.has("roles")) { //post action sent without roles, add previously used roles, if there are any
            try {
                LGJob oldJob = JobManager.getJob(job.getJobId());
                JSONObject oldJobConfig = oldJob.getJobConfigAsJSON();
                if(oldJobConfig.has("roles")) {
                    jobConfig.put("roles", oldJobConfig.getJSONObject("roles"));
                }
            }
            catch(Exception e) {
                e.printStackTrace();
                log.error(e.getMessage());
            }
        }

        //Use the post action job config instead of the original
        job.setJobConfig(jobConfig.toString());

        // For every adapterQuery, we are going to append ,1(ID=[1,4,12]). 0 is an invalid ID, and causes Lgraph errors.
        StringBuilder nodeList = new StringBuilder();
        nodeList.append(",1(ID=[");
        boolean isFirst = true;
        for(String nodeid: nodeListStrings) {
            if (!isFirst) {
                nodeList.append(",");
            } else {
                isFirst = false;
            }
            nodeList.append(nodeid);
        }
        nodeList.append("])");

        // Build adapter queries
        HashMap<String, String> adapterQueryMap = new HashMap<>();
        for (String a: adapters) {
            String adapterId = AdapterManager.findBestAdapterByAdapterName(a);
            // Build the query from adapter, leave off the depth
            String adapterGraphQuery = AdapterManager.getGraphQueryForAdapter(adapterId, job, false);
            if (!adapterGraphQuery.equals("")) {
                // Append node list to adapterQuery
                adapterGraphQuery += nodeList.toString();
                adapterQueryMap.put(adapterId, adapterGraphQuery);
            }
        }

        int maxNodes = LGProperties.getInteger("max_nodes_per_task", 0);
        if(!(job.getStatus() == job.STATUS_STOPPED || maxGraphActivity(job, ""))) { //job hasn't stopped, continue
            // Send all tmp command to LemonGraph
            JSONObject resultData = LemonGraph.queryBasedOnPatterns(jobId, adapterQueryMap);
            HashMap<String, JSONArray> resultMap = LemonGraph.parseLemonGraphResult(resultData);

            // Parse LemonGraphResult and generated new tasks as needed
            Set<Map.Entry<String, JSONArray>> entrySet = resultMap.entrySet();
            int newTasksGenerated = 0;

            for (Map.Entry<String, JSONArray> entry : entrySet) {
                String query = entry.getKey();
                JSONArray queryNodes = entry.getValue();
                // Helper will possibly split results into smaller tasks if a payload is too big
                newTasksGenerated += handleLemonGraphProcessingBatchHelper(job, "", adapterQueryMap, query, queryNodes, maxNodes, 0, 0, null);
            }

            JobManager.updateJobIfFinished(job);//sets job finished items if no active tasks remain
        }
    }//end of postAction

    //Checks of graph activity is > MAX_GRAPH_SIZE. Updates the job status to STOPPED and returns true, if so.
    public Boolean maxGraphActivity(LGJob job, String taskId) {
        if(MAX_GRAPH_SIZE <= 0)//ignore for MAX_GRAPH_SIZE <=0
            return false;

        Boolean errorCheck = false;
        // Check to see if GraphActivity exceeds threshold; if so, issue STOP command for this job
        int graphActivity = JobManager.getJob(job.getJobId()).getGraphActivity();

        if (graphActivity > MAX_GRAPH_SIZE && MAX_GRAPH_SIZE > 0) { //ignore for MAX_GRAPH_SIZE <=0
            JobManager.setStatus(job, LGJob.STATUS_STOPPED, "Exceeded MAX_GRAPH_SIZE", true);
            String msg = "job:"+job.getJobId()+" GraphActivity:"+graphActivity+" exceeds the MAX_GRAPH_SIZE:" + MAX_GRAPH_SIZE;
            log.warn(msg);
            LGTask task = JobManager.getTask(taskId);
            LGJobError error = new LGJobError(job.getJobId(), taskId, "Job Error", task.getAdapterId(), msg);
            JobManager.updateMaxGraphSizeErrorsForJob(job.getJobId(), error);
            errorCheck = true;
        }
       return errorCheck;
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(LGConstants.LG_JOB_ID, LGConstants.LG_PAYLOAD, "destination"));
    }

    /*** Sends given task and payload to the adapter via adapter queues
     * @param job LGJob
     * @param task LGTask
     * @param lgp LGPayload
     * */
    protected void sendTaskToAdapter(LGJob job, LGTask task, LGPayload lgp) {// Don't emit a task for a stopped adapter
        if(!(job.getStatus() == job.STATUS_STOPPED || maxGraphActivity(job, task.getTaskId()))) { //job hasn't stopped, continue
            log.info("Sending Task:" + task.getTaskId()
                    + " to Adapter:" + AdapterManager.getAdapterNameById(task.getAdapterId()) + " [" + task.getAdapterId() + "]"
                    + " Job:" + job.getJobId()
                    + " Status:" + job.getStatusString(job.getStatus()));
            JobManager.addTaskToJob(job, task);
            lgp.setTaskId(task.getTaskId());
            AdapterManager.incrementTaskCount(task.getAdapterId());
            Boolean statusUpdated = JobManager.setStatus(job, LGJob.STATUS_PROCESSING);  // There's no fear of race condition here
            if (statusUpdated) { //Don't emit a new task if the status couldn't be changed. This prevents STOPPED tasks from receiving additional tasks to process.
                oc.emit(new Values(job.getJobId(), lgp, AdapterManager.getAdapterQueueNameById(task.getAdapterId())));
            } else {
                log.info("Couldn't update status for job:" + job.getJobId() + " task:" + task.getTaskId() + ". Dropping task.");
                JobManager.updateTaskToDropped(job, task.getTaskId());
            }
        }
        else {
            log.info("Job:"+job.getJobId()+" has STOPPED. Dropping task:"+task.getTaskId());
            JobManager.updateTaskToDropped(job, task.getTaskId());
        }
    }

    /**
     * Given the job and list of uniqueAdapter List, returns a list of tasks to process
     * Note: this is only used by the internal processing code (not used for LemonGraph)
     * @param job The incoming job
     * @param taskId current task id that we are processing
     * @param uniqueAdapterList  a list of adapters with duplicates removed
     * @return List<LGTask>
     */
    private List<LGTask> buildListOfTasksBasedOnListOfAdapters(LGJob job, String taskId, List<String> uniqueAdapterList,
                                                               int nodeSize, int currentGraphId, int maxGraphId) {
        List<LGTask> newTasks = new ArrayList<>();
        for (String adapterId : uniqueAdapterList) {
            // Look to first see if this job is on the approved lemongrenade.adapters list
            String adapterName = AdapterManager.getAdapterNameById(adapterId);

            if (job.isAdapterApproved(adapterName.toLowerCase())) {
                LGTask task = new LGTask(job, adapterId, adapterName, nodeSize, taskId, currentGraphId, maxGraphId, 0);
                log.info("Built tasks for Task:" + task.getTaskId()
                         + " Adapter:" + AdapterManager.getAdapterNameById(adapterId)
                         + "[" + adapterId + "] Job:" + job.getJobId());
                newTasks.add(task);
            } else {
                log.info("Skipping adapter for this job. Adapter not on approved list: "+adapterName);
            }
        }
        return newTasks;
    }
}

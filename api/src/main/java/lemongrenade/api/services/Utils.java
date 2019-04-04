package lemongrenade.api.services;

import lemongrenade.api.services.Exceptions.HttpException;
import lemongrenade.core.SubmitToRabbitMQ;
import lemongrenade.core.coordinator.AdapterManager;
import lemongrenade.core.coordinator.JobManager;
import lemongrenade.core.database.lemongraph.LemonGraph;
import lemongrenade.core.database.mongo.MongoDBStore;
import lemongrenade.core.models.LGJob;
import lemongrenade.core.models.LGPayload;
import lemongrenade.core.util.JSONUtils;
import lemongrenade.core.util.LGProperties;
import lemongrenade.core.util.RequestResult;
import lemongrenade.core.util.Requests;
import org.bson.Document;
import org.eclipse.jetty.http.HttpStatus;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.*;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Utils {
    private static final Logger log = LoggerFactory.getLogger(Utils.class);
    private static final int MAX_THREADS = LGProperties.getInteger("api.threadpool", 25);
    private static final double BILLION = 1000000000.0;
    public static final String graphStoreStr = LGProperties.get("coordinator.graphstore", "lemongraph");
    public static String graph_url = LGProperties.get("lemongraph_url");

    //Common HTTP Status Codes
    public static final int OK = 200;
    public static final int CREATED = 201;
    public static final int ACCEPTED = 202;
    public static final int NO_CONTENT = 204;
    public static final int MULTI_STATUS = 207;
    public static final int BAD_REQUEST = 400;
    public static final int UNAUTHORIZED = 401;
    public static final int FORBIDDEN = 403;
    public static final int NOT_FOUND = 404;
    public static final int REQUEST_TOO_LARGE = 413;
    public static final int INTERNAL_SERVER_ERROR = 500;

    public static final int SECOND = 1000;

    //Open Connections
    private static JobManager JOB_MANAGER = new JobManager();
    private static ExecutorService EXECUTOR = Executors.newFixedThreadPool(MAX_THREADS);
    private static MongoDBStore MONGO_DB_STORE = new MongoDBStore();
    private static LemonGraph LEMONGRAPH = new LemonGraph();
    private static SubmitToRabbitMQ SUBMIT_TO_RABBIT =  new SubmitToRabbitMQ();
    private static AdapterManager ADAPTER_MANAGER = new AdapterManager();

    public static AdapterManager getAdapterManager() {
        return ADAPTER_MANAGER;
    }

    public static SubmitToRabbitMQ getSubmitToRabbit() {
        return SUBMIT_TO_RABBIT;
    }

    public static MongoDBStore getMongoDBStore() {
        return MONGO_DB_STORE;
    }

    public static LemonGraph getLemongraph() {
        return LEMONGRAPH;
    }

    public static JobManager getJobManager() {
        return JOB_MANAGER;
    }

    public static ExecutorService getExecutor() {
        return EXECUTOR;
    }

    //closes all opened connections and thread pools used by Utils
    public static void close() {
        try {
            ADAPTER_MANAGER.close();
            LEMONGRAPH.close();
            JOB_MANAGER.close();
            MONGO_DB_STORE.close();
            SUBMIT_TO_RABBIT.close();
            EXECUTOR.shutdownNow();
        }
        catch(Exception e) {
            log.error("Error closing connections.");
            e.printStackTrace();
        }
    }

    static String transformJob(JSONObject jobJSON) {
        String jobID = jobJSON.getString("graph");
        LGJob lgJob = JOB_MANAGER.getJob(jobJSON.getString("graph"));
        jobJSON.put("errors", Job.getErrorsHelper(lgJob));
        jobJSON.put("status", lgJob.getStatusString(lgJob.getStatus()));
        jobJSON.put("reason",lgJob.getReason());
        return jobID;
    }

    static JSONObject getStatusObject(HttpServletRequest request, String jobId) throws Exception {
        LGJob lgJob = JOB_MANAGER.getJob(jobId);
        String status = lgJob != null ? lgJob.getStatusString(lgJob.getStatus()) : null;
        JSONObject jobJSON;

        if (status == null) {
            jobJSON = new JSONObject();
            return jobJSON
                    .put("status", "404")
                    .put("graph", jobId)
                    ;
        }
        Response graphResponse = lemongraphProxy(request, "GET", "graph/" + jobId + "/status", "");
        String body = graphResponse.readEntity(String.class);
        jobJSON = new JSONObject(body);

        return jobJSON
                .put("status", status)
                .put("reason", lgJob.getReason())
                .put("errors", Job.getErrorsHelper(lgJob))
                ;
    }

    //for a jobId - update the data
    private static Response updateJobData(HttpServletRequest request, String jobId, JSONObject input, JSONObject params) { //JSON format items that overwrite will be merged
        //Update job data
        JSONObject retObject = updateLemongraphJobData(request, jobId, input, params);
        Response res = buildResponse(Utils.MULTI_STATUS, retObject);
        return res;
    }

    //for a jobId - update the data
    private static Response updateJobData(HttpServletRequest request, String jobId, JSONObject input) { //JSON format items that overwrite will be merged
        //Update job data
        JSONObject retObject = updateLemongraphJobData(request, jobId, input);
        Response res = buildResponse(Utils.MULTI_STATUS, retObject);
        return res;
    }

    private static JSONObject updateLemongraphJobData(HttpServletRequest request, String jobId, JSONObject input) {
        return updateLemongraphJobData(request, jobId, input, Utils.getRequestParameters(request));
    }

    private static JSONObject updateLemongraphJobData(HttpServletRequest request, String jobId, JSONObject input, JSONObject params) {
        params.put("ids", new JSONArray().put(jobId));
        try {Utils.addChildIDs(params);}
        catch(Exception e) {log.error("Error adding child IDs:"+e.getMessage());}
        Set<String> IDs = Utils.toSet(params.getJSONArray("ids"));

        JSONObject retObject = new JSONObject();
        Iterator iterator = IDs.iterator();
        while(iterator.hasNext()) {
            String currentID = iterator.next().toString();
            String extraPath = "graph/" + currentID + "/meta";
            Response graphResponse = lemongraphProxy(request, "PUT", extraPath, input.toString());
            String body = graphResponse.readEntity(String.class);
            JSONObject data;
            try {
                data = new JSONObject(body);
            }
            catch(Exception e) {
                data = new JSONObject();
            }
            Integer status = graphResponse.getStatus();
            String errors = null;
            if(data.has("message")) {
                errors = data.get("message").toString();
            }
            JSONObject standard = Utils.getStandardReturn(status, data, errors);
            retObject.put(currentID, standard);
        }

        return retObject;
    }

    //for a jobId - update the data
    private static Future<Response> updateJobDataFuture(HttpServletRequest request, String jobId, JSONObject data) { //JSON format items that overwrite will be merged
        String extraPath = "graph/" + jobId + "/meta";
        return getFutureLemongraphRequest(request, "PUT", extraPath, data.toString());
    }

    //for a jobId - update the meta object as provided; return a Future
    private static Future<Response> updateMetaFuture(HttpServletRequest request, String jobId, JSONObject meta) { //JSON format items that overwrite will be merged
        return updateJobDataFuture(request, jobId, meta);
    }

    //for a jobId - update the meta object as provided
    public static Response updateMeta(HttpServletRequest request, String jobId, JSONObject meta, JSONObject params) { //JSON format items that overwrite will be merged
        Response res = updateJobData(request, jobId, meta, params);
        return res;
    }

    //for a jobId - update the meta object as provided
    public static Response updateMeta(HttpServletRequest request, String jobId, JSONObject meta) { //JSON format items that overwrite will be merged
        Response res = updateJobData(request, jobId, meta);
        return res;
    }

    //returns a Response for updating multiple job meta entries
    public static Response updateMetas(HttpServletRequest request, JSONObject meta, JSONObject params) { //JSON format items that overwrite will be merged
        int end_status = MULTI_STATUS;
        //Collect child IDs too
        try {Utils.addChildIDs(params);}
        catch(Exception e) {log.error("Error fetching child IDs. Error:"+e.getMessage());}

        Set<String> IDs = Utils.toSet(params.getJSONArray("ids"));
        Iterator idIterator = IDs.iterator();
        HashMap<String, Future> futureJobs = new HashMap();
        while(idIterator.hasNext()) { //gather futures for updateMeta requests
            String id = idIterator.next().toString();
            Future res = updateMetaFuture(request, id, meta);
            futureJobs.put(id, res);
        }

        JSONObject responseJSON = new JSONObject();
        idIterator = futureJobs.keySet().iterator();
        while(idIterator.hasNext()) {//Return total response for future requests
            String id = idIterator.next().toString();
            try {
                Future<Response> future = futureJobs.get(id);
                Response res = future.get();
                Integer status = res.getStatus();
                String message = null;
                String body = res.readEntity(String.class);
                try {
                    JSONObject jBody = new JSONObject(body);
                    message = jBody.getString("message");
                }
                catch(Exception e) {
                    message = body;
                }

                JSONObject standard = Utils.getStandardReturn(status, null, message);
                responseJSON.put(id, standard);
            }
            catch(Exception e) {
                JSONObject standard = Utils.getStandardReturn(INTERNAL_SERVER_ERROR, null, e.getMessage());
                responseJSON.put(id, standard);
            }
        }
        return buildResponse(end_status, responseJSON);
    }

    public static Response buildResponse(HttpException e) {
        return buildResponse(e.getStatus(), e.getMessage());
    }

    public static Response buildResponse(Exception e) {
        return buildResponse(INTERNAL_SERVER_ERROR, e.getMessage());
    }

    public static Response buildResponse(int status, Object body) {
        return buildResponse(status, body.toString());
    }

    public static Response buildResponse(int status, String body) {
        return Response.status(status).entity(body).build();
    }

    //Gets params from the request and JSONObject body sent in and returns combined JSONObject of them. Errors if body isn't a JSONObject.
    static public JSONObject getRequestParameters(HttpServletRequest request, String body) throws Exception {
        JSONObject params = getRequestParameters(request);
        JSONObject bodyParams;
        if(body != null && body.length() > 0) {
            try {
                bodyParams = new JSONObject(body);
                Iterator keyIterator = bodyParams.keys();
                while(keyIterator.hasNext()) {
                    String key = keyIterator.next().toString();
                    String value = bodyParams.get(key).toString();
                    try {
                        JSONArray valueArray = new JSONArray(value);//value is an array
                        if(params.has(key)) {
                            JSONArray oldArray = params.getJSONArray(key);
                            valueArray = mergeArrays(oldArray, valueArray);
                        }
                        params.put(key, valueArray);
                    }
                    catch(Exception e) { //value is not an array
                        if (params.has(key)) {
                            JSONArray oldValue = params.getJSONArray(key);
                            oldValue.put(value);
                            params.put(key, oldValue);
                        }
                        else {
                            params.put(key, new JSONArray().put(value));
                        }
                    }
                }
            }
            catch(Exception e) {
                e.printStackTrace();
                String msg = "Invalid body. JSONObject expected. Error: "+e.getMessage();
                throw new Exception(msg);
            }
        }
        return params;
    }

    public static HashSet<String> getHashSet(JSONArray input) {//convert JSONArray to Set
        Iterator inputIterator = input.iterator();
        HashSet set = new HashSet();
        while(inputIterator.hasNext()) {
            String item = inputIterator.next().toString();
            set.add(item);
        }
        return set;
    }

    static JSONArray mergeArrays(JSONArray array1, JSONArray array2) {
        Iterator arrayIterator = array2.iterator();
        while(arrayIterator.hasNext()) {
            String item = arrayIterator.next().toString();
            array1.put(item);
        }
        array1 = removeDuplicates(array1);
        return array1;
    }//Returns the merged JSONArray of 2 arrays with no duplicates

    static JSONArray removeDuplicates(JSONArray input) {//Returns a JSONArray with any duplicates removed
        HashSet<String> set = getHashSet(input);
        JSONArray newArray = new JSONArray();
        Iterator setIterator = set.iterator();

        while(setIterator.hasNext()) {
            String item = setIterator.next().toString();
            newArray.put(item);
        }

        return newArray;
    }

    //Returns a String that isn't URL encoded.
    static String recursiveUrlDecode(String input) {
        String output = URLDecoder.decode(input);
        while(!output.equals(input)) {
            input = String.copyValueOf(output.toCharArray());
            output = URLDecoder.decode(output);
        }
        return input;
    }

    //Returns a once-encoded String. Doesn't double-encode already-encoded strings.
    public static String encodeOnce(String input) {
        input = recursiveUrlDecode(input);
        input = URLEncoder.encode(input);
        return input;
    }

    //returns true if String input is formatted as a JSONObject
    static boolean isJSONObject(String input) {
        try {
            new JSONObject(input);
            return true;//input is JSONObject
        }
        catch(Exception e) {
            return false;//input isn't JSONObject
        }
    }

    //returns a Resposne of JSONObject jobs
    public static Response fetchJobsByIds(HttpServletRequest request, JSONObject params) { //body should be {ids:[],...}
        int retStatus = Utils.OK;
        try {
            addChildIDs(params);
            if(params.has("ids") && params.getJSONArray("ids").length() > 0) {
                retStatus = Utils.MULTI_STATUS;
            }
            JSONObject jobs = getJobsByIds(request, params);
            return buildResponse(retStatus, jobs);
        } catch (HttpException e) {
            return buildResponse(e);
        } catch(Exception e) {
            e.printStackTrace();
            return buildResponse(Utils.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    //takes a params JSONObject and adds child IDs, if any are present
    public static void addChildIDs(JSONObject params) throws Exception {
        boolean children = true;
        if(params.has("children")) {
            children = params.getJSONArray("children").getBoolean(0);
        }

        if(children && params.has("ids") && params.getJSONArray("ids").length() > 0) {
            JSONArray ids = params.getJSONArray("ids");
            Set<String> childIDs = getChildIDs(ids);
            childIDs.addAll(Utils.toSet(ids));
            params.put("ids", Utils.toJson(childIDs));
        }
    }

    //returns a JSONObject of jobs
    public static JSONObject getJobsByIds(HttpServletRequest request, JSONObject params) throws Exception { //body should be {ids:[],...}
        String user = null;
        addChildIDs(params);
        if(params.has("user")) {
            JSONArray temp = params.getJSONArray("user");
            user = encodeOnce(temp.getString(0));
        }

        JSONObject graphJobs;
        JSONArray ids = new JSONArray();
        Long startTime = Utils.startTime("Fetching lemongraph jobs.");
        try {
            if (params.has("ids")) {
                ids = params.getJSONArray("ids");
                graphJobs = getLemongraphJobs(request, ids, user);
            } else {
                if(params.has("do")) {
                    HashSet doSet = getHashSet(params.getJSONArray("do"));
                    if(doSet.contains("graph")) {
                        String msg = "do=graph requested for the entire graph. Request specific job ids or remove do=graph.";
                        throw new HttpException(msg, Utils.REQUEST_TOO_LARGE);
                    }
                }
                graphJobs = getLemongraphJob(request, "", user);
            }
        }
        catch(HttpException e) {
            throw e;
        }
        catch(Exception e) {
            e.printStackTrace();
            String msg = "Failed to fetch Lemongraph jobs. Error:"+e.getMessage();
            throw new HttpException(msg, Utils.INTERNAL_SERVER_ERROR);
        }
        Utils.duration(startTime, "Done fetching lemongraph jobs.");

        JSONObject mongoJobs = new JSONObject();
        startTime = Utils.startTime("Fetching mongo jobs.");
        if(user != null && !user.equals("")) {
            ids = new JSONArray();
            Iterator<String> idIterator = graphJobs.keys();
            while(idIterator.hasNext()) {
                String id = idIterator.next();
                ids.put(id);
            }
        }

        try {
            mongoJobs = getMongoJobs(ids);
        } catch (Exception e) {
            e.printStackTrace();
            String msg = "Failed to fetch standard job for job_id:" + ". Exception:" + e.getMessage();
            throw new HttpException(msg, Utils.INTERNAL_SERVER_ERROR);
        }
        Utils.duration(startTime, "Done fetching mongo jobs.");
        startTime = Utils.startTime("Creating standard jobs.");
        JSONObject allJobs = createStandardJobs(request, graphJobs, mongoJobs);
        Utils.duration(startTime, "Done creating standard jobs.");
        return allJobs;
    }

    //Place a default job into LEMONGRENADE. Returns the job ID.
    public static String seedJob() throws Exception {
        JSONObject defaultJob = new JSONObject()
                .put("config", "config")
                .put("ID", "ID")
                .put("meta", "meta")
                .put("reason", "reason")
                .put("graph", "graph");
        String port = LGProperties.get("api.port");
        String url =  "http://localhost:" + port + "/api/job";
        RequestResult result = Requests.post(url, defaultJob.toString(), new JSONObject());
        String body = result.response_msg;
        JSONObject jBody = new JSONObject(body);
        String jobId = jBody.getString("job_id");

        boolean exists = false;
        int retries = 3;
        while(exists == false && retries > 0) {
            JSONObject graph = LemonGraph.getGraph(jobId);
            if (graph.has("created")) {
                exists = true;
                break;
            }
            retries--;
            Thread.sleep(Utils.SECOND / 2);
        }
        if(!exists) {
            log.error("Couldn't fetch seeded job.");
        }

        return jobId;
    }

    LGJob getMongoJob(String jobId) throws Exception {
        LGJob daoJob = JOB_MANAGER.getJob(jobId);//reason, endtime, approvedadapters, starttime, error_count, task_count, active_task_count, job_config, expire_date, job_id, graph_activity, create_date, status
        if (null == daoJob) {
            throw new Exception("Unable to fetch job:"+jobId+" from MongoDB.");
        }
        return daoJob;
    }//get LG job data from Mongo

    HashMap<String, LGJob> getMongoJobsDAO(JSONArray jobIds) throws Exception {
        HashMap<String, LGJob> daoJobs = JOB_MANAGER.getJobs(jobIds);//reason, endtime, approvedadapters, starttime, error_count, task_count, active_task_count, job_config, expire_date, job_id, graph_activity, create_date, status
        if (null == daoJobs) {
            throw new Exception("Unable to fetch jobs from MongoDB. jobIds:"+jobIds);
        }
        return daoJobs;
    }//get LG job data from Mongo

    public static JSONObject getMongoJobs(JSONArray jobIds) throws Exception {
        ArrayList<Document> documents = new ArrayList<Document>();
        if(jobIds.length() == 0) {
            documents = getMongoDBStore().getJobs();
        }
        else {
            documents = getMongoDBStore().getJobs(jobIds);
        }
        JSONObject jobs = MongoDBStore.toJSON(documents);
        return jobs;
    }//get LG job data from Mongo

    //Creates a thread for each LemonGraph job to fetch
    public static JSONObject getLemongraphJobs(HttpServletRequest request, JSONArray jobIds, String user) throws Exception {
        HashMap<String, Future> futureJobs = new HashMap<>();
        Iterator idIterator = jobIds.iterator();
        //create all of the Future jobs
        while(idIterator.hasNext()) {
            String jobId = idIterator.next().toString();
            Future<JSONObject> job = getLemongraphJobFuture(request, jobId, user);
            futureJobs.put(jobId, job);
        }

        JSONObject jobs = new JSONObject();
        Iterator jobsIterator = futureJobs.keySet().iterator();
        //get all of the jobs from the Future objects
        while(jobsIterator.hasNext()) {
            String jobId = jobsIterator.next().toString();
            Future<JSONObject> futureJob = futureJobs.get(jobId);
            JSONObject job = futureJob.get();
            jobs.put(jobId, job.getJSONObject(jobId));
        }
        return jobs;
    }

    //returns a Future to a Lemongraph job
    static Future<JSONObject> getLemongraphJobFuture(HttpServletRequest request, String jobId, String user) throws Exception {
        class LemongraphJobRequest implements Callable<JSONObject> {
            String jobId;
            HttpServletRequest request;
            String user;

            LemongraphJobRequest(HttpServletRequest request, String jobId, String user) {
                this.jobId = jobId;
                this.request = request;
                this.user = user;
            }

            @Override
            public JSONObject call() throws Exception {
                return getLemongraphJob(request, jobId, user);
            }
        }

        return getExecutor().submit(new LemongraphJobRequest(request, jobId, user));
    }

    public static JSONObject getLemongraphJob(HttpServletRequest request, String jobId, String user) throws HttpException {
        String extra = "graph/"+jobId;
        if(user != null) {
            extra+="?user="+user;
        }

        Response graphResponse = lemongraphProxy(request, "GET", extra, "");
        int status = graphResponse.getStatus();
        JSONObject graphJobs =  new JSONObject();
        if(status != Utils.OK) {
            String body = graphResponse.readEntity(String.class);
            String errors = "";
            try {
                body = body.replace("/n", "");
                JSONObject jBody = new JSONObject(body);
                String message = jBody.getString("message");
                errors = message;
            }
            catch(Exception e) {
                errors = body;
            }
            JSONObject standard = Utils.getStandardReturn(status, null, errors);
            graphJobs.put(jobId, standard);
            return graphJobs;
        }

        String body = graphResponse.readEntity(String.class);
        JSONArray rawJobs;
        try {
            rawJobs = new JSONArray(body);
        }
        catch(Exception e) {
            JSONObject rawJob = new JSONObject(body);
            rawJobs = new JSONArray().put(rawJob);
        }

        Iterator jobsIterator = rawJobs.iterator();

        while(jobsIterator.hasNext()) {
            JSONObject job = (JSONObject) jobsIterator.next();
            jobId = job.getString("graph");
            JSONObject standard = Utils.getStandardReturn(status, job, "");
            graphJobs.put(jobId, standard);
        }
        return graphJobs;
    }

    static Future<Response> getFutureLemongraphRequest(HttpServletRequest request, String method, String extraPath, String body) {
        class LemongraphRequest implements Callable<Response> {
            HttpServletRequest request;
            String method;
            String extraPath;
            String body;

            LemongraphRequest(HttpServletRequest request, String method, String extraPath, String body) {
                this.request = request;
                this.method = method;
                this.extraPath = extraPath;
                this.body = body;
            }

            @Override
            public Response call() throws Exception {
                return lemongraphProxy(request, method, extraPath, body);
            }
        }

        return getExecutor().submit(new LemongraphRequest(request, method, extraPath, body));
    }

    //gets a standardJob for a single jobId
    public static JSONObject getStandardJob(HttpServletRequest request, String jobId) throws Exception {
        JSONObject lgJobs = getLemongraphJob(request, jobId, null);
        ArrayList<Document> documents = getMongoDBStore().getJob(jobId);
        JSONObject mongoJobs = MongoDBStore.toJSON(documents);
        return createStandardJobs(request, lgJobs, mongoJobs);
    }

    //Creates a JSONObject of standard jobs from lemongraph jobs and mongo jobs
    public static JSONObject createStandardJobs(HttpServletRequest request, JSONObject graphJobs, JSONObject mongoJobs) throws Exception {
        JSONObject jobs = new JSONObject();
        JSONObject params = getRequestParameters(request);
        boolean hasStatus = false;
        boolean hasReason = false;
        String reason_in = null;
        String status_in = null;

        if (hasStatus = params.has("status")) {
            status_in = ((JSONArray) params.get("status")).get(0).toString().toLowerCase();
            if (status_in.isEmpty()) {
                throw new Exception("An empty string was passed in with the \'status\' filter.");
            }
        }

        if (hasReason = params.has("reason")) {
            reason_in = ((JSONArray) params.get("reason")).get(0).toString().toLowerCase();
            if (reason_in.isEmpty()) {
                throw new Exception("An empty string was passed in with the \'reason\' filter.");
            }
        }

        Iterator graphIterator = graphJobs.keySet().iterator();

        while(graphIterator.hasNext()) {
            String jobId = graphIterator.next().toString();
            JSONObject graphJob = graphJobs.getJSONObject(jobId);
            Boolean success = graphJob.getBoolean("success");
            if(success == false) {
                JSONArray lgErrors = graphJob.getJSONArray("errors");
                Iterator errorIterator = lgErrors.iterator();
                JSONArray errors = new JSONArray();
                while(errorIterator.hasNext()) {
                    String error = "LEMONGRAPH:"+errorIterator.next().toString();
                    errors.put(error);
                }
                graphJob.put("errors", errors);
                jobs.put(jobId, graphJob);
                continue;
            }
            Integer httpStatus = graphJob.getInt("status_code");
            try {
                if (!mongoJobs.has(jobId) || !graphJobs.has(jobId)) {
                    if(!mongoJobs.has(jobId)) {
                        JSONObject standard = Utils.getStandardReturn(Utils.NOT_FOUND, null, "Job not found in MongoDB");
                        jobs.put(jobId, standard);
                    }
                    continue;
                }
                JSONObject mongoJob = mongoJobs.getJSONObject(jobId);

                /*
                If REASON and STATUS match passed in reason and status parameters, continue to the next job
                 */
                int jobStatus = mongoJob.getInt("status");
                String status = LGJob.getStatusString(jobStatus);
                String reason = "";
                if(mongoJob.has("reason")) {
                    reason = mongoJob.getString("reason");
                }

                /*
                Check to see if the mongoJob has the lg_type field, if not, enter it here assuming the value "graph"
                 */
                if (!mongoJob.has("lg_type")) {
                    MongoDBStore.updateJob(jobId, "lg_type", "graph");
                }

                //check to see if status or reason filters have been passed in
                if (hasStatus || hasReason) {

                    boolean shouldContinue = false;
                    hasStatus = hasStatus && !status_in.isEmpty();
                    hasReason = hasReason && !reason_in.isEmpty();

                    /* if status and reason match with the filters that were passed in, then we will NOT add this job to the list; skip to the next job */
                    if (hasStatus && hasReason) {
                        if (reason.toLowerCase().equals(reason_in) && status.toLowerCase().equals(status_in)) {
                            continue;
                        }
                    }

                    else {
                        //If we have a status, return all jobs that match to this status
                        if (hasStatus) {

                            if (status.toLowerCase().equals(status_in)) {
                            /* flow down for further processing */
                            } else {
                                shouldContinue = true;
                            }
                        } else if (hasReason) {

                            if (reason.toLowerCase().equals(reason_in)) {
                                shouldContinue = false;
                            } else {
                                shouldContinue = true;
                            }
                        }
                    }

                    if (shouldContinue)
                    {
                        continue;
                    }

                }

                JSONArray errors = new JSONArray();

                JSONObject taskMap = new JSONObject();
                if (mongoJob.has("taskMap")) {
                    taskMap = mongoJob.getJSONObject("taskMap");
                }
                int taskCount = taskMap.length();
                if (mongoJob.has("jobErrors")) {
                    errors = mongoJob.getJSONArray("jobErrors");
                }

                int active = 0;
                if (jobStatus == LGJob.STATUS_PROCESSING) { //only processing jobs have non-zero active_task_count
                    ArrayList<Document> docs = getMongoDBStore().getTasksFromJob(jobId);
                    JSONObject tasks = MongoDBStore.toJSON(docs);
                    active = LGJob.getActiveTaskCount(tasks);
                }

                JSONObject lgData = graphJob.getJSONObject("data");
                int graphActivity = 0;
                if(mongoJob.has("graphActivity")) {
                    graphActivity = mongoJob.getInt("graphActivity");
                }

                //combine job data for standardized return
                JSONObject job = new JSONObject()
                        .put("id", jobId)
                        .put("activity", graphActivity) // maxID and graph_activity are the same thing
                        .put("status", status)
                        .put("reason", reason)
                        .put("error_count", errors.length())
                        .put("task_count", taskCount)
                        .put("active_task_count", active)
                        .put("created_date", lgData.getString("created"))
                        .put("meta", lgData.getJSONObject("meta"))//fetch meta data
                        .put("size", lgData.get("size").toString()) //fetch size from LemonGraph
                        ;
                if(lgData.has("nodes_count")) {
                    job.put("nodes_count", lgData.get("nodes_count"));
                }
                if(lgData.has("edges_count")) {
                    job.put("edges_count", lgData.get("edges_count"));
                }

                //Check which 'do' flags were passed in

                if (params.has("do")) { //if 'do' parameter is present
                    JSONArray doValues = params.getJSONArray("do");
                    HashSet set = getHashSet(doValues);
                    if (set.contains("errors")) {
                        job.put("errors", errors);
                    }
                    if (set.contains("graph")) {
                        if (lgData.has("nodes"))
                            job.put("nodes", lgData.getJSONArray("nodes"));
                        if (lgData.has("edges"))
                            job.put("edges", lgData.getJSONArray("edges"));
                    }
                }

                JSONObject standard = Utils.getStandardReturn(httpStatus, job, "");
                jobs.put(jobId, standard);
            }
            catch(Exception e) {
                log.error("Failed to handle job:"+ jobId+". Skipping");
                e.printStackTrace();
                continue;
            }
        }

        return jobs;
    }

    //Get a JSONObject of request paramaters for the current request
    public static JSONObject getRequestParameters(HttpServletRequest request) {
        JSONObject parameters = new JSONObject();
        Enumeration names = request.getParameterNames();
        while(names.hasMoreElements()) {
            String name = names.nextElement().toString();
            String[] values = request.getParameterValues(name);
            JSONArray JSONValues =  new JSONArray();
            for(int i = 0; i < values.length; i++) {
                String value = values[i];
                JSONValues.put(value);
            }
            parameters.put(name, JSONValues);
        }
        return parameters;
    }

    public static Response lemongraphProxy(HttpServletRequest request, String extraPath, String body) {
        String method = request.getMethod();
        return lemongraphProxy(request, method, extraPath, body);
    }

    public static Response lemongraphProxy(HttpServletRequest request, String method, String extraPath, String body) {
        String url = graph_url+extraPath;
        System.setProperty("sun.net.http.allowRestrictedHeaders", "true"); //Allows user option to send any header
        try {
            Client client = ClientBuilder.newClient();
            String contentType = request.getContentType();
            WebTarget target = client.target(url);
            //Add original request headers
            Enumeration names = request.getHeaderNames();
            String name;
            MultivaluedHashMap<String, Object> headers = new MultivaluedHashMap<String, Object>();
            while(names.hasMoreElements()) {
                name = names.nextElement().toString();
                Object value = request.getHeader(name);
                List temp = new ArrayList<Object>();
                temp.add(value);
                headers.put(name, temp);
            }

            //Add Query Params from original to new request
            Form form = new Form();
            names = request.getParameterNames();
            while(names.hasMoreElements()) {
                name = names.nextElement().toString();
                Object value = request.getParameter(name);
                target = target.queryParam(name, value); //add params for GET requests
                form.param(name, value.toString()); //add params for POST and PUT requests
            }

            Invocation.Builder builder = target.request().headers(headers);
            Response res;

            if(method.equals("GET"))
                res = builder.get();
            else if (method.equals("POST")) {
                res = builder.post(Entity.entity(body, contentType));
            }
            else if(method.equals("PUT")) {
                res = builder.put(Entity.entity(body, contentType));
            }
            else {
                String msg = "LEMONGRENADE failed to query LEMONGRAPH. Bad method:"+method;
                return Response.status(Utils.INTERNAL_SERVER_ERROR).entity(msg).build();
            }
            return res;
        } catch (Exception e) {
            e.printStackTrace();
            String msg = "LEMONGRENADE failed to query LEMONGRAPH. Error:"+e.getMessage();
            return Response.status(Utils.INTERNAL_SERVER_ERROR).entity(msg).build();
        }
    }

    //gets the current time as Long. Provide return to "duration" to get the time since 'start'
    public static Long startTime(String name) {
        log.info("Starting " + name);
        Long startTime = System.nanoTime();
        return startTime;
    }

    //Gets a set of every 'value' of all nodes with type of 'job'. Used to get children of bulk jobs.
    public static Set<String> getChildIDs(String job_id) throws Exception {
        String regex = "n(type~/graph/i)";
        String url = LGProperties.get("lemongraph_url")+"graph/"+job_id+"?q="+regex+"&start=1";
        RequestResult result = Requests.get(url);
        HashSet<String> IDs = new HashSet();
        try {
            String msg = result.response_msg;
            JSONArray data = new JSONArray();
            try {
                data = new JSONArray(msg);
            }
            catch(JSONException e) {
                JSONObject ret = new JSONObject(msg);
                if(ret.has("code") && ret.getInt("code") == Utils.NOT_FOUND) {
                    return IDs;//graph wasn't found, so no IDs available
                }
                log.error("Error getting child IDs from LEMONGRAPH. message:"+result.response_msg);
                throw e;
            }
            Iterator iterator = data.iterator();
            JSONArray type = (JSONArray) iterator.next(); //first item is an array containing the regex used
            if(type.length() == 1 && type.get(0).toString().equals(regex)) {
                while (iterator.hasNext()) {
                    JSONArray item = (JSONArray) iterator.next();
                    if(item.length() != 2) {
                        throw new Exception("Bad LEMONGRAPH array return length. item:"+item);
                    }
                    int int1 = item.getInt(0);
                    JSONArray nodes = item.getJSONArray(1);
                    Iterator nodeIterator = nodes.iterator();
                    while(nodeIterator.hasNext()) {
                        JSONObject node = (JSONObject) nodeIterator.next();
                        if(node.getString("type").equals("graph") && node.has("value")) {
                            String child_job_id = node.getString("value");
                            IDs.add(child_job_id);
                        }
                    }
                    log.debug("LEMONGRAPH query for child item:"+item.toString());
                }
            }
            else {
                throw new Exception("Invalid LEMONGRAPH response:"+type.toString());
            }
        }
        catch(Exception e) {
            log.error("Unable to parse bulk IDs from LEMONGRAPH.");
            log.error(e.getMessage());
            throw new Exception(e);
        }
        return IDs;
    }

    public static Set<String> getChildIDs(Iterable inputIDs) throws Exception {
        HashSet<String> retIDs = new HashSet();
        Iterator iterator = inputIDs.iterator();
        while(iterator.hasNext()) {
            String currentID = iterator.next().toString();
            Set<String> newIDs = getChildIDs(currentID);
            retIDs.addAll(newIDs);
        }
        return retIDs;
    }

    //Checks params to determine if delete was requested
    public static boolean deleteCheck(JSONObject params) {
        if(params.has("do")) {
            JSONArray doValues = params.getJSONArray("do");
            HashSet set = Utils.getHashSet(doValues);
            if (set.contains("delete")) {
                return true;
            }//end of delete check
        }//check if there is a 'do', because this may be a delete
        return false;
    }

    //Only run if deleteCheck=true
    public static Response doDelete(JSONObject params) throws Exception {
        Utils.addChildIDs(params);
        if(params.has("do")) {
            JSONArray doValues = params.getJSONArray("do");
            HashSet doSet = Utils.getHashSet(doValues);
            if (doSet.contains("delete")) {
                if(params.has("ids")) {
                    JSONObject responses =  new JSONObject();
                    HashMap<String, Future<JSONObject>> futureStandards = new HashMap<>();

                    Iterator idIterator = params.getJSONArray("ids").iterator();
                    while(idIterator.hasNext()) {
                        String id = (String) idIterator.next();
                        Future<JSONObject> futureStandard = getDeleteResponseFuture(id); //load future responses
                        futureStandards.put(id, futureStandard);
                    }

                    idIterator = futureStandards.keySet().iterator();
                    while(idIterator.hasNext()) {
                        String id = (String) idIterator.next();
                        Future<JSONObject> futureStandard = futureStandards.get(id);
                        responses.put(id, futureStandard.get());//get all future responses
                    }//end of idIterator loop

                    return Utils.buildResponse(Utils.MULTI_STATUS, responses); //ids were provided, 207
                } //end of params ids check
                else {
                    return Utils.buildResponse(Utils.REQUEST_TOO_LARGE, "Delete request requires IDs be specified.");
                }
            }//end of delete check
        }//check if there is a 'do', because this may be a delete
        throw new Exception("Invalid params for do=delete request.");
    }

    //returns a Future to a Lemongraph job
    static Future<JSONObject> getDeleteResponseFuture(String job_id) throws Exception {
        class DeleteRequest implements Callable<JSONObject> {
            String job_id;

            DeleteRequest(String job_id) {
                this.job_id = job_id;
            }

            @Override
            public JSONObject call() throws Exception {
                try {
                    JSONObject ret = deleteJob(job_id);
                    int status = ret.getInt("status");
                    String error = null;
                    if(ret.has("error"))
                        error = ret.get("error").toString();
                    JSONObject standard = Utils.getStandardReturn(status, null, error);
                    return standard;
                } catch (Exception e) {
                    JSONObject standard = Utils.getStandardReturn(Utils.INTERNAL_SERVER_ERROR, null, e.getMessage());
                    return standard;
                }
            }
        }

        return getExecutor().submit(new DeleteRequest(job_id));
    }

    public static JSONObject deleteJob(String jobId) {
        try {
            JSONObject ret = deleteHelper(jobId);
            int status = ret.getInt("status");
            ret.put("status", status);
            return ret;
        } catch (Exception e) {

        }

        JSONObject ret = new JSONObject();
        ret.put("job_id",jobId);
        ret.put("error", "Job with the id " + jobId + " delete FAILED for unknown reason.");
        ret.put("deleted",false);
        ret.put("status", Utils.INTERNAL_SERVER_ERROR);
        return ret;
    }

    public static JSONObject deleteHelper(String jobId) {
        HashSet<String> jobIDs = new HashSet();
        jobIDs.add(jobId);
        return deleteHelper(jobIDs, true);
    }

    public static JSONObject deleteHelper(JSONObject params) throws Exception {
        if(!params.has("ids")) {
            throw new Exception("No 'ids' field provided!");
        }
        boolean children = false;
        if(params.has("children")) {
            children = params.getJSONArray("children").getBoolean(0);
        }
        return deleteHelper(Utils.toSet(params.getJSONArray("ids")), children);
    }

    public static JSONObject deleteHelper(Set<String> jobIDs, boolean children) {
        if (children == true) {
            Set<String> allIDs = new HashSet();
            allIDs.addAll(jobIDs);
            Iterator iterator = jobIDs.iterator();
            while(iterator.hasNext()) {
                String currentID = iterator.next().toString();
                try {
                    Set<String> childIDs = Utils.getChildIDs(currentID);
                    allIDs.addAll(childIDs);
                }
                catch(Exception e) {
                    log.error("Error fetching child IDs for currentID:"+currentID+" Error:"+e.getMessage());
                }
            }
            return deleteHelper(allIDs);
        }
        return deleteHelper(jobIDs);
    }

    /** Used by single and bulk delete */
    public static JSONObject deleteHelper(Set<String> jobIDs) {
        // Build return object common parts
        Set<String> allIDs = new HashSet();
        allIDs.addAll(jobIDs);
        JSONObject ret = new JSONObject();
        ret.put("job_IDs", allIDs);
        JSONObject cancelRet = cancelHelper(jobIDs); //this function modifies jobIDs
        Set<String> toDelete = Utils.toSet(cancelRet.getJSONArray("cancelled_IDs")); //these jobs were set to STOPPED in Mongo
        Set<String> missing = Utils.toSet(cancelRet.getJSONArray("missing"));//these jobs are not present in MongoDB
        Set<String> failed = Utils.toSet(cancelRet.getJSONArray("failed"));//these jobs were not set to STOPPED in the time allocated
        Set<String> deleted = new HashSet();

        Iterator iterator = allIDs.iterator();
        while (iterator.hasNext()) {
            String currentID = iterator.next().toString();
            if(failed.contains(currentID)) { //skip deleting IDs that couldn't be put into a stopped state
                continue;
            }
            try {
                //Attempt to remove LEMONGRAPH entry, Mongo DB Values, and Mongo job and tasks
                JSONObject graphRet = LemonGraph.deleteGraph(currentID);
                int status = graphRet.getInt("status");
                if(status != 204) { //no content status expected for delete
                    ret.append("errors", "Invalid LEMONGRAPH delete status:"+status+". Expected 204.");
                    ret.append("errors", "LEMONGRPAH Response:"+graphRet.getString("message"));
                }
                else {
                    log.info("Deleted job:"+currentID+" from LEMONGRAPH.");
                }

                if(missing.contains(currentID)) { //Don't perform Mongo deletes for job not found in Mongo
                    continue;
                }
                JOB_MANAGER.deleteDBValues(currentID);
                getMongoDBStore().deleteJob(currentID); //delete the job from MongoDB
                log.info("Deleted job:" + currentID + " from MongoDB");
                getMongoDBStore().deleteTasksByJob(currentID);//delete the tasks from MongoDB
                log.info("Deleted tasks for job:" + currentID + " from MongoDB");
                deleted.add(currentID);
            } catch (Exception e) {
                failed.add(currentID);
                String msg = "An error occurred while deleting job:" + currentID + ". Error:"+e.getMessage();
                log.error(msg);
                ret.append("errors", msg);
                e.printStackTrace();
            }
        }

        ret.put("deleted_IDs", deleted);
        ret.put("missing", missing);
        ret.put("failed", failed);
        ret.put("deleted", false);
        if(ret.has("errors")) {
            ret.put("error", ret.getJSONArray("errors").toString());
        }
        if(deleted.size() == 0 && failed.size() == 0) {
            ret.put("status", Utils.NOT_FOUND);
        }
        else if(missing.size() == 0 && failed.size() == 0) {
        ret.put("status", Utils.OK);
        ret.put("deleted", true);
        }
        else if (failed.size() > 0) {
            ret.put("status", Utils.INTERNAL_SERVER_ERROR);
        }
        else {
            ret.put("status", Utils.MULTI_STATUS);
        }

        log.info("API Delete Job successfully deleted jobIDs:" + toDelete +"failed:"+failed+" missing:"+missing);
        return ret;
    }


    /**
     * Used by /job/ID/cancel and /jobs/cancel
     * @param jobId the id of the job to cancel
     */
    public static JSONObject cancelHelper(String jobId) {
        HashSet<String> jobIDs = new HashSet();
        jobIDs.add(jobId);
        return cancelHelper(jobIDs, true).put("job_id", jobId);
    }

    public static JSONObject cancelHelper(JSONObject params) throws Exception {
        if(!params.has("ids")) {
            throw new Exception("No 'ids' field provided!");
        }
        boolean children = false;
        if(params.has("children")) {
            children = params.getJSONArray("children").getBoolean(0);
        }
        return cancelHelper(Utils.toSet(params.getJSONArray("ids")), children);
    }

    public static JSONObject cancelHelper(Set<String> jobIDs, boolean children) {
        if (children == true) {
            Set<String> allIDs = new HashSet();
            allIDs.addAll(jobIDs);
            Iterator iterator = jobIDs.iterator();
            while(iterator.hasNext()) {
                String currentID = iterator.next().toString();
                try {
                    Set<String> childIDs = Utils.getChildIDs(currentID);
                    allIDs.addAll(childIDs);
                }
                catch(Exception e) {
                    log.error("Error fetching child IDs for currentID:"+currentID+" Error:"+e.getMessage());
                }
            }
            return cancelHelper(allIDs);
        }
        return cancelHelper(jobIDs);
    }

    /**
     * Used by /job/ID/cancel and /jobs/cancel
     * @param jobIDs the IDs of the job to cancel
     */
    public static JSONObject cancelHelper(Set<String> jobIDs) {
        JSONObject ret = new JSONObject()
                .put("errors", new JSONArray())
                .put("job_IDs", jobIDs.toString())
                ;
        HashSet<String> missing = new HashSet();
        HashSet<String> cancelled = new HashSet();
        try {
            JSONArray idCopy = Utils.toJson(jobIDs);
            Iterator idIterator = idCopy.iterator();

            //Iterator through all ID and send cancel if job needs to be stopped
            while(idIterator.hasNext()) {
                String currentID = idIterator.next().toString();
                LGJob lg = JOB_MANAGER.getJob(currentID);
                if (null == lg) {
                    ret.append("errors", "Couldn't fetch job:" + currentID + ".");
                    jobIDs.remove(currentID);//remove an ID for which there is no job existing
                    missing.add(currentID);
                    continue; //job no longer exists, no cancel needed
                }
                int status = lg.getStatus();
                if ((status == LGJob.STATUS_NEW) || (status == LGJob.STATUS_PROCESSING) || (status == LGJob.STATUS_QUEUED)) {
                    log.info("job:"+currentID+" status:"+lg.getStatusString(status)+" eligible for cancel.");
                    getSubmitToRabbit().sendCancel(currentID);
                }
                else {
                    log.info("job:"+currentID+" status:"+lg.getStatusString(status));
                    cancelled.add(currentID); //ID is already cancelled
                    jobIDs.remove(currentID);//remove an ID for which no cancel is needed
                    continue; //no cancel needed
                }
            }

            int retries = 3;
            while(retries > 0 && jobIDs.size() > 0) { //loop up to 3 retries until all IDs are cancelled
                idCopy = Utils.toJson(jobIDs);
                idIterator = idCopy.iterator();
                while (idIterator.hasNext()) {
                    String currentID = idIterator.next().toString();
                    LGJob lg = JOB_MANAGER.getJob(currentID);
                    int status = lg.getStatus();
                    log.info("job:" + currentID + " status:" + lg.getStatusString(status));
                    if ((status != LGJob.STATUS_STOPPED) && (status != LGJob.STATUS_FINISHED) && (status != LGJob.STATUS_FINISHED_WITH_ERRORS)) {}//end of status check
                    else { //job is in a cancelled state, remove it.
                        cancelled.add(currentID);
                        jobIDs.remove(currentID);
                    }
                    retries--;
                    Thread.sleep(Utils.SECOND/2);
                }//end of idIterator
            }

            boolean success = false;
            if(jobIDs.size() == 0) {success = true;}
            ret.put("cancelled", success);//true if no job IDs provided are still in a non-cancelled state
            ret.put("missing", missing); //jobs which 404d
            ret.put("failed", jobIDs); //jobs we failed to get into a cancelled state
            ret.put("cancelled_IDs", cancelled);//jobs which are now in a cancelled state

            if(jobIDs.size() == 0 && cancelled.size() == 0) { //no cancel was needed
                ret.put("status", Utils.ACCEPTED);
                ret.append("errors", "No cancellation needed.");
            }

            if(jobIDs.size() == 0 && cancelled.size() > 0) { //all IDs cancelled succesfully
                ret.put("status", Utils.OK);
            }

            if(jobIDs.size() > 0) {
                ret.append("errors", "Failed to cancel the following IDs:"+jobIDs.toString());
                ret.put("status", Utils.CREATED);
            }
        }
        catch (Exception e) {
            log.error("Error:" + e.getMessage());
            ret.append("errors", e.getMessage());
            ret.put("cancelled", false);
            ret.put("status", Utils.CREATED);
        }
        ret.put("error", ret.getJSONArray("errors").toString());
        return ret;
    }

    /**
     * Used by /jobs/reset and /job/reset
     *
     * @param reason Is a value supplied by the api user to say why it was reset. They can query for this value later
     * @param allowOverwrite  - allows a follow on reset to rewrite the REASON setting, otherwise, its kept the same.
     *                          default is false
     * @return JSONOBject - results
     */
    @Deprecated
    public static JSONObject resetHelper(String jobId, String reason, Boolean allowOverwrite) {
        LGJob lg = Utils.getJobManager().getJob(jobId);

        JSONObject jobInfo = new JSONObject();
        JSONObject ret     = new JSONObject();
        jobInfo.put("status",lg.getStatusString(lg.getStatus()));
        jobInfo.put("reason",lg.getReason());
        if (null == lg) {
            jobInfo.put("message", "job not found");
            jobInfo.put("status_code",400);
            ret.put(jobId, jobInfo);
            return ret;
        }
        int status = lg.getStatus();
        if (status == LGJob.STATUS_PROCESSING) {
            jobInfo.put("message", "Can not reset job, Job is " + lg.getStatusString(status));
            jobInfo.put("status_code", 409);
            ret.put(jobId, jobInfo);
            return ret;
        }

        if ((status == LGJob.STATUS_RESET) && (!allowOverwrite)) {
            jobInfo.put("message", "Can not reset job that is already in reset mode, unless overwrite is true");
            jobInfo.put("status_code", 409);
            ret.put(jobId, jobInfo);
            return ret;
        }

        try {
            Utils.getSubmitToRabbit().sendReset(jobId, reason);
        }
        catch (Exception e) {
            jobInfo.put("message", "Can not reset job :"+e.getMessage());
            jobInfo.put("status_code",Utils.CREATED);
            ret.put(jobId, jobInfo);
            return ret;
        }
        jobInfo.put("message", "success");
        jobInfo.put("status",lg.getStatusString(LGJob.STATUS_RESET));
        jobInfo.put("reason", reason);
        jobInfo.put("status_code",Utils.OK);
        ret.put(jobId, jobInfo);
        return ret;
    }

    public static JSONObject resetHelper(JSONArray jobIds, String reason, Boolean allowOverwrite) {
        ArrayList<Document> documents = getMongoDBStore().getJobs(jobIds);
        JSONObject mongoJobs = MongoDBStore.toJSON(documents);

        Iterator jobIterator = mongoJobs.keySet().iterator();
        JSONObject ret = new JSONObject();
        while(jobIterator.hasNext()) {
            String jobId = jobIterator.next().toString();
            JSONObject mongoJob = mongoJobs.getJSONObject(jobId);
            JSONObject data = new JSONObject();
            String error = null;

            int status = mongoJob.getInt("status");
            data.put("status", LGJob.getStatusString(status));
            data.put("reason", reason);

            if (status == LGJob.STATUS_PROCESSING) {
                error = "Can not reset job, Job is " + LGJob.getStatusString(status);
                ret.put(jobId, getStandardReturn(409, data, error));
                continue;
            }
            else if ((status == LGJob.STATUS_RESET) && (!allowOverwrite)) {
                error = "Can not reset job that is already in reset mode, unless overwrite is true";
                ret.put(jobId, getStandardReturn(409, data, error));
                continue;
            }

            try {
                Utils.getSubmitToRabbit().sendReset(jobId, reason);
            } catch (Exception e) {
                error = "Can not reset job :" + e.getMessage();
                ret.put(jobId, getStandardReturn(Utils.CREATED, data, error));
                continue;
            }
            data.put("status", LGJob.getStatusString(LGJob.STATUS_RESET));
            ret.put(jobId, getStandardReturn(Utils.OK, data, error));
            continue;
        }
        return ret;
    }

    public static void duration(Long startTime, String name) { //prints info about the duration since startTime
        Long endTime = System.nanoTime();
        Long nanoseconds = endTime-startTime;
        double seconds = nanoseconds/BILLION;
        log.info("Finished "+ name +" after "+seconds+" seconds.");
    }

    //Returns a default object of the standard return format for /rest endpoints
    public static JSONObject getStandardReturn() {
        JSONObject standard = new JSONObject()
                .put("success", false)
                .put("status_code", Utils.INTERNAL_SERVER_ERROR)
                .put("status_message", "Internal Server Error")
                .put("data", new JSONObject())
                .put("errors", new JSONArray())
                ;
        return standard;
    }

    //Returns a default object of the standard return format for /rest endpoints
    public static JSONObject getStandardReturn(int status_code, JSONObject data, String error) {
        JSONArray errors = new JSONArray();
        if(error != null && error.length() > 0) {
            errors.put(error);
        }
        return getStandardReturn(status_code, data, errors);
    }

    //Returns a default object of the standard return format for /rest endpoints
    public static JSONObject getStandardReturn(int status_code, JSONObject data, JSONArray errors) {
        if(data == null) {
            data = new JSONObject();
        }
        if(errors == null) {
            errors = new JSONArray();
        }

        JSONObject standard = new JSONObject()
                .put("success", httpSuccess(status_code))
                .put("status_code", status_code)
                .put("status_message", HttpStatus.getMessage(status_code))
                .put("data", data)
                .put("errors", errors)
                ;
        return standard;
    }

    static boolean httpSuccess(int status) {
        if(status >= 200 && status < 300)
            return true;
        return false;
    }

    public static JSONArray toJson(Set set) {
        return JSONUtils.toJson(set);
    }

    static Set<String> toSet(Iterable items) {
        Set<String> set = new HashSet();
        Iterator iterator = items.iterator();
        while(iterator.hasNext()) {
            String item = iterator.next().toString();
            set.add(item);
        }
        return set;
    }
}

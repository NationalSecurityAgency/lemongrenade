package lemongrenade.api.services;

import lemongrenade.core.database.lemongraph.LemonGraph;
import lemongrenade.core.database.mongo.MongoDBStore;
import lemongrenade.core.models.*;
import lemongrenade.core.util.LGConstants;
import lemongrenade.core.util.LGProperties;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.*;

/**
 * See /docs/coordinator-api.txt for documentation
 */
@Path("/api/")
public class Job {
    @Context
    HttpServletRequest request;
    @Context
    HttpServletResponse response;
    @Context
    ServletContext context;

    private static final Logger log = LoggerFactory.getLogger(lemongrenade.api.services.Job.class);

    public void close() {
        try {
            Utils.close();
        }
        catch(Exception e) {
            log.error("Error closing connections.");
        }
    }

    //alters a job and returns its job_id
    String transformJob(JSONObject job) {
        if(job.has("config")) {
            job.put("meta", job.get("config"));
            job.remove("config");
        }
        String job_id = job.getString("graph");
        job.remove("graph");
        job.put("errors", getErrors(job_id));
        job.put("status", getStatus(job_id));
        return job_id;
    }

    //alters every job in a graph
    JSONObject transformGraph(JSONArray graph) {
        log.info("Started transformGraph.");
        Iterator graphIterator = graph.iterator();
        JSONObject returnObject = new JSONObject();
        JSONArray jobIds = new JSONArray();

        //Gather all job ids and make a single request to MongoDB for all matching jobs
        while(graphIterator.hasNext()) {
            JSONObject job = (JSONObject) graphIterator.next();
            String job_id = job.getString("graph");
            jobIds.put(job_id);
        }
        log.info("Requesting " + jobIds.length() + " jobs from MongoDB.");
        Long startTime = Utils.startTime("dao.getByJobIds(jobIds)");
        HashMap<String, LGJob> jobs = Utils.getJobManager().getJobs(jobIds);
        Utils.duration(startTime, "dao.getByJobIds(jobIds)");

        //Iterate over every job in the graph, transform and add the errors and status
        graphIterator = graph.iterator();
        while(graphIterator.hasNext()) {
            JSONObject job = (JSONObject) graphIterator.next();
            LGJob lgJob = null;
            String job_id = job.getString("graph");
            if(jobs.containsKey(job_id)) {
                lgJob = jobs.get(job_id);
            }

            //Transform config->meta
            if(job.has("config")) {
                job.put("meta", job.get("config"));
                job.remove("config");
            }
            job.remove("graph");
            job.put("errors", getErrorsHelper(lgJob));
            job.put("status", getStatusHelper(lgJob));
            returnObject.put(job_id, job);
        }
        log.info("Finished transformGraph.");
        return returnObject;
    }

    @GET
    @Path("jobs/all")
    @Produces(MediaType.APPLICATION_JSON)
    //Proxies LEMONGRAPH /graph endpoint and replaces "graph" and "meta" items with "job_id" and "config" respectively
    public Response jobs() {
        log.info("Received request for api/jobs/all.");
        Response graphResponse = Utils.lemongraphProxy(request, "graph", "");;
        int status = graphResponse.getStatus();
        if(status != 200)
            return graphResponse;
        try {
            String body = graphResponse.readEntity(String.class);
            JSONArray graph = new JSONArray(body);
            JSONObject returnObject = transformGraph(graph);
            return Response.status(200).entity(returnObject.toString()).build();
        }
        catch(Exception e) { //When an error is returned jetty returns 500 and error, even if it's caught
            log.warn("Error processing /api/jobs/all");
            e.printStackTrace();
            return graphResponse;
        }
    }

    @GET
    @Path("jobs/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    //Proxies LEMONGRAPH /graph endpoint and replaces "graph" and "meta" items with "job_id" and "config" respectively
    public Response job(@PathParam("id") String id) {
        Response graphResponse = Utils.lemongraphProxy(request, "graph/" + id, "");
        int status = graphResponse.getStatus();
        if(status != 200)
            return graphResponse;
        try {
            String body = graphResponse.readEntity(String.class);
            JSONObject job = new JSONObject(body);
            JSONObject jobs = new JSONObject();
            String job_id = transformJob(job);
            jobs.put(job_id, job);
            return Response.status(200).entity(jobs.toString()).build();
        }
        catch(Exception e) { //When an error is returned jetty returns 500 and error, even if it's caught
            log.warn("Error processing /api/graph");
            e.printStackTrace();
            return graphResponse;
        }
    }

    @POST
    @Path("jobs")
    @Produces(MediaType.APPLICATION_JSON)
    //Proxies LEMONGRAPH /graph endpoint and replaces "graph" and "meta" items with "job_id" and "config" respectively
    public Response jobs(String input) {
        JSONObject returnObject = new JSONObject();
        try {
            JSONArray jobIDs = new JSONArray(input);
            Iterator idIterator = jobIDs.iterator();
            while (idIterator.hasNext()) {
                String id = idIterator.next().toString();
                Response graphResponse = Utils.lemongraphProxy(request, "GET", "graph/" + id, "");
                int status = graphResponse.getStatus();
                if (status != 200)
                    return graphResponse;
                try {
                    String body = graphResponse.readEntity(String.class);
                    JSONObject job = new JSONObject(body);
                    String job_id = transformJob(job);
                    returnObject.put(job_id, job);
                } catch (Exception e) { //When an error is returned jetty returns 500 and error, even if it's caught
                    log.warn("Error processing /api/graph");
                    e.printStackTrace();
                    return graphResponse;
                }
            }//end while loop
        }
        catch(Exception e) {
            log.info("Failed processing for input:" + input);
            e.printStackTrace();
            return Response.status(500).entity("Error fetching jobs.").build();
        }
        return Response.status(200).entity(returnObject.toString()).build();
    }

    /**
     * GetJobs()
     *
     * Deprecated in favor of /rest/jobs
     * If NO params, returns all jobs in database.
     *
     * You can submit one or both params at once:
     *
     * @param createdBefore  Finds all jobs created Before time and date "2016-08-08T18:04:23.514Z"
     * @param createdAfter   Finds all jobs created After time and date. Format: 2016-08-08T18:04:23.514Z"
     * @return List of LGJobs
     */
    @GET
    @Deprecated
    @Path("jobs")
    @Produces(MediaType.APPLICATION_JSON)
    public Response jobGet(@QueryParam("created_before") String createdBefore,
                           @QueryParam("created_after")  String createdAfter) throws Exception {
        //List<LGJob> jobs;
        boolean doCreatedBefore = false;
        boolean doCreatedAfter = false;
        Date beforeDate = null;
        Date afterDate = null;

        // Parse created_before if present
        if ((createdBefore != null) && (!createdBefore.equals(""))) {
            Instant beforeInstant = null;
            try {
                beforeInstant = Instant.parse(createdBefore);
            } catch (DateTimeParseException e) {
                return Response.status(201).entity("Invalid created_on Date format [" + createdBefore + "]").build();
            }
            beforeDate = Date.from(beforeInstant);
            doCreatedBefore = true;
        }

        // Parse created_after if present
        if ((createdAfter != null) && (!createdAfter.equals(""))) {
            Instant afterInstant = null;
            try {
                afterInstant = Instant.parse(createdAfter);
            } catch (DateTimeParseException e) {
                return Response.status(201).entity("Invalid created_after Date format [" + createdAfter + "]").build();
            }
            afterDate = Date.from(afterInstant);
            doCreatedAfter = true;
        }


        JSONObject graphJobs = new JSONObject();
        try {
            graphJobs = Utils.getLemongraphJob(request, "", null);//this considers createdBefore/createdAfter params
        } catch (Exception e) {
            e.printStackTrace();
        }

        JSONArray ids = new JSONArray();
        Iterator<String> idIterator = graphJobs.keys();
        while (idIterator.hasNext()) {
            String id = idIterator.next();
            ids.put(id);
        }

        JSONObject ob = new JSONObject();
        if(ids.length() > 0) {
        // Build response
        SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm:ss");
        JSONObject mongoJobs = Utils.getMongoJobs(ids);
        Iterator iterator = mongoJobs.keySet().iterator();
        while (iterator.hasNext()) {
            String id = iterator.next().toString();
            try {
                JSONObject mongoJob = mongoJobs.getJSONObject(id);
                JSONObject job = new JSONObject();

                //Default value checks
                if (!mongoJob.has("reason")) {
                    mongoJob.put("reason", "");
                }
                if (!mongoJob.has("endTime")) {
                    mongoJob.put("endTime", 0);
                }
                if (!mongoJob.has("totalRunningTimeSeconds")) {
                    mongoJob.put("totalRunningTimeSeconds", 0);
                }
                if (!mongoJob.has("approvedAdapterNames")) {
                    mongoJob.put("approvedAdapterNames", new JSONArray());
                }
                if (!mongoJob.has("startTime")) {
                    mongoJob.put("startTime", 0);
                }
                if (!mongoJob.has("jobErrors")) {
                    mongoJob.put("jobErrors", new JSONArray());
                }
                if (!mongoJob.has("taskMap")) {
                    mongoJob.put("taskMap", new JSONObject());
                }
                if (!mongoJob.has("jobConfig")) {
                    mongoJob.put("jobConfig", new JSONObject());
                }
                if (!mongoJob.has("expireDate")) {
                    mongoJob.put("expireDate", 0);
                }
                if (!mongoJob.has("graphActivity")) {
                    mongoJob.put("graphActivity", 0);
                }
                if (!mongoJob.has("createDate")) {
                    mongoJob.put("createDate", 0);
                }
                if (!mongoJob.has("status")) {
                    mongoJob.put("status", 0);
                }

                job.put("reason", mongoJob.get("reason"));
                job.put("endtime", sdf.format(mongoJob.get("endTime")));
                job.put("runtime", mongoJob.get("totalRunningTimeSeconds"));
                job.put("approvedadapters", mongoJob.getJSONArray("approvedAdapterNames"));
                job.put("starttime", sdf.format(mongoJob.get("startTime")));
                job.put("error_count", mongoJob.getJSONArray("jobErrors").length());
                job.put("task_count", mongoJob.getJSONObject("taskMap").length());
                job.put("job_config", new JSONObject(mongoJob.get("jobConfig").toString()));
                job.put("expire_date", sdf.format(mongoJob.get("expireDate")));
                job.put("job_id", id);
                job.put("graph_activity", mongoJob.get("graphActivity"));
                job.put("create_date", sdf.format(mongoJob.get("createDate")));
                int status = mongoJob.getInt("status");
                job.put("status", LGJob.getStatusString(status));

                int active = 0;
                try {
                    if (status == LGJob.STATUS_PROCESSING) { //only processing jobs have non-zero active_task_count
                        ArrayList<Document> docs = new MongoDBStore().getTasksFromJob(id);
                        JSONObject tasks = MongoDBStore.toJSON(docs);
                        active = LGJob.getActiveTaskCount(tasks);
                    }
                } catch (Exception e) {
                    log.debug("Couldn't fetch active task count for job:" + id + " Error:" + e.getMessage());
                }

                job.put("active_task_count", active);
                ob.put(id, job);
            } catch (Exception e) {
                log.info("Invalid job:" + id + " Error:" + e.getMessage());
            }
        }
    }
        return Response.status(200).entity(ob.toString()).build();
    }

    /**
     * Gets all the jobs for the given status.
     * Valid status values are "NEW", PROCESSING", "FINISHED", "FINISHED_WITH_ERRORS"
     * "QUEUED", "STOPPED","EXPIRED", "RESET", "ERROR"
     */
    @GET
    @Path("jobs/status/{value}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response jobActive(@PathParam("value") String status) {
        if (status.equalsIgnoreCase("NEW")
                || status.equalsIgnoreCase("PROCESSING")
                || status.equalsIgnoreCase("QUEUED")
                || status.equalsIgnoreCase("STOPPED")
                || status.equalsIgnoreCase("EXPIRED")
                || status.equalsIgnoreCase("RESET")
                || status.equalsIgnoreCase("FINISHED_WITH_ERRORS")
                || status.equalsIgnoreCase("FINISHED")
                || (status.equalsIgnoreCase("ERROR"))) {
            List<LGJob> jobs = Utils.getJobManager().getAllByStatus(status);
            JSONObject ob = new JSONObject();
            for (LGJob job : jobs) {
                ob.put(job.getJobId(), job.toJson());
            }
            return Response.status(200).entity(ob.toString(1)).build();
        }
        return Response.status(500).entity("Invalid State Query ["+status+"]").build();
    }

    /** Just gives you jobId: status */
    @GET
    @Path("jobs/status")
    @Produces(MediaType.APPLICATION_JSON)
    public Response jobActiveByStatus() {
        List<LGJob> jobs = Utils.getJobManager().getAllLimitFields("_id", "status", "reason");
        JSONObject ob = new JSONObject();
        for (LGJob job : jobs) {
            JSONObject t = new JSONObject();
            t.put("status", job.getStatusString(job.getStatus()));
            t.put("reason", job.getReason());
            ob.put(job.getJobId(), t);
        }
        return Response.status(200).entity(ob.toString(1)).build();
    }

    @POST
    @Path("jobs/status/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response jobByStatusSingle(@PathParam("id") String job_id) {
        try {
            JSONObject newJob = getStatusObject(job_id);
            return Response.status(200).entity(newJob.toString()).build();
        }
        catch(Exception e) {
            e.printStackTrace();
            JSONObject ret = new JSONObject();
            ret.put("error", e.getMessage());
            return Response.status(500).entity(ret.toString()).build();
        }
    }

    /** Takes an array of job IDs as input; e.g. [1,2,3] */
    @PUT
    @Path("jobs/status")
    @Produces(MediaType.APPLICATION_JSON)
    public Response jobActiveByStatusBulk(String body) {
        try {
            JSONArray jobs = new JSONArray(body);
            JSONObject ob = new JSONObject();
            for (int i = 0; i < jobs.length(); i++) {
                String jobId = jobs.getString(i);
                JSONObject t = new JSONObject();
                LGJob lg = Utils.getJobManager().getJob(jobId);
                if (null == lg) {
                    t.put("error", "unknown");
                    t.put("reason", "");
                    ob.put(jobId, t);
                } else {
                    t.put("status", lg.getStatusString(lg.getStatus()));
                    t.put("reason", lg.getReason());
                    ob.put(jobId, t);
                }
            }
            return Response.status(200).entity(ob.toString(1)).build();
        }
        catch(Exception e) {
            e.printStackTrace();
            JSONObject ret = new JSONObject();
            ret.put("error", e.getMessage());
            return Response.status(500).entity(ret.toString()).build();
        }
    }

    @POST
    @Path("jobs/status")
    @Produces(MediaType.APPLICATION_JSON)
    public Response jobByStatusBulk(String input) {
        try {
            JSONArray jobs = new JSONArray(input);
            JSONArray newJobs = new JSONArray();
            Iterator jobsIterator = jobs.iterator();
            while(jobsIterator.hasNext()) {
                String job_id = jobsIterator.next().toString();
                try {
                    JSONObject newJob = getStatusObject(job_id);
                    newJobs.put(newJob);
                }
                catch(Exception e) {
                    e.printStackTrace();
                    JSONObject ret = new JSONObject();
                    ret.put("error", e.getMessage());
                    return Response.status(500).entity(ret.toString()).build();
                }
            }
            return Response.status(200).entity(newJobs.toString()).build();
        }
        catch(Exception e) {
            e.printStackTrace();
            JSONObject ret = new JSONObject();
            ret.put("error", e.getMessage());
            return Response.status(500).entity(ret.toString()).build();
        }
    }

    JSONObject getStatusObject(String job_id) throws Exception {
        String status = getStatus(job_id);
        JSONObject newJob = new JSONObject();
        if (status == null) {
            newJob
                    .put("status", "404")
                    .put("job_id", job_id)
            ;
        }
        else {
            Response graphResponse = Utils.lemongraphProxy(request, "GET", "graph/" + job_id + "/status", "");
            String body = graphResponse.readEntity(String.class);
            JSONObject job = new JSONObject(body);
            newJob
                    .put("job_id", job.get("graph"))
                    .put("config", job.get("meta"))
                    .put("maxID", job.get("maxID"))
                    .put("size", job.get("size"))
                    .put("status", status)
                    .put("errors", getErrors(job_id))
            ;
        }
        return newJob;
    }

    /**
     * Gets all the jobs for the given status.
     * Valid status values are "NEW", PROCESSING", "FINISHED", "QUEUED", "STOPPED","EXPIRED", "RESET", "ERROR"
     */
    @GET
    @Path("jobs/status/{value}/reason/{reason}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response jobActive(@PathParam("value") String status, @PathParam("reason") String reason) {

        if (status.equalsIgnoreCase("NEW")
                || status.equalsIgnoreCase("PROCESSING")
                || status.equalsIgnoreCase("QUEUED")
                || status.equalsIgnoreCase("STOPPED")
                || status.equalsIgnoreCase("EXPIRED")
                || status.equalsIgnoreCase("RESET")
                || status.equalsIgnoreCase("FINISHED")
                || (status.equalsIgnoreCase("ERROR"))) {
            List<LGJob> jobs = Utils.getJobManager().getAllByStatusAndReason(status, reason);
            JSONObject ob = new JSONObject();
            for (LGJob job : jobs) {
                ob.put(job.getJobId(), job.toJson());
            }
            return Response.status(200).entity(ob.toString(1)).build();
        }
        return Response.status(500).entity("Invalid State Query ["+status+"]").build();
    }

    /**
     * Used by the lgstats program for now.
     */
    @GET
    @Path("/jobs/days/full/{from_days}/{to_days}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobByIdDaysFull(@PathParam("from_days") int fdays, @PathParam("to_days") int tdays) {
        List<LGJob> jobs = Utils.getJobManager().getAllByDays(fdays, tdays);
        if (Utils.getLemongraph().client == null) {
            return Response.status(404).entity("Not found").build();
        }
        JSONObject ob = new JSONObject();
        for (LGJob job : jobs) {
            JSONObject tmpJob = new JSONObject();
            tmpJob.put("job",job.toJson());

            JSONArray tasks = job.getTaskList();
            tmpJob.put("tasks",tasks);

            List<LGJobError> errors = job.getJobErrors();
            JSONArray errorsJson = new JSONArray();
            for (LGJobError je : errors) {
                errorsJson.put(je);
            }
            tmpJob.put("errors",errorsJson);

            List<LGJobHistory> history = job.getJobHistory();
            JSONArray historyJson = new JSONArray();
            for (LGJobHistory jh : history) {
                historyJson.put(jh.toJson());
            }
            tmpJob.put("history",historyJson);

            ob.put(job.getJobId(),tmpJob);
        }
        return Response.status(200).entity(ob.toString()).build();
    }

    /**
     * Gets you values from a certain day to a certain date. If you set to_days to 0, it will get all
     * days older than from_days. Deprecated because still used by the api pages and scripts.
     *
     * For example,    /jobs/days/30/0  Will get you all jobs older than 30 days
     *                 /jobs/days/30/60 Will get you all jobs between 30 and 60 days old
     *
     */
    @GET
    @Deprecated
    @Path("/jobs/days/{from_days}/{to_days}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobByIdDays(@PathParam("from_days") int fdays, @PathParam("to_days") int tdays) {
        if ((tdays != 0)&& (tdays < fdays)) {
            return Response.status(404).entity("Invalid parameters").build();
        }
        List<LGJob> jobs = Utils.getJobManager().getAllByDays(fdays, tdays);
        if (Utils.getLemongraph().client == null) {
            return Response.status(404).entity("Not found").build();
        }
        JSONObject ob = new JSONObject();
        for (LGJob job : jobs) {
            ob.put(job.getJobId(),job.toJson());
        }
        return Response.status(200).entity(ob.toString()).build();
    }

    @GET
    @Deprecated
    @Path("/jobs/days/olderthan/{days}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobByIdOlderThanDays(@PathParam("days") int days) {
        List<LGJob> jobs = Utils.getJobManager().getAllByOlderThanDays(days);
        if (Utils.getLemongraph().client == null) {
            return Response.status(404).entity("Not found").build();
        }
        JSONObject ob = new JSONObject();
        for (LGJob job : jobs) {
            ob.put(job.getJobId(),job.toJson());
        }
        return Response.status(200).entity(ob.toString()).build();
    }

    @GET
    @Path("/jobs/mins/{mins}/{tomins}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobByIdMins(@PathParam("mins") int mins, @PathParam("tomins") int tomins) {
        List<LGJob> jobs = Utils.getJobManager().getAllByMins(mins, tomins);
        if (Utils.getLemongraph().client == null) {
            return Response.status(404).entity("Not found").build();
        }
        JSONObject ob = new JSONObject();
        for (LGJob job : jobs) {
            ob.put(job.getJobId(),job.toJson());
        }
        return Response.status(200).entity(ob.toString()).build();
    }

    /** Gets the last X jobs by create_date */
    @GET
    @Path("/jobs/last/{count}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobsCount(@PathParam("count") int count) {
        List<LGJob> jobs = Utils.getJobManager().getLast(count);
        if (Utils.getLemongraph().client == null) {
            return Response.status(404).entity("Not found").build();
        }
        JSONObject ob = new JSONObject();
        for (LGJob job : jobs) {
            ob.put(job.getJobId(),job.toJson());
        }
        return Response.status(200).entity(ob.toString(1)).build();
    }

    @GET
    @Path("/jobs/age/{days}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobByIdAge(@PathParam("days") int days) {
        List<LGJob> jobs = Utils.getJobManager().getAllByAge(days);
        if (Utils.getLemongraph().client == null) {
            return Response.status(404).entity("Not found").build();
        }
        JSONObject ob = new JSONObject();
        for (LGJob job : jobs) {
            ob.put(job.getJobId(),job.toJson());
        }
        return Response.status(200).entity(ob.toString()).build();
    }

    @GET
    @Path("/job/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobById(@PathParam("id") String jobId) {
        LGJob lg = Utils.getJobManager().getJob(jobId);
        if (null == lg) {
            return Response.status(404).entity("Not found").build();
        }
        return Response.status(200).entity(lg.toJson().toString(1)).build();
    }

    @GET
    @Path("/job/standardJob/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobByIdStandardJob(@PathParam("id") String jobId) throws Exception {

        JSONObject job = Utils.getStandardJob(request, jobId);
        if (null ==job) {
            return Response.status(404).entity("Not found").build();
        }
        return Response.status(200).entity(job.toString(1)).build();
    }

    @GET
    @Path("/job/{id}/full")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobByIdFull(@PathParam("id") String jobId) {
        LGJob lg = Utils.getJobManager().getJob(jobId);
        if (null == lg) {
            return Response.status(404).entity("Not found").build();
        }
        JSONObject jobResult = lg.toJson();
        jobResult.put("history",getHistoryHelper(lg));
        jobResult.put("errors",getErrorsHelper(lg));
        jobResult.put("tasks",getTasksHelper(lg));
        return Response.status(200).entity(jobResult.toString(1)).build();
    }

    /**
     * Get job specific metrics
     *
     * @return JSON blob that contains data that's easily graphed/displayed with javascript tools
     */
    @GET
    @Path("/job/{id}/metrics")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobByIdMetrics(@PathParam("id") String jobId) {
        LGJob lg = Utils.getJobManager().getJob(jobId);
        if (null == lg) {
            return Response.status(404).entity("Not found").build();
        }
        JSONObject jobResult = new JSONObject();
        Map<String, LGTask> tasks = lg.getTasks();

        // BUild history graph size per task (bar graph)
        List<LGJobHistory> history = lg.getJobHistory();
        JSONArray graphPerTask   = new JSONArray();
        JSONArray idPerTask      = new JSONArray();
        JSONArray adapterPerTask = new JSONArray();
        JSONArray errorPerTask   = new JSONArray();

        JSONArray adapterPie      = new JSONArray();
        JSONArray adapterPieLabels= new JSONArray();
        HashMap<String, Integer> adapterPieCounters = new HashMap<String,Integer>();
        for(LGJobHistory l: history) {
            if (l.getCommandType() == 1) {
                LGTask t = tasks.get(l.getTaskId());
                if (t == null) {
                    log.error("Missing task for taskid:"+l.getTaskId());
                } else {

                    String adapter = t.getAdapterName();
                    int graphChange = l.getGraphChanges();
                    graphPerTask.put(graphChange);
                    idPerTask.put(l.getTaskId());
                    adapterPerTask.put(adapter);
                    int count = graphChange;
                    if (adapterPieCounters.containsKey(adapter)) {
                        count = adapterPieCounters.get(adapter).intValue();
                        count += graphChange;
                    }
                    adapterPieCounters.put(adapter,count);
                    int error = 0;
                    if (t.getStatus() != t.TASK_STATUS_COMPLETE) {
                        error = 1;
                    }
                    errorPerTask.put(error);
                }
            }
        }

        for (Map.Entry<String, Integer> entry : adapterPieCounters.entrySet()) {
            String adapter = entry.getKey();
            Integer value  = entry.getValue();
            adapterPieLabels.put(adapter);
            adapterPie.put(value.intValue());
        }

        jobResult.put("graph_changes_per_task",graphPerTask);
        jobResult.put("id_per_task",idPerTask);
        jobResult.put("adapter_per_task",adapterPerTask);
        jobResult.put("error_per_task",errorPerTask);
        jobResult.put("adapter_pie_labels",adapterPieLabels);
        jobResult.put("adapter_pie",adapterPie);

        return Response.status(200).entity(jobResult.toString(1)).build();
    }

    @GET
    @Path("/job/{id}/status")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobStatus(@PathParam("id") String jobId) {
        LGJob lg = Utils.getJobManager().getJob(jobId);
        if (null == lg) {
            return Response.status(404).entity("Not found").build();
        }
        JSONObject result = new JSONObject();
        result.put("status",lg.getStatusString(lg.getStatus()));
        result.put("reason",lg.getReason());

        return Response.status(200).entity(result.toString()).build();
    }

    @GET
    @Path("/job/{id}/history")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobHistory(@PathParam("id") String jobId) {
        LGJob lg = Utils.getJobManager().getJob(jobId);
        if (null == lg) {
            return Response.status(404).entity("Not found").build();
        }
        JSONObject historyResult = new JSONObject();
        historyResult.put("history", getHistoryHelper(lg));
        return Response.status(200).entity(historyResult.toString(1)).build();
    }

    private JSONArray getHistoryHelper(LGJob lg) {
        List<LGJobHistory> history = lg.getJobHistory();
        JSONArray result = new JSONArray();
        for(LGJobHistory l: history) {
            JSONObject rl = l.toJson();
            result.put(rl);
        }
        return result;
    }

    /** Gets task map for job, useful for troubleshooting/testing */
    @GET
    @Path("/job/{id}/tasks")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobTasks(@PathParam("id") String jobId) {
        LGJob lg = Utils.getJobManager().getJob(jobId);
        if (null == lg) {
            return Response.status(404).entity("Not found").build();
        }
        JSONObject result = new JSONObject();
        result.put("tasks",getTasksHelper(lg));
        return Response.status(200).entity(result.toString(1)).build();
    }

    private JSONArray getTasksHelper(LGJob lg) {
        JSONObject result = new JSONObject();
        JSONArray s = lg.getTaskList();
        return s;
    }

    /** Gets task map for job, useful for troubleshooting/testing */
    @GET
    @Path("/job/dbValues/{dbValue}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllJobsThatHavedbValue(@PathParam("dbValue") String dbValue) {
        JSONArray allJobs = Utils.getJobManager().getAllJobsThatHaveDbValueKeyJSONArray(dbValue.toLowerCase());
        if (allJobs.length() == 0) {
            return Response.status(404).entity("dbValue " + dbValue + " was not found in the database.").build();
        }
        return Response.status(200).entity(allJobs.toString(1)).build();
    }

    /** Gets task map for job, useful for troubleshooting/testing */
    @GET
    @Path("/job/{id}/{key}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobdbValueByKey(@PathParam("id") String jobId, @PathParam("key") String key) {
        LGdbValue value = Utils.getJobManager().getDbValuesByJobIdandKey(jobId, key);
        if (null == value) {
            return Response.status(404).entity("JobId " + jobId + " not found in " + key + " database.").build();
        }

//        JSONObject result = new JSONObject();
//        result.put(key,lgValue.toJson());
        return Response.status(200).entity(value.toJson().toString(1)).build();
    }

    @GET
    @Path("/job/{id}/dbValues")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobdbValueAll(@PathParam("id") String jobId) {
        LGdbValue lGdbValue = Utils.getJobManager().getDbValuesByJobId(jobId);
        if (null == lGdbValue) {
            return Response.status(404).entity("JobId " + jobId + " not found in database.").build();
        }

//        JSONObject result = new JSONObject();
//        result.put(lGdbValue.toJson());
        return Response.status(200).entity(lGdbValue.toJson().toString(1)).build();
    }

    /** Gets error list for job */
    @GET
    @Path("/job/{id}/errors")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobErrors(@PathParam("id") String jobId) {
        JSONObject errorResult = new JSONObject();
        errorResult.put("errors",getErrors(jobId));
        if (null == errorResult) {
            return Response.status(404).entity("Not found").build();
        }
        return Response.status(200).entity(errorResult.toString(1)).build();
    }

    JSONArray getErrors(String jobId) {
        LGJob lg = Utils.getJobManager().getJob(jobId);
        if (null == lg) { return null;
        }
        return getErrorsHelper(lg);
    }

    String getStatus(String jobId) {
        LGJob lg = Utils.getJobManager().getJob(jobId);
        if (null == lg) { return null;
        }
        return lg.getStatusString(lg.getStatus());
    }

    private String getStatusHelper(LGJob lg) {
        if (null == lg) { return null; }
        return lg.getStatusString(lg.getStatus());
    }

    public static JSONArray getErrorsHelper(LGJob lg) {
        if (null == lg) { return null; }
        List<LGJobError> errors = lg.getJobErrors();
        JSONArray result = new JSONArray();
        for(LGJobError l: errors) {
            JSONObject rl = l.toJson();
            result.put(rl);
        }
        return result;
    }

    /** Gets the graph from LemonGraph/DB */
    @GET
    @Path("/job/{id}/graph")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobGraphData(@PathParam("id") String jobId) {
        if (Utils.graphStoreStr.equalsIgnoreCase("lemongraph")) {
            try {
                JSONObject graph = LemonGraph.getGraph(jobId);
                return Response.status(200).entity(graph.toString()).build();
            } catch (Exception e) {
                log.error("Lookup from LemonGraph failed " + e.getMessage());
                return Response.status(404).entity("Graph Not stored in lemongraph").build();
            }
        }
        return Response.status(404).entity("Not found").build();
    }

    /** Gets the graph from LemonGraph/DB in cytoscape format */
    @GET
    @Path("/job/{id}/graph/cytoscape")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobGraphDataCytoscape(@PathParam("id") String jobId) {
        //
        if (Utils.graphStoreStr.equalsIgnoreCase("lemongraph")) {
            try {
                JSONObject graph = LemonGraph.getGraphCytoscape(jobId);
                return Response.status(200).entity(graph.toString()).build();
            } catch (Exception e) {
                log.error("Lookup from LemonGraph failed " + e.getMessage());
                return Response.status(404).entity("Graph Not stored in lemongraph").build();
            }
        }
        return Response.status(404).entity("Not found").build();
    }

    /** Gets the graph from LemonGraph/DB in d3 format */
    @GET
    @Path("/job/{id}/graph/d3")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobGraphDataD3(@PathParam("id") String jobId) {
        //
        if (Utils.graphStoreStr.equalsIgnoreCase("lemongraph")) {
            try {
                JSONObject graph = LemonGraph.getGraphD3(jobId);
                return Response.status(200).entity(graph.toString()).build();
            } catch (Exception e) {
                log.error("Lookup from LemonGraph failed " + e.getMessage());
                return Response.status(404).entity("Graph Not stored in lemongraph").build();
            }
        }
        return Response.status(404).entity("Not found").build();
    }

    /** Gets the graph from LemonGraph/DB in d3 format */
    @GET
    @Path("/job/{id}/graph/d3/{start}/{stop}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobGraphDataD3(@PathParam("id") String jobId
            , @PathParam("start") int start
            , @PathParam("stop") int stop) {

        if (Utils.graphStoreStr.equalsIgnoreCase("lemongraph")) {
            try {
                JSONObject graph = LemonGraph.getGraphD3(jobId, start, stop);
                return Response.status(200).entity(graph.toString()).build();
            } catch (Exception e) {
                log.error("Lookup from LemonGraph failed " + e.getMessage());
                return Response.status(404).entity("Graph Not stored in lemongraph").build();
            }
        }
        return Response.status(404).entity("Not found").build();
    }

    @PUT
    @Path("/job/{id}/cancel")
    @Produces(MediaType.APPLICATION_JSON)
    public Response cancelJob(@PathParam("id") String jobId) throws Exception {
        log.info("Cancel job received for "+jobId);
        JSONObject params = Utils.getRequestParameters(request);
        params.put("ids", new JSONArray().put(jobId));
        JSONObject ret = Utils.cancelHelper(params);
        return Response.status(200).entity(ret.toString()).build();
    }

    @PUT
    @Path("/jobs/cancel")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response cancelJobs(String body) throws Exception {
        JSONArray jobIds = new JSONArray(body);
        JSONArray retVals = new JSONArray();
        JSONObject params = Utils.getRequestParameters(request);
        params.put("ids", jobIds);
        JSONObject ret = Utils.cancelHelper(params);
        retVals.put(ret);
        return Response.status(200).entity(retVals.toString()).build();
    }

    /**
     * Single Job reset call
     * Optional: pass a jsonobject in the body with key of "REASON" and this will be stored with the
     *           job history.
     * Delete graph information from LEMONGRAPH. Job meta data remains in database(mongo) and can be reran in
     * the future.
     *
     * Result
     *          status : job status after this operation
     *          reason : job status after this operation
     *          status_code:: 409 is  conflist
     *
     *
     *
     * See api documentation for more information
     */
    @PUT
    @Path("/job/{id}/reset")
    @Produces(MediaType.APPLICATION_JSON)
    public Response resetJob(@PathParam("id") String jobId, String body) {
        log.info("Raw resetjob " + body.toString());
        String reason = "";
        JSONObject  jb = new JSONObject(body);
        if (jb.has("reason")) {
            reason = jb.getString("reason");
        }
        Boolean allowOverwrite = false;  // Default behaviour (if you send a reset on a reset , overwrite will allow
        // you to change the reason
        if (jb.has(LGConstants.LG_RESET_OVERWRITE)) {
            allowOverwrite = jb.getBoolean(LGConstants.LG_RESET_OVERWRITE);
        }
        log.info("Reset job received for [" + jobId + "]  Reason [" + reason + "]");
        JSONObject ret = Utils.resetHelper(jobId, reason, allowOverwrite);
        int statusCode = 200;
        if (ret.has(jobId)) {
            if (ret.getJSONObject(jobId).has("job_code"))
                statusCode = ret.getInt("status_code");
        }
        return Response.status(statusCode).entity(ret.toString()).build();
    }

    /**
     * Bulk Job reset call
     * Deletes graph information from LEMONGRAPH. Job meta data remains in database(mongo) and can be reran in
     * the future.
     * See api documentation for more information
     */
    @PUT
    @Path("/jobs/reset")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response resetJobsPut(String body) {
        return resetJobs(body);
    }

    @POST
    @Path("/jobs/reset")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response resetJobsPost(String body) {
        try {
            JSONArray jobIds = new JSONArray(body);
            return resetJobs(jobIds);
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(500).entity(e.getMessage()).build();
        }
    }

    public Response resetJobs(String body) {
        JSONObject  jb = new JSONObject(body);
        if (!jb.has("jobs")) {
            log.error("Missing 'jobs' field.");
            return Response.status(500).entity("{'error':'missing jobs field'}").build();
        }
        // Note: this is the global reason. If the parser sees a reason for an individual job listing
        //       it will use that instead.
        String globalreason = "";
        if (jb.has(LGConstants.LG_RESET_REASON)) {
            globalreason = jb.getString(LGConstants.LG_RESET_REASON);
        }
        log.info("Received bulk reset command global reason ["+globalreason+"]");

        Boolean allowOverwrite = false;  // Default behaviour (if you send a reset on a reset , overwrite will allow
        // you to change the reason
        if (jb.has(LGConstants.LG_RESET_OVERWRITE)) {
            allowOverwrite = jb.getBoolean(LGConstants.LG_RESET_OVERWRITE);
        }

        JSONObject jobs = jb.getJSONObject("jobs");
        JSONObject retVals = new JSONObject();

        for(Object key: jobs.keySet()) {
            String jobId = (String) key;
            JSONObject info = jobs.getJSONObject(jobId);
            System.out.println("  Processing:"+info.toString());
            // You can supply individual reason for a specific job, otherwise the 'global'
            String tmpReason = globalreason;
            if (info.has(LGConstants.LG_RESET_REASON)) {
                tmpReason = info.getString(LGConstants.LG_RESET_REASON);
            }
            try {
                log.info("Resetting job : "+jobId);
                JSONObject ret = Utils.resetHelper(jobId, tmpReason, allowOverwrite);
                JSONObject data = ret.getJSONObject(jobId);
                retVals.put(jobId, data);
            } catch (Exception e) {
                JSONObject ret = new JSONObject();
                ret.put("message", "Job with the id " + jobId + " Reset FAILED: "+e.getMessage());
                ret.put("status_code", 201);
                ret.put("reset",false);
                retVals.put(jobId, ret);
            }
        }
        return Response.status(200).entity(retVals.toString()).build();
    }

    public Response resetJobs(JSONArray jobIds) {
        String globalreason = "";
        log.info("Received bulk reset command global reason [" + globalreason + "]");

        Boolean allowOverwrite = false;  // Default behaviour (if you send a reset on a reset , overwrite will allow

        JSONObject retVals = new JSONObject();

        for(int i = 0; i < jobIds.length(); i++) {
            String jobId = jobIds.getString(i);
            System.out.println("  Processing:"+jobId);
            // You can supply individual reason for a specific job, otherwise the 'global'
            String tmpReason = globalreason;
            try {
                log.info("Resetting job : "+jobId);
                JSONObject ret = Utils.resetHelper(jobId, tmpReason, allowOverwrite);
                JSONObject data = ret.getJSONObject(jobId);
                retVals.put(jobId, data);
            } catch (Exception e) {
                JSONObject ret = new JSONObject();
                ret.put("message", "Job with the id " + jobId + " Reset FAILED: "+e.getMessage());
                ret.put("status_code", 201);
                ret.put("reset",false);
                retVals.put(jobId, ret);
            }
        }
        return Response.status(200).entity(retVals.toString()).build();
    }

    /**
     * Single Job Retry call
     *
     * This call will retry all your failed tasks for a job. Any new tasks spawned from these
     * failed tasks will continue processing as normal. Job state will return to PROCESSING
     * If there are no failed tasks, nothing will occur.
     *
     */
    @PUT
    @Path("/job/{id}/retry")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response retryJob(@PathParam("id") String jobId, String body) {
        String reason = "";
        JSONObject  jb = new JSONObject(body);
        log.info("Retry job received for [" + jobId + "]");
        JSONObject ret = retryHelper(jobId, "");
        return Response.status(200).entity(ret.toString()).build();
    }

    @PUT
    @Path("/job/{id}/retry/{taskid}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response retryJobForTask(@PathParam("id") String jobId, @PathParam("taskid") String taskId, String body) {
        String reason = "";
        JSONObject  jb = new JSONObject(body);
        log.info("Retry job received for [" + jobId + "] and task [" + taskId + "]");
        if ((jobId == null) || jobId.equals("")) {
            return Response.status(201).entity("{ 'error':'Invalid jobId'}").build();
        }
        if ((taskId == null) || taskId.equals("")) {
            return Response.status(201).entity("{ 'error':'Invalid taskId'}").build();
        }

        JSONObject ret = retryHelper(jobId,taskId);
        return Response.status(200).entity(ret.toString()).build();
    }

    /**
     * Used by /jobs/retry and /job/retry
     *
     * @param jobId the job id to retry
     * @param taskId if you want to only retry a certain task inside a job. set to "" otherwise
     * @return JSONOBject - results
     */
    private JSONObject retryHelper(String jobId, String taskId) {
        LGJob lg = Utils.getJobManager().getJob(jobId);

        JSONObject jobInfo = new JSONObject();
        JSONObject ret     = new JSONObject();
        if (null == lg) {
            jobInfo.put("message", "job not found");
            jobInfo.put("retry",false);
            jobInfo.put("status_code",400);
            ret.put(jobId, jobInfo);
            return ret;
        }
        int status = lg.getStatus();
        if ( (status != LGJob.STATUS_FINISHED_WITH_ERRORS)) {
            jobInfo.put("message", "Can not retry job, Job is "+lg.getStatusString(status));
            jobInfo.put("retry",false);
            jobInfo.put("status_code", 201);
            ret.put(jobId, jobInfo);
            return ret;
        }

        try {
            Utils.getSubmitToRabbit().sendRetry(jobId, taskId);
        }
        catch (Exception e) {
            jobInfo.put("message", "Can not retry job "+e.getMessage());
            jobInfo.put("retry",false);
            jobInfo.put("status_code",201);
            ret.put(jobId, jobInfo);
            return ret;
        }
        jobInfo.put("message", "");
        jobInfo.put("retry", true);
        jobInfo.put("status_code",200);
        ret.put(jobId, jobInfo);
        return ret;
    }

    /**
     * Deletes from Mongo Lemongrenade and from Lemongraph
     */
    @DELETE
    @Path("/job/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteJobEndpoint(@PathParam("id") String jobId) {
        log.info("API Delete Job received " + jobId);
        JSONObject ret = Utils.deleteJob(jobId);
        return Response.status(500).entity(ret.toString()).build();
    }

    /**
     * BULK Delete from Mongo Lemongrenade and from Lemongraph
     */
    @POST
    @Path("/jobs/delete")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response deleteJobs(String body) {
        log.info("API Delete Jobs received " + body);
        JSONArray retVals = new JSONArray();
        String jobId;
        JSONObject params = Utils.getRequestParameters(request);
        try {
            JSONArray input_jobs = new JSONArray(body);
            for (int i = 0; i < input_jobs.length(); i++) {
                jobId = input_jobs.getString(i);
                try {
                    log.info("Deleting job : " + jobId);
                    params.put("ids", new JSONArray().put(jobId));
                    JSONObject ret = Utils.deleteHelper(params);
                    retVals.put(ret);
                } catch (Exception e) {
                    JSONObject ret = new JSONObject();
                    ret.put("error", "Job with the id " + jobId + " FAILED: " + e.getMessage());
                    ret.put("deleted", false);
                    retVals.put(ret);
                }
            }
            return Response.status(200).entity(retVals.toString()).build();
        }
        catch(Exception e) {
            e.printStackTrace();
            return Response.status(500).entity(retVals.toString()).build();
        }
    }

    /**
     * Create New Job
     */
    @POST
    @Path("/job")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public static Response createJobFromJsonNoJobId(String jobJson) throws Exception {
        JSONObject result = new JSONObject();
        log.info("RAW POST REQUEST : "+jobJson);
        JSONObject job;
        try {
            job = new JSONObject(jobJson);
        } catch (org.json.JSONException e) {
            JSONObject ret = new JSONObject();
            ret.put("error", "Error Parsing JSON. " + e.getMessage() );
            ret.put("original_request",jobJson);
            return Response.status(500).entity(ret.toString()).build();
        }

        // Required adapterlist, seed
        ArrayList<String> approvedAdapters = new ArrayList<>();
        JSONObject seedData  = new JSONObject();

        if (job.has("seed")) {
            seedData = job.getJSONObject("seed");
        }
        // Add Incoming Nodes
        JSONArray nodes = new JSONArray();
        if (seedData.has("nodes")) {
            nodes = seedData.getJSONArray("nodes");
        }

        JSONObject jobConfig = new JSONObject();
        if (job.has("job_config")) {
            jobConfig = job.getJSONObject("job_config");
        }

        boolean graph_group = false;
        if(jobConfig.has("graph_group") && jobConfig.getBoolean("graph_group") == true) {
            graph_group = true;
        }
        else if(jobConfig.has("job_type") && jobConfig.get("job_type").toString().toLowerCase().equals("graph_group")) {
            graph_group = true;
        }

        String jobId = "";
        if(jobConfig.has("job_id")) {
            jobId = jobConfig.getString("job_id");
        }

        if(jobId.length() == 0) { //create a job ID
            jobId = LemonGraph.createGraph(new JSONObject()); //fetch a valid jobId from LEMONGRAPH
            LemonGraph.deleteGraph(jobId);//clear the empty job we got the ID from
        }

        jobConfig.put("job_id", jobId);
        // Parse the adapter list from the job_config
        try {
            approvedAdapters = Utils.getAdapterManager().parseAdaptersListFromJobConfig(jobConfig);
        }
        catch (Exception e) {
            JSONObject ret = new JSONObject();
            ret.put("error", "Error parsing adapter list:"+e.getMessage());
            return Response.status(500).entity(ret.toString()).build();
        }

        log.info("Adapter list : "+approvedAdapters.toString());

        // Set any missing default information in jobConfig
        if (!jobConfig.has("depth")) {
            int depth = LGProperties.getInteger("api.default_depth", 3);
            jobConfig.put("depth", depth);
        }
        if (!jobConfig.has("ttl")) {
            int ttl = LGProperties.getInteger("api.default_ttl", 0);
            jobConfig.put("ttl", ttl);
        }
        if (!jobConfig.has("priority")) {
            String priority = LGProperties.get("api.default_priority", "user_low");
            jobConfig.put("priority", priority);
        }
        if (!jobConfig.has("description")) {
            String description = LGProperties.get("api.default_description", "");
            jobConfig.put("description", description);
        }

        log.info("Job Post - adapterlist: " + approvedAdapters.toString()
                + " seed" + seedData.toString()
                + " job_config:" + jobConfig.toString());

        LGPayload newPayload = new LGPayload(jobConfig);

        for (int i = 0; i < nodes.length(); i++) {
            JSONObject node = nodes.getJSONObject(i);
            if(!node.has("LG:METADATA")) {node.put("LG:METADATA", new JSONObject());}
            newPayload.addResponseNode(node);
        }
        // Add incoming Edges
        JSONArray edges = new JSONArray();
        if (seedData.has("edges")) {
            edges = seedData.getJSONArray("edges");
        }
        for (int i = 0; i < edges.length(); i++) {
            JSONArray edgeData = edges.getJSONArray(i); //source, edge, target
            if(edgeData.length() == 3) {//add an edge of the size is correct
                Iterator iterator = edgeData.iterator();
                while(iterator.hasNext()) {
                    JSONObject node = (JSONObject) iterator.next();
                    if(!node.has("LG:METADATA")) {node.put("LG:METADATA", new JSONObject());}
                }
                JSONObject src = edgeData.getJSONObject(0);
                JSONObject edge = edgeData.getJSONObject(1);
                JSONObject target = edgeData.getJSONObject(2);
                newPayload.addResponseEdge(src, edge, target);
            }
            throw new Exception("Invalid edge present:"+edgeData);
        }


        try {
            LGJob lgJob = Utils.getSubmitToRabbit().sendNewJobToCommandController(approvedAdapters, newPayload);
            jobId = lgJob.getJobId();
        } catch (Exception e) {
            log.error("Failed to sendNewJobToCommandController.");
            e.printStackTrace();
            return Response.status(500).entity(e.getMessage()).build();
        }

        // Return result in JSON
        result.put("status", "created");
        result.put("job_id", jobId);

        return Response.status(200).entity(result.toString()).build();
    }

    /**
     *  Add To Job
     *
     * adapterList  (You can add new adapters?)
     *
     * Little different than Create Job, users have the option of passing in data they want to
     * add to the job and marking each piece as a seed or not.
     *
     * Required:
     *
     * job_data {}   - New Data to post to graph
     * job_config.adapers() - Just like the post job, it's a list of adapters
     *
     */
    @POST
    @Path("/job/{id}/insert")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response addToJob(@PathParam("id") String jobId, String jobJson) {

        JSONObject job = new JSONObject(jobJson);

        // Required adapterlist, seed
        ArrayList<String> approvedAdapters = new ArrayList<>();
        JSONObject jobData  = new JSONObject();

        if (job.has("job_data")) {
            jobData = job.getJSONObject("job_data");
        }
        JSONObject jobConfig = new JSONObject();
        if (job.has("job_config")) {
            jobConfig = job.getJSONObject("job_config");
        }

        // Parse the adapter list from the job_config
        try {
            approvedAdapters = Utils.getAdapterManager().parseAdaptersListFromJobConfig(jobConfig);
        }
        catch (Exception e) {
            JSONObject ret = new JSONObject();
            ret.put("error", "Error parsing adapter list:"+e.getMessage());
            return Response.status(500).entity(ret.toString()).build();
        }

        log.info("Adapter list "+approvedAdapters.toString());
        if ((approvedAdapters.size() == 0) || jobData.equals("")) {
            JSONObject ret = new JSONObject();
            ret.put("error", "Missing required job information, adapterlist, seed");
            return Response.status(500).entity(ret.toString()).build();
        }


        // If job_id is in jobConfig make sure it matches the job_id variable
        if (jobConfig.has("job_id")) {
            String jid = jobConfig.getString("job_id");
            if (!jobId.equals(jid)) {
                // Throw error
                return Response.status(500).entity("Job ID mismatch job_id and jobconfig{job_id}").build();
            }
        } else {
            // Append job_id to job_config
            jobConfig.put("job_id",jobId);
        }

        // Set missing default information in jobConfig
        if (!jobConfig.has("depth")) {
            int depth = LGProperties.getInteger("api.default_depth",3);
            jobConfig.put("depth",depth);
        }
        if (!jobConfig.has("ttl")) {
            int ttl = LGProperties.getInteger("api.default_ttl",0);
            jobConfig.put("ttl",ttl);
        }
        if (!jobConfig.has("priority")) {
            String priority = LGProperties.get("api.default_priority", "user_low");
            jobConfig.put("priority",priority);
        }
        if (!jobConfig.has("description")) {
            String description = LGProperties.get("api.default_description", "");
            jobConfig.put("description",description);
        }

        log.info("Job ADD TO JOB  job_id "+jobId+"  adapterlist: "+approvedAdapters.toString()+"  seed"+jobData.toString()
                +"  job_config:"+jobConfig.toString());

        // Make sure the job already exists in the system
        LGJob lg = Utils.getJobManager().getJob(jobId);
        if (null == lg) {
            JSONObject ret = new JSONObject();
            ret.put("error", "Job with the id " + jobId + " Does not exist. Job add cancelled.");
            return Response.status(404).entity(ret.toString()).build();
        }

        LGPayload newPayload = new LGPayload(jobConfig);
        // Add incoming Nodes
        JSONArray nodes = new JSONArray();
        if (jobData.has("nodes")) {
            nodes = jobData.getJSONArray("nodes");
        }
        for(int i = 0; i < nodes.length(); i++){
            JSONObject node = nodes.getJSONObject(i);
            newPayload.addResponseNode(node);
        }
        // Add incoming Edges
        JSONArray edges = new JSONArray();
        if (jobData.has("edges")) {
            edges = jobData.getJSONArray("edges");
        }
        for(int i = 0; i < edges.length(); i++){
            JSONObject edge = edges.getJSONObject(i);
            // TODO: proper support for edges required here (src, tgt, data)
            //newPayload.addResponseEdge(edge);
        }

        // Set the payload type
        newPayload.setPayloadType(LGConstants.LG_PAYLOAD_TYPE_COMMAND);

        /*
         rrayList<String> approvedAdapters = new ArrayList<>();
         for (int i=0; i< adapterList.length(); i++) {
         approvedAdapters.add(adapterList.getString(i));
         }*/

        try {
            Utils.getSubmitToRabbit().sendAddToJobToCommandController(jobId, approvedAdapters, newPayload);
        }
        catch (Exception e) {
            return Response.status(201).entity(e.getMessage()).build();
        }

        // Return result in JSON
        JSONObject result = new JSONObject();
        result.put("status", "added");
        result.put("job_id",jobId);

        return Response.status(200).entity(result.toString()).build();
    }

    /**
     *  postaction - Allows you to run certain adapters on specific nodes.
     *   Only required information is post_action_job_config, post_action_nodes, and post_action_adapters
     *
     *  {
     *    "post_action_job_config": {
     *        "post_action_nodes": ["2","3","4"],
     *        "description": "job description",
     *        "adapters": {
     *               "PlusBang": {}
     *        }
     *    }
     * }
     *
     */
    @POST
    @Path("/job/{id}/postaction")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response postAction(@PathParam("id") String jobId, String jobJson) {
        JSONObject result = new JSONObject();
        ArrayList<String> postActionAdapters = new ArrayList<>();
        JSONArray postActionNodes    = new JSONArray() ;
        JSONObject jobConfig         = new JSONObject();
        result.put("job_id", jobId);
        JSONObject job = new JSONObject(jobJson);
        log.info("Received RAW post action request:"+jobJson);

        // Sanity Check input data for required
        if (job.has("post_action_job_config")) {
            jobConfig = job.getJSONObject("post_action_job_config");
        } else {
            result.put("message", "Missing required post_action_job_config");
            return Response.status(201).entity(result.toString()).build();
        }
        if (jobConfig.has("nodes")) {
            postActionNodes = jobConfig.getJSONArray("nodes");
        } else {
            result.put("message", "Missing required post_action_job_config:nodes");
            return Response.status(201).entity(result.toString()).build();
        }
        if (postActionNodes.length() == 0) {
            result.put("message", "Empty post_action_nodes array");
            return Response.status(201).entity(result.toString()).build();
        }
        if(!(postActionNodes.get(0) instanceof String)) { //if the first postActionNode isn't a String, warn the submitter
            result.put("message", "post_action_nodes array must contain String types.");
            return Response.status(201).entity(result.toString()).build();
        }

        // Parse the adapter list from the post_action_job_config
        if (!jobConfig.has("adapters")) {
            result.put("message", "Missing required post_action_job_config:adapters");
            return Response.status(201).entity(result.toString()).build();
        }
        try {
            postActionAdapters = Utils.getAdapterManager().parseAdaptersListFromJobConfig(jobConfig);
        }
        catch (Exception e) {
            JSONObject ret = new JSONObject();
            ret.put("error", "Error parsing adapter list:"+e.getMessage());
            return Response.status(500).entity(ret.toString()).build();
        }
        if (postActionAdapters.size() == 0) {
            result.put("message", "Empty post_action_adapters array");
            return Response.status(201).entity(result.toString()).build();
        }
        log.info("Adapter list : "+postActionAdapters.toString());


        // If job_id is in jobConfig make sure it matches the job_id variable
        if (jobConfig.has("job_id")) {
            String jid = jobConfig.getString("job_id");
            if (!jobId.equals(jid)) {
                return Response.status(201).entity("Job ID mismatch job_id and post_action_job_confg{job_id}").build();
            }
        } else {
            // Append job_id to job_config
            jobConfig.put("job_id",jobId);
        }

        // Set missing default information in jobConfig. None of which is required
        if (!jobConfig.has("depth")) {
            int depth = LGProperties.getInteger("api.default_depth",5);
            jobConfig.put("depth",depth);
        }
        if (!jobConfig.has("ttl")) {
            int ttl = LGProperties.getInteger("api.default_ttl",0);
            jobConfig.put("ttl",ttl);
        }
        if (!jobConfig.has("priority")) {
            String priority = LGProperties.get("api.default_priority", "user_low");
            jobConfig.put("priority",priority);
        }
        if (!jobConfig.has("description")) {
            String description = LGProperties.get("api.default_description", "");
            jobConfig.put("description",description);
        }

        log.info("Execute postaction on job:"+jobId
                +" adapters: "+postActionAdapters.toString()+" nodes:"+ postActionNodes.toString());

        // Make sure the job already exists in the system
        LGJob lg = Utils.getJobManager().getJob(jobId);
        if (lg == null) {
            result.put("message", "Job with the id " + jobId + " Does not exist. postaction canceled.");
            return Response.status(201).entity(result.toString()).build();
        }

        // The new job config gets stored in the LG_INTERnAL_DATA structure
        jobConfig.put(LGConstants.LG_INTERNAL_OP,LGConstants.LG_INTERNAL_OP_EXECUTE_ON_ADAPTERS);
        JSONObject tmpData = new JSONObject();
        tmpData.put("post_action_job_config",jobConfig);
        jobConfig.put(LGConstants.LG_INTERNAL_DATA,tmpData.toString());
        LGPayload newPayload = new LGPayload(jobConfig);
        newPayload.setPayloadType(LGConstants.LG_PAYLOAD_TYPE_COMMAND);

        try {
            Utils.getSubmitToRabbit().sendPostActionCommandController(jobId, postActionAdapters, newPayload);
        }
        catch (Exception e) {
            return Response.status(201).entity(e.getMessage()).build();
        }
        // Submit was success
        result.put("message", "submitted");
        return Response.status(200).entity(result.toString()).build();
    }

    public static void main(String[] args) {
        Job job = new Job();
        for (int i = 0; i < 10; i++){
            long start = System.currentTimeMillis();
            Response res = job.jobActiveByStatus();
            System.out.println(System.currentTimeMillis() - start);
        }
    }

}

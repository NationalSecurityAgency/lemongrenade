package lemongrenade.core.api.services;

import com.mongodb.WriteResult;
import lemongrenade.core.SubmitCancelCommand;
import lemongrenade.core.SubmitJob;
import lemongrenade.core.coordinator.AdapterManager;
import lemongrenade.core.database.lemongraph.LemonGraph;
import lemongrenade.core.database.mongo.LGJobDAOImpl;
import lemongrenade.core.database.mongo.MorphiaService;
import lemongrenade.core.models.LGJob;
import lemongrenade.core.models.LGJobError;
import lemongrenade.core.models.LGJobHistory;
import lemongrenade.core.models.LGPayload;
import lemongrenade.core.util.LGConstants;
import lemongrenade.core.util.LGProperties;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

/**
 * See /docs/coordinator-api.txt for documentation
 */

@Path("/api/")
public class Job {
    private static final Logger log = LoggerFactory.getLogger(lemongrenade.core.api.services.Job.class);
    private static final MorphiaService ms = new MorphiaService();
    private static final LGJobDAOImpl dao  = new LGJobDAOImpl(LGJob.class, ms.getDatastore());;
    private static final LemonGraph lg = new LemonGraph();
    private static final AdapterManager adapterManager = new AdapterManager();
    private static final String graphStoreStr = LGProperties.get("coordinator.graphstore");
    private static final SubmitJob sj = new SubmitJob();
    private static final SubmitCancelCommand sjc = new SubmitCancelCommand();

    public Job() {
        //System.out.println("Class "+getClass().toString());
    }

    /** */
    @GET
    @Path("jobs")
    @Produces(MediaType.APPLICATION_JSON)
    public Response jobGet() {
        List<LGJob> jobs = dao.getAll();
        JSONObject ob = new JSONObject();
        for (LGJob job : jobs) {
            ob.put(job.getJobId(),job.toJson());
        }
        return Response.status(200).entity(ob.toString()).build();
    }

    /**
     * Gets all the jobs for the given status.
     * Valid status values are "NEW", PROCESSING", "FINISHED", "FINISHED_WITH_ERRORS"
     * "QUEUED", "STOPPED","EXPIRED", "RESET", "ERROR"
     * */
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
            List<LGJob> jobs = dao.getAllByStatus(status);
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
        List<LGJob> jobs = dao.getAll();
        JSONObject ob = new JSONObject();
        for (LGJob job : jobs) {
            JSONObject t = new JSONObject();
            t.put("status", job.getStatusString(job.getStatus()));
            t.put("reason", job.getReason());
            ob.put(job.getJobId(), t);
        }
        return Response.status(200).entity(ob.toString(1)).build();
    }

    /** Just gives you jobId: status for list of job ids*/
    @PUT
    @Path("jobs/status")
    @Produces(MediaType.APPLICATION_JSON)
    public Response jobActiveByStatusBulk(String body) {
        JSONArray jobs= new JSONArray();
        JSONObject jb;
        JSONObject ob;
        try {
            jb = new JSONObject(body);
            ob = new JSONObject();
            if (jb.has("jobs")) {
                jobs = jb.getJSONArray("jobs");
            }
        }
        catch (org.json.JSONException e) {
            JSONObject ret = new JSONObject();
            ret.put("error","Error Parsing JSON" + e.getMessage());
            return Response.status(500).entity(ret.toString()).build();
        }

        for (int i =0; i<jobs.length(); i++) {
            JSONObject job = jobs.getJSONObject(i);
            String jobId = job.getString("job_id");
            JSONObject t = new JSONObject();
            LGJob lg = dao.getByJobId(jobId);
            if (null == lg) {
                t.put("error","unknown");
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


    /**
     * Gets all the jobs for the given status.
     * Valid status values are "NEW", PROCESSING", "FINISHED", "QUEUED", "STOPPED","EXPIRED", "RESET", "ERROR"
     * */
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
            List<LGJob> jobs = dao.getAllByStatusAndReason(status,reason);
            JSONObject ob = new JSONObject();
            for (LGJob job : jobs) {
                ob.put(job.getJobId(), job.toJson());
            }
            return Response.status(200).entity(ob.toString(1)).build();
        }
        return Response.status(500).entity("Invalid State Query ["+status+"]").build();
    }


    /** */
    @GET
    @Path("/jobs/days/{from_days}/{to_days}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobByIdDays(@PathParam("from_days") int fdays, @PathParam("to_days") int tdays) {
        List<LGJob> jobs = dao.getAllByDays(fdays, tdays);
        if (null == lg) {
            return Response.status(404).entity("Not found").build();
        }
        JSONObject ob = new JSONObject();
        for (LGJob job : jobs) {
            ob.put(job.getJobId(),job.toJson());
        }
        return Response.status(200).entity(ob.toString()).build();
    }

    /**  */
    @GET
    @Path("/jobs/mins/{mins}/{tomins}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobByIdMins(@PathParam("mins") int mins, @PathParam("tomins") int tomins) {
        List<LGJob> jobs = dao.getAllByMins(mins,tomins);
        if (null == lg) {
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
        List<LGJob> jobs = dao.getLast(count);
        if (null == lg) {
            return Response.status(404).entity("Not found").build();
        }
        JSONObject ob = new JSONObject();
        for (LGJob job : jobs) {
            ob.put(job.getJobId(),job.toJson());
        }
        return Response.status(200).entity(ob.toString(1)).build();
    }

    /**  */
    @GET
    @Path("/jobs/age/{days}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobByIdAge(@PathParam("days") int days) {
        List<LGJob> jobs = dao.getAllByAge(days);
        if (null == lg) {
            return Response.status(404).entity("Not found").build();
        }
        JSONObject ob = new JSONObject();
        for (LGJob job : jobs) {
            ob.put(job.getJobId(),job.toJson());
        }
        return Response.status(200).entity(ob.toString()).build();
    }

    /** */
    @GET
    @Path("/job/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobById(@PathParam("id") String jobId) {
        LGJob lg = dao.getByJobId(jobId);
        if (null == lg) {
            return Response.status(404).entity("Not found").build();
        }
        return Response.status(200).entity(lg.toJson().toString(1)).build();
    }

    /** */
    @GET
    @Path("/job/{id}/full")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobByIdFull(@PathParam("id") String jobId) {
        LGJob lg = dao.getByJobId(jobId);
        if (null == lg) {
            return Response.status(404).entity("Not found").build();
        }
        JSONObject jobResult = lg.toJson();
        jobResult.put("history",getHistoryHelper(lg));
        jobResult.put("errors",getErrorsHelper(lg));
        jobResult.put("tasks",getTasksHelper(lg));
        return Response.status(200).entity(jobResult.toString(1)).build();
    }


    /** */
    @GET
    @Path("/job/{id}/status")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobStatus(@PathParam("id") String jobId) {
        LGJob lg = dao.getByJobId(jobId);
        if (null == lg) {
            return Response.status(404).entity("Not found").build();
        }
        JSONObject result = new JSONObject();
        result.put("status",lg.getStatusString(lg.getStatus()));
        result.put("reason",lg.getReason());

        return Response.status(200).entity(result.toString()).build();
    }

    /** */
    @GET
    @Path("/job/{id}/history")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobHistory(@PathParam("id") String jobId) {
        LGJob lg = dao.getByJobId(jobId);
        if (null == lg) {
            return Response.status(404).entity("Not found").build();
        }
        JSONObject historyResult = new JSONObject();
        historyResult.put("history", getHistoryHelper(lg));
        return Response.status(200).entity(historyResult.toString(1)).build();
    }

    /** */
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
        LGJob lg = dao.getByJobId(jobId);
        if (null == lg) {
            return Response.status(404).entity("Not found").build();
        }
        JSONObject result = new JSONObject();
        result.put("tasks",getTasksHelper(lg));
        return Response.status(200).entity(result.toString(1)).build();
    }

    /** */
    private JSONArray getTasksHelper(LGJob lg) {
        JSONObject result = new JSONObject();
        JSONArray s = lg.getTaskList();
        return s;
    }

    /** Gets error list for job */
    @GET
    @Path("/job/{id}/errors")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobErrors(@PathParam("id") String jobId) {
        LGJob lg = dao.getByJobId(jobId);
        if (null == lg) {
            return Response.status(404).entity("Not found").build();
        }
        JSONObject errorResult = new JSONObject();
        errorResult.put("errors",getErrorsHelper(lg));
        return Response.status(200).entity(errorResult.toString(1)).build();
    }

    /** */
    private JSONArray getErrorsHelper(LGJob lg) {
        List<LGJobError> errors = lg.getJobErrors();
        JSONArray result = new JSONArray();
        for(LGJobError l: errors) {
            JSONObject rl = l.toJson();
            result.put(rl);
        }
        return result;
    }

    /** Gets the graph from LemonGraph/DB*/
    @GET
    @Path("/job/{id}/graph")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobGraphData(@PathParam("id") String jobId) {
        //
        if (graphStoreStr.equalsIgnoreCase("lemongraph")) {
            try {
                JSONObject graph = lg.getGraph(jobId);
                return Response.status(200).entity(graph.toString()).build();
            } catch (Exception e) {
                log.error("Lookup from LemonGraph failed " + e.getMessage());
                return Response.status(404).entity("Graph Not stored in lemongraph").build();
            }
        }
        return Response.status(404).entity("Not found").build();
    }

    /** Gets the graph from LemonGraph/DB in cytoscope format */
    @GET
    @Path("/job/{id}/graph/cytoscope")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobGraphDataCytoscape(@PathParam("id") String jobId) {
        //
        if (graphStoreStr.equalsIgnoreCase("lemongraph")) {
            try {
                JSONObject graph = lg.getGraphCytoscape(jobId);
                return Response.status(200).entity(graph.toString()).build();
            } catch (Exception e) {
                log.error("Lookup from LemonGraph failed " + e.getMessage());
                return Response.status(404).entity("Graph Not stored in lemongraph").build();
            }
        }
        return Response.status(404).entity("Not found").build();
    }


    /** */
    @PUT
    @Path("/job/{id}/cancel")
    @Produces(MediaType.APPLICATION_JSON)
    public Response cancelJob(@PathParam("id") String jobId) {
        log.info("Cancel job received for "+jobId);
        JSONObject ret = cancelHelper(jobId);
        return Response.status(200).entity(ret.toString()).build();
    }

    /** */
    @PUT
    @Path("/jobs/cancel")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response cancelJobs(String body) {
        JSONObject  jb = new JSONObject(body);
        if (!jb.has("jobs")) {
            log.error("Missing 'jobs' field.");
            return Response.status(500).entity("{'error':'missing jobs field'}").build();
        }
        JSONArray jobs = jb.getJSONArray("jobs");
        JSONArray retVals = new JSONArray();
        for (int i =0; i<jobs.length(); i++) {
            JSONObject job = jobs.getJSONObject(i);
            String jobId = job.getString("job_id");
            try {
                log.info("Cancelling job : "+jobId);
                JSONObject ret = cancelHelper(jobId);
                retVals.put(ret);
            } catch (Exception e) {
                JSONObject ret = new JSONObject();
                ret.put("error","Job with the id " + jobId + " Cancel FAILED: "+e.getMessage());
                ret.put("cancelled",false);
                retVals.put(ret);
            }
        }
        return Response.status(200).entity(retVals.toString()).build();
    }

    /**
     * Used by /job/ID/cancel and /jobs/cancel
     * @param jobId the ide of the job to cancel
     * @return
     */
    private JSONObject cancelHelper(String jobId) {
        LGJob lg = dao.getByJobId(jobId);

        JSONObject ret = new JSONObject();
        ret.put("job_id",jobId);

        if (null == lg) {
            ret.put("error","job not found");
            ret.put("cancelled",false);
            ret.put("status",400);
            return ret;
        }
        int status = lg.getStatus();
        if ((status == LGJob.STATUS_NEW) || (status == LGJob.STATUS_PROCESSING) || (status == LGJob.STATUS_QUEUED)) {
            //log.info("Status eligible for Cancel");
        } else {
            ret.put("error","Can not cancel job, invalid state "+lg.getStatusString(status));
            ret.put("cancelled",false);
            ret.put("status", 200);
            return ret;
        }
        try {
            sjc.sendCancel(jobId);
        }
        catch (Exception e) {
            ret.put("error","Can not cancel job "+e.getMessage());
            ret.put("cancelled",false);
            ret.put("status",201);
            return ret;
        }

        ret.put("cancelled",true);
        ret.put("status",200);
        return ret;
    }

    /**
     * Single Job reset call
     * Optional: pass a jsonobject in the body with key of "REASON" and this will be stored with the
     *           job history.
     * Delete graph information from LEMONGRAPH. Job meta data remains in database(mongo) and can be reran in
     * the future.
     * See api documentation for more information
     */
    @PUT
    @Path("/job/{id}/reset")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response resetJob(@PathParam("id") String jobId, String body) {
        String reason = "";
        JSONObject  jb = new JSONObject(body);
        if (jb.has("reason")) {
            reason = jb.getString("reason");
        }
        log.info("Reset job received for ["+jobId+ "]  Reason ["+reason+"]");
        JSONObject ret = resetHelper(jobId,reason);
        return Response.status(200).entity(ret.toString()).build();
    }

    /**
     * Bulk Job reset call
     * Deletes graph information from LEMONGRAPH. Job meta data remains in database(mongo) and can be reran in
     * the future.
     * See api documentation for more information
     */
    @PUT
    @Path("/jobs/reset")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response resetJobs(String body) {
        JSONObject  jb = new JSONObject(body);
        if (!jb.has("jobs")) {
            log.error("Missing 'jobs' field.");
            return Response.status(500).entity("{'error':'missing jobs field'}").build();
        }
        // Note: this is the global reason. If the parser sees a reason for an individual job listing
        //       it will use that instead.
        String reason = "";
        if (jb.has(LGConstants.LG_RESET_REASON)) {
            reason = jb.getString(LGConstants.LG_RESET_REASON);
        }
        log.info("Received bulk reset command global reason ["+reason+"]");
        JSONArray jobs = jb.getJSONArray("jobs");
        JSONArray retVals = new JSONArray();
        for (int i =0; i<jobs.length(); i++) {
            JSONObject job = jobs.getJSONObject(i);
            String jobId = job.getString("job_id");
            // You can supply individual reason for a specific job, otherwise the 'global'
            String tmpReason = reason;
            if (job.has(LGConstants.LG_RESET_REASON)) {
                tmpReason = job.getString(LGConstants.LG_RESET_REASON);
            }
            try {
                log.info("Resetting job : "+jobId);
                JSONObject ret = resetHelper(jobId,tmpReason);
                retVals.put(ret);
            } catch (Exception e) {
                JSONObject ret = new JSONObject();
                ret.put("error","Job with the id " + jobId + " Reset FAILED: "+e.getMessage());
                ret.put("reset",false);
                retVals.put(ret);
            }
        }
        return Response.status(200).entity(retVals.toString()).build();
    }

    /**
     * Used by /jobs/reset and /job/reset
     *
     * @param jobId
     * @param reason Is a value supplied by the api user to say why it was reset. They can query for this value later
     * @return JSONOBject - results
     */
    private JSONObject resetHelper(String jobId, String reason) {
        LGJob lg = dao.getByJobId(jobId);

        JSONObject ret = new JSONObject();
        ret.put("job_id",jobId);
        if (null == lg) {
            ret.put("error","job not found");
            ret.put("reset",false);
            ret.put("status",400);
            return ret;
        }
        int status = lg.getStatus();
        if ((status == LGJob.STATUS_NEW) || (status == LGJob.STATUS_FINISHED) || (status == LGJob.STATUS_STOPPED)) {

        } else {
            ret.put("error","Can not reset job, invalid state "+lg.getStatusString(status));
            ret.put("reset",false);
            ret.put("status", 200);
            return ret;
        }

        try {
            sj.sendReset(jobId,reason);
        }
        catch (Exception e) {
            ret.put("error","Can not reset job "+e.getMessage());
            ret.put("reset",false);
            ret.put("status",201);
            return ret;
        }
        ret.put("reset",true);
        ret.put("status",200);
        return ret;
    }


    /**
     * Deletes from Mongo Lemongrenade and from Lemongraph
     */
    @DELETE
    @Path("/job/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteJob(@PathParam("id") String jobId) {
        log.info("API Delete Job received " + jobId);
        try {
            JSONObject ret = deleteHelper(jobId);
            int status = ret.getInt("status");
            return Response.status(status).entity(ret.toString()).build();
        } catch (Exception e) {

        }

        JSONObject ret = new JSONObject();
        ret.put("job_id",jobId);
        ret.put("error","Job with the id " + jobId + " delete FAILED for unknown reason.");
        ret.put("deleted",false);
        ret.put("status",500);
        return Response.status(500).entity(ret.toString()).build();
    }

    /**
     * Used by single and bulk delete
     */
    private JSONObject deleteHelper(String jobId) throws Exception
    {
        // If database set to lemongraph, call lemongraph delete first
        if (graphStoreStr.equalsIgnoreCase("lemongraph")) {
            try {
                lg.deleteGraph(jobId);
            } catch (Exception e) {
                log.error("Delete from LemonGraph failed " + e.getMessage());
            }
        }

        // Build return object common parts
        JSONObject ret = new JSONObject();
        ret.put("job_id",jobId);

        // Verify that the job is eligible for deletion
        LGJob lg = dao.getByJobId(jobId);
        if (null == lg) {
            ret.put("error","Job with the id " + jobId + " is not present in the database");
            ret.put("deleted",false);
            ret.put("status", 500);
            return ret;
        }
        int status = lg.getStatus();
        if ((status == LGJob.STATUS_PROCESSING) || (status == LGJob.STATUS_QUEUED)) {
            log.warn("job "+jobId+ " in bulk delete is in processing state. need to cancel it first");
            // TODO: Isssue a CANCEL_AND_DELETE_COMMAND HERE
        }

        if ((status != LGJob.STATUS_STOPPED) && (status != LGJob.STATUS_FINISHED) && (status != LGJob.STATUS_RESET)) {
            ret.put("error","Job State invalid for deletion. " + lg.getStatusString(status));
            ret.put("deleted",false);
            ret.put("status", 404);
            return ret;
        }

        // Delete from Mongo
        WriteResult wr = dao.delete(lg);
        if (wr != null) {
            ret.put("deleted",true);
            ret.put("status",200);
            return ret;
        }
        // Get this far, fail
        ret.put("error","Job with the id " + jobId + " is not present in the database");
        ret.put("deleted",false);
        ret.put("status",500);
        return ret;
    }

    /**
     * BULK Delete from Mongo Lemongrenade and from Lemongraph
     */
    @POST
    @Path("/jobs/")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response deleteJobs(String body) {
        log.info("API Delete Jobs received "+body);
        JSONObject  jb ;
        try {
            jb = new JSONObject(body);
        } catch (org.json.JSONException e) {
            JSONObject ret = new JSONObject();
            ret.put("error","Error Parsing JSON" + e.getMessage());
            return Response.status(500).entity(ret.toString()).build();
        }

        if (!jb.has("jobs")) {
            log.error("Missing 'jobs' field.");
            return Response.status(500).entity("{'error':'missing jobs key'}").build();
        }
        System.out.println("In: "+jb.get("jobs"));
        JSONArray jobs = jb.getJSONArray("jobs");

        JSONArray retVals = new JSONArray();
        for (int i =0; i<jobs.length(); i++) {
            JSONObject job = jobs.getJSONObject(i);
            String jobId = job.getString("job_id");
            try {
                log.info("Deleting job : "+jobId);
                JSONObject ret = deleteHelper(jobId);
                int status = ret.getInt("status");
                retVals.put(ret);
            } catch (Exception e) {
                JSONObject ret = new JSONObject();
                ret.put("error","Job with the id " + jobId + " FAILED: "+e.getMessage());
                ret.put("deleted",false);
                retVals.put(ret);
            }
        }
        return Response.status(200).entity(retVals.toString()).build();
    }


    /**
     * Create New Job
     */
    @POST
    @Path("/job")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createJobFromJsonNoJobId(String jobJson) {

        log.info("RAW POST REQUEST : "+jobJson);
        JSONObject job;
        try {
            job = new JSONObject(jobJson);
        } catch (org.json.JSONException e) {
            JSONObject ret = new JSONObject();
            ret.put("error","Error Parsing JSON " + e.getMessage() );
            ret.put("original_request",jobJson);
            return Response.status(500).entity(ret.toString()).build();
        }

        // Required adapterlist, seed
        ArrayList<String> approvedAdapters = new ArrayList<>();
        JSONObject seedData  = new JSONObject();

        if (job.has("seed")) {
            seedData = job.getJSONObject("seed");
        }
        JSONObject jobConfig = new JSONObject();
        if (job.has("job_config")) {
            jobConfig = job.getJSONObject("job_config");
        }

        // Parse the adapter list from the job_config
        try {
            approvedAdapters = adapterManager.parseAdaptersListFromJobConfig(jobConfig);
        }
        catch (Exception e) {
            JSONObject ret = new JSONObject();
            ret.put("error","Error parsing adapter list:"+e.getMessage());
            return Response.status(500).entity(ret.toString()).build();
        }

        log.info("Adapter list : "+approvedAdapters.toString());

        // If job_id is in jobConfig, remove it
        if (jobConfig.has("job_id")) {
            jobConfig.remove("job_id");
        }

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
        // Add Incoming Nodes
        JSONArray nodes = new JSONArray();
        if (seedData.has("nodes")) {
            nodes = seedData.getJSONArray("nodes");
        }
        for (int i = 0; i < nodes.length(); i++) {
            JSONObject node = nodes.getJSONObject(i);
            newPayload.addResponseNode(node);
        }
        // Add incoming Edges
        JSONArray edges = new JSONArray();
        if (seedData.has("edges")) {
            edges = seedData.getJSONArray("edges");
        }
        for (int i = 0; i < edges.length(); i++) {
            JSONObject edge = edges.getJSONObject(i);
            // TODO: proper support for edges required here (src, tgt, data)
            //newPayload.addResponseEdge(edge);
        }


        String jobId = "";
        try {
            jobId = sj.sendNewJobToCommandController(approvedAdapters, newPayload);
        } catch (Exception e) {
            return Response.status(201).entity(e.getMessage()).build();
        }

        // Return result in JSON
        JSONObject result = new JSONObject();
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
     * */
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
            approvedAdapters = adapterManager.parseAdaptersListFromJobConfig(jobConfig);
        }
        catch (Exception e) {
            JSONObject ret = new JSONObject();
            ret.put("error","Error parsing adapter list:"+e.getMessage());
            return Response.status(500).entity(ret.toString()).build();
        }

        log.info("Adapter list "+approvedAdapters.toString());
        if ((approvedAdapters.size() == 0) || jobData.equals("")) {
            JSONObject ret = new JSONObject();
            ret.put("error","Missing required job information, adapterlist, seed");
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
            String priority = LGProperties.get("api.default_priority","user_low");
            jobConfig.put("priority",priority);
        }
        if (!jobConfig.has("description")) {
            String description = LGProperties.get("api.default_description","");
            jobConfig.put("description",description);
        }

        log.info("Job ADD TO JOB  job_id "+jobId+"  adapterlist: "+approvedAdapters.toString()+"  seed"+jobData.toString()
                +"  job_config:"+jobConfig.toString());

        // Make sure the job already exists in the system
        LGJob lg = dao.getByJobId(jobId);
        if (null == lg) {
            JSONObject ret = new JSONObject();
            ret.put("error","Job with the id " + jobId + " Does not exist. Job add cancelled.");
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

        /**
        rrayList<String> approvedAdapters = new ArrayList<>();
        for (int i=0; i< adapterList.length(); i++) {
            approvedAdapters.add(adapterList.getString(i));
        }*/

        try {
            sj.sendAddToJobToCommandController(jobId, approvedAdapters, newPayload);
        }
        catch (Exception e) {
            return Response.status(201).entity(e.getMessage()).build();
        }

        // Return result in JSON
        JSONObject result = new JSONObject();
        result.put("status","added");
        result.put("job_id",jobId);

        return Response.status(200).entity(result.toString()).build();
    }



    /**
     *  ExecuteAdaptersOnNodes
     *
     * As the name suggests, this function will run a specific list of adapters on a specified list of graph nodes.
     *
     * oneTimeAdapterList  - This is the adapter list you want to run against these nodes in the graph.
     *                  This will be a one time run on these nodes. The results will then be posted to the graph and
     *                  all current adapterlist (the list that came with the job originally) are ran on the results.
     *                  This oneTimeAdapterList is discarded after this operation is ran. [Although, it will be stored
     *                  in the job_history.]
     *
     * job_id : is the job id you want to run.
     *
     * nodes : JSONArray of node ID's that you want to run the oneTimeAdapterList on
     *
     *
     */
    @POST
    @Path("/job/{id}/execute_adapters_on_nodes")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response executeAdaptersOnNodes(@PathParam("id") String jobId, String jobJson) {

        JSONObject job = new JSONObject(jobJson);

        // Required oneTimeAdapterList, Nodes
        JSONArray adapterList = new JSONArray();
        JSONArray jobNodes   = new JSONArray() ;
        if (job.has("onetimeadapterlist")) {
            adapterList = job.getJSONArray("onetimeadapterlist");
        }
        if (job.has("nodes")) {
            jobNodes = job.getJSONArray("nodes");
        }
        if (jobId.equals("") || (adapterList.length()==0) || (jobNodes.length()==0)) {
            return Response.status(500).entity("Missing required job information  job_id, onetimeadapterlist, nodes").build();
        }

        // Parse jobConfig
        JSONObject jobConfig = new JSONObject();
        if (job.has("job_config")) {
            jobConfig = job.getJSONObject("job_config");
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
            String priority = LGProperties.get("api.default_priority","user_low");
            jobConfig.put("priority",priority);
        }
        if (!jobConfig.has("description")) {
            String description = LGProperties.get("api.default_description","");
            jobConfig.put("description",description);
        }

        log.info("Execute adapters on onodes job_id "+jobId+"  adapterlist: "+adapterList.toString()
                +" Nodes "+jobNodes.toString()
                +"  job_config:"+jobConfig.toString());

        // Make sure the job already exists in the system
        LGJob lg = dao.getByJobId(jobId);
        if (null == lg) {
            JSONObject ret = new JSONObject();
            ret.put("error","Job with the id " + jobId + " Does not exist. Job execute on nodes cancelled.");
            return Response.status(404).entity(ret.toString()).build();
        }

        // Markup jobConfig
        // By annotating the job_config, LG coordinator will see this when passed into the coordinator queue and
        // process it as a special operation ("lg_internal_op") instead of as a normal LG result. If you look in
        // the coordinator_bolt, you'll see it look for this in the job_config and handle the processing separately.
        // Note: the coordinator will strip this information out of the job_config once the command has been processed
        // so we don't store it and accidentally re-run it later.
        jobConfig.put(LGConstants.LG_INTERNAL_OP,"execute_on_adapters");
        JSONObject tmpData = new JSONObject();
        tmpData.put("nodes",jobNodes);
        tmpData.put("onetimeadapterlist",adapterList);
        jobConfig.put(LGConstants.LG_INTERNAL_DATA,tmpData.toString());

        LGPayload newPayload = new LGPayload(jobConfig);
        newPayload.setPayloadType(LGConstants.LG_PAYLOAD_TYPE_COMMAND);

        try {
            // Notice there is NO adapter list in this function call. the node list and adapterlist to run them on
            // is stored in the job_config under lg_internal_data. See CoordinatorBolt for more information.
            sj.sendExecutOnNodesToJobToCommandController(jobId, newPayload);
        }
        catch (Exception e) {
            return Response.status(201).entity(e.getMessage()).build();
        }

        // Return result in JSON
        JSONObject result = new JSONObject();
        result.put("status","submitted");
        result.put("job_id",jobId);

        return Response.status(200).entity(result.toString()).build();
    }




}
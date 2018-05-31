package lemongrenade.api.services;

import lemongrenade.api.services.Exceptions.HttpException;
import lemongrenade.core.coordinator.AdapterManager;
import lemongrenade.core.database.mongo.MongoDBStore;
import lemongrenade.core.models.LGJob;
import lemongrenade.core.models.LGPayload;
import lemongrenade.core.util.LGConstants;
import lemongrenade.core.util.LGProperties;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.swing.text.Document;
import javax.ws.rs.*;
import javax.ws.rs.client.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

@Path("/rest")
public class Rest {
	@Context
	HttpServletRequest request;
	@Context
	HttpServletResponse response;

	private static final Logger log = LoggerFactory.getLogger(Rest.class);

	public Rest() {}

	/**
	 * Close all open connections used by Rest
	 * <p>
	 * Calls Utils.close() and throws an Exception if any error occurs while closing. Any other open connections added to
	 * Rest will be added here to be closed also.
	 *
	 */
	public static void close() {
		try {
			Utils.close();
		}
		catch(Exception e) {
			log.error("Error closing connections.");
		}
	}

	/**
	 * Accessed on /rest/info/{id}. Returns job info for an input job ID.
	 * <p>
	 * This endpoint returns a JSONObject formatted Response body keyed by the input job ID. The value is another
	 * JSONObject with keys for "reason", "maxID", "size", "created", "meta", "graph", "errors", and "status"
	 * <p>
	 * This endpoint is deprecated in favor of /rest/v2/info/{id} which updates the return format to the new standard.
	 * @param jobID This represents a job ID present in LEMONGRAPH and MongoDB
	 * @return Returns a Response with a JSONObject body containing job info.
	 */
	@Deprecated
	@GET
	@Path("/info/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response singleJobInfo(@PathParam("id") String jobID) {
		JSONObject returnObject = new JSONObject();
		try {
			JSONObject statusObject = Utils.getStatusObject(request, jobID);
			//Manually add 'task_id' to errors returned to support old UI
			if(statusObject.has("errors")) {
				JSONArray errors = statusObject.getJSONArray("errors");
				for(int i = 0; i < errors.length(); i++) {
					JSONObject error = errors.getJSONObject(i);
					if(error.has("taskId") && !error.has("task_id")) {
						String taskId = error.getString("taskId");
						error.put("task_id", taskId);
						errors.put(i, error);
					}
				}
			}
			returnObject.put(jobID, statusObject);
		}
		catch(Exception e) {
			e.printStackTrace();
			JSONObject ret = new JSONObject();
			ret.put("error", e.getMessage());
			return Response.status(Utils.INTERNAL_SERVER_ERROR).entity(ret.toString()).build();
		}
		return Response.status(Utils.OK).entity(returnObject.toString()).build();
	}

	/**
	 * Accessed on /rest/update/{id}. Updates LEMONGRAPH "meta" field for a job.
	 * Deprecated in favor of the /rest/meta PUT endpoint.
	 *
	 * @param jobId The job ID of the job in LEMONGRAPH to update.
	 * @param reqBody a JSONObject formatted string representing keys and values for fields to add/update to the job
	 * 'meta' field in LEMONGRAPH.
	 * @return Returns a Response object with an empty JSONObject
	 */
	@Deprecated
	@PUT
	@Path("/update/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response updateUser(@PathParam("id") String jobId, String reqBody) {
		try {
			LGJob lgJob = Utils.getJobManager().getJob(jobId);
			String jobStatus = lgJob != null
					? lgJob.getStatusString(lgJob.getStatus())
					: null
					;

			JSONObject bodyJSON = new JSONObject(reqBody);

			if (jobStatus == null) {
				return Response.status(Utils.NOT_FOUND).entity(jobId).build();
			}else{

				Client client = ClientBuilder.newClient();
				String contentType = request.getContentType();
				String url = Utils.graph_url + "graph/" + jobId + "/meta";
				WebTarget resourceTarget = client.target(url);

				Invocation.Builder builder = resourceTarget.request(contentType);
				Response res = builder.put(Entity.entity(reqBody, MediaType.APPLICATION_JSON));

				int statusCode = res.getStatus();
				MediaType mediaType = res.getMediaType();

				JSONObject jobs = new JSONObject();
				JSONObject resJSON = new JSONObject();

				switch(statusCode){
					case Utils.OK:
						String resBody = res.readEntity(String.class);
						if(resBody.length() == 0){
							return Response.noContent().build();
						}

						switch(mediaType.getType()){
							case MediaType.TEXT_PLAIN:
								resJSON.put("message", resBody);
								break;
							case MediaType.APPLICATION_JSON:
								resJSON = res.readEntity(JSONObject.class);
								Utils.transformJob(resJSON);
								break;
							default:
								return res;
						}
						resJSON.put("status_code", statusCode);
						resJSON.put("job_id", jobId);
						jobs.put(jobId, resJSON);
						return Response.ok(jobs.toString(), MediaType.APPLICATION_JSON).build();
					//END [case 200:]
					case Utils.NO_CONTENT:
						return Response.ok(jobs.toString(), MediaType.APPLICATION_JSON).build();
					//END [case 204:]
					default:
						log.warn("[/rest/update/" + jobId + "] Default Case...Graph response getting returned.");
						return res;
				}
			}

		}catch(Exception e){
			log.error("Error processing /rest/update/ " + jobId );
			log.error("Message: " + e.getMessage());
			e.printStackTrace();
			return Response.serverError().entity(jobId).build();
		}
	}

	/**
	 * This endpoint is for post actions. It starts new tasks on target nodes in a graph with a provided job config and
	 * adapter list.
	 * <p>
	 * postaction - Allows you to run certain adapters on specific nodes.
	 * Only required information is config, nodes, and adapters
	 *  {
	 *    "config": {
	 *        "nodes": ["11","14","15"],
	 *        "description": "job description",
	 *        "adapters": {
	 *               "PlusBang": {}
	 *        }
	 *    }
	 * }
	 *
	 * Check a graph in Lemongraph and get the "ID" field for list of valid node IDs.
	 *
	 * Provided job_config is used for nodes/adapters being posted. Additional triggered
	 * queries will use the original job_config.
	 *
	 * @param jobId This is the job ID for the job to perform a post action on.
	 * @param jobJson String representing the body of this request. This should be formatted as a JSONObject as shown
	 * in the example provided in the description
	 * @return Returns a Response with a JSONObject body of the standard job return.
	 */
	@POST
	@Path("/append/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response postAction(@PathParam("id") String jobId, String jobJson) {
		JSONObject result = new JSONObject();
		ArrayList<String> postActionAdapters;
		JSONArray postActionNodes;
		JSONObject jobConfig;
		JSONObject returnObject = new JSONObject();
		result.put("job_id", jobId);
		JSONObject job;
		log.info("Received RAW post action request:"+jobJson);

		try {//verify job is a JSONObject
			job = new JSONObject(jobJson);
		}
		catch(JSONException e) {
			return Utils.buildResponse(Utils.BAD_REQUEST, "JSONObject body is required.");
		}

		// Sanity Check input data for required
		if (job.has("config")) {
			try {
				jobConfig = job.getJSONObject("config");
			}
			catch(JSONException e) {
				String error = "parameter 'config' must be a JSONObject.";
				JSONObject standard = Utils.getStandardReturn(Utils.BAD_REQUEST, null, error);
				returnObject.put(jobId, standard);
				return Response.status(Utils.BAD_REQUEST).entity(returnObject.toString()).build();
			}
		} else {
			String error = "Missing required parameter 'config'.";
			JSONObject standard = Utils.getStandardReturn(Utils.BAD_REQUEST, null, error);
			returnObject.put(jobId, standard);
			return Response.status(Utils.BAD_REQUEST).entity(returnObject.toString()).build();
		}
		if (jobConfig.has("nodes")) {
			try {
				postActionNodes = jobConfig.getJSONArray("nodes");
			}
			catch(JSONException e) {
				String error = "Param 'config'->'nodes' must be a JSONArray of node indexes.";
				JSONObject standard = Utils.getStandardReturn(Utils.BAD_REQUEST, null, error);
				returnObject.put(jobId, standard);
				return Response.status(Utils.MULTI_STATUS).entity(returnObject.toString()).build();
			}
		} else {
			String error = "Param 'config' requires 'nodes' JSONArray of node indexes.";
			JSONObject standard = Utils.getStandardReturn(Utils.BAD_REQUEST, null, error);
			returnObject.put(jobId, standard);
			return Response.status(Utils.MULTI_STATUS).entity(returnObject.toString()).build();
		}
		if (postActionNodes.length() == 0) {
			String error = "Empty 'config'->'nodes'. Must contain integers.";
			JSONObject standard = Utils.getStandardReturn(Utils.BAD_REQUEST, null, error);
			returnObject.put(jobId, standard);
			return Response.status(Utils.MULTI_STATUS).entity(returnObject.toString()).build();
		}
		Iterator nodesIterator = postActionNodes.iterator();
		while(nodesIterator.hasNext()) {
			String node = nodesIterator.next().toString();
			int nodeIndex = 0;
			try {
				nodeIndex = Integer.parseInt(node);
			}
			catch(Exception e) {
				String error = "'config' 'nodes' array must contain numbers. Invalid node:"+node;
				JSONObject standard = Utils.getStandardReturn(Utils.BAD_REQUEST, null, error);
				returnObject.put(jobId, standard);
				return Response.status(Utils.MULTI_STATUS).entity(returnObject.toString()).build();
			}
			if(nodeIndex == 0) {
				String error = "'0' is not a valid node ID.";
				JSONObject standard = Utils.getStandardReturn(Utils.BAD_REQUEST, null, error);
				returnObject.put(jobId, standard);
				return Response.status(Utils.MULTI_STATUS).entity(returnObject.toString()).build();
			}
		}

		// Parse the adapter list from the config
		if (jobConfig.has("adapters")) {
			JSONObject adapters;
			try {
				adapters = jobConfig.getJSONObject("adapters");
			}
			catch(JSONException e) {
				String error = "Param 'config' requires 'adapters' JSONObject attribute.";
				JSONObject standard = Utils.getStandardReturn(Utils.BAD_REQUEST, null, error);
				returnObject.put(jobId, standard);
				return Response.status(Utils.MULTI_STATUS).entity(returnObject.toString()).build();
			}

			Iterator adapterIterator = adapters.keys();
			while(adapterIterator.hasNext()) {
				String adapter = adapterIterator.next().toString();
				try {
					JSONObject job_config = adapters.getJSONObject(adapter);
				}
				catch(JSONException e) {
					String error = "Param 'config' -> 'adapters' -> '"+adapter+"' must be JSONObject.";
					JSONObject standard = Utils.getStandardReturn(Utils.BAD_REQUEST, null, error);
					returnObject.put(jobId, standard);
					return Response.status(Utils.MULTI_STATUS).entity(returnObject.toString()).build();
				}
			}

		}
		else { //jobConfig doesn't have "adapters" item
			String error = "Param 'config' requires 'adapters' JSONObject attribute.";
			JSONObject standard = Utils.getStandardReturn(Utils.BAD_REQUEST, null, error);
			returnObject.put(jobId, standard);
			return Response.status(Utils.MULTI_STATUS).entity(returnObject.toString()).build();
		}

		try {
			postActionAdapters = Utils.getAdapterManager().parseAdaptersListFromJobConfig(jobConfig);
		}
		catch (Exception e) {
			String error = "Error parsing adapter list:"+e.getMessage();
			JSONObject standard = Utils.getStandardReturn(Utils.BAD_REQUEST, null, error);
			returnObject.put(jobId, standard);
			return Response.status(Utils.MULTI_STATUS).entity(returnObject.toString()).build();
		}
		if (postActionAdapters.size() == 0) {
			String error = "Empty adapters JSONObject.";
			JSONObject standard = Utils.getStandardReturn(Utils.BAD_REQUEST, null, error);
			returnObject.put(jobId, standard);
			return Response.status(Utils.MULTI_STATUS).entity(returnObject.toString()).build();
		}
		log.info("Adapter list : "+postActionAdapters.toString());


		// If job_id is in jobConfig make sure it matches the job_id variable
		if (jobConfig.has("job_id")) {
			String jid = jobConfig.getString("job_id");
			if (!jobId.equals(jid)) {
				String error = "Job ID mismatch job_id and config:"+jobId;
				JSONObject standard = Utils.getStandardReturn(Utils.BAD_REQUEST, null, error);
				returnObject.put(jobId, standard);
				return Response.status(Utils.MULTI_STATUS).entity(returnObject.toString()).build();
			}
		} else {
			jobConfig.put("job_id",jobId);// Append job_id to job_config
		}

		// Set missing default information in jobConfig. None of which is required
		if (!jobConfig.has("depth")) {
			int depth = LGProperties.getInteger("api.default_depth", 5);
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
			String error = "Job with the id " + jobId + " Does not exist. postaction canceled.";
			JSONObject standard = Utils.getStandardReturn(Utils.BAD_REQUEST, null, error);
			returnObject.put(jobId, standard);
			return Response.status(Utils.MULTI_STATUS).entity(returnObject.toString()).build();
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
			String error = e.getMessage();
			JSONObject standard = Utils.getStandardReturn(Utils.INTERNAL_SERVER_ERROR, null, error);
			returnObject.put(jobId, standard);
			return Response.status(Utils.MULTI_STATUS).entity(returnObject.toString()).build();
		}

		// Submit was success
		JSONObject standard = Utils.getStandardReturn(Utils.OK, null, "");
		returnObject.put(jobId, standard);
		return Response.status(Utils.MULTI_STATUS).entity(returnObject.toString()).build();
	}

	/**
	 * Accessed on /rest/meta/{id}. Updates LEMONGRAPH "meta" field for a job.
	 *
	 * @param jobId The job ID of the job in LEMONGRAPH to update.
	 * @param body a JSONObject formatted string representing keys and values for fields to add/update to the job
	  * 'meta' field in LEMONGRAPH. Must contain a "meta" key with a JSONObject value.
	 * @return Returns a Response object with a standard job return of a JSONObject of job data keyed by job ID.
	 */
	@PUT
	@Path("/meta/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response putMeta(@PathParam("id") String jobId, String body) {
		try {
			JSONObject jBody;
			JSONObject meta;
			String message = "Body must be JSONObject containing 'meta' key as JSONObject.";
			try {
				jBody = new JSONObject(body);
				if(!jBody.has("meta")) {
					return Utils.buildResponse(Utils.BAD_REQUEST, message);
				}
				meta = jBody.getJSONObject("meta");
			}
			catch(JSONException e) {
				return Utils.buildResponse(Utils.BAD_REQUEST, message);
			}

			JSONObject params = Utils.getRequestParameters(request, body);
			Response responseData = Utils.updateMeta(request, jobId, meta, params);
			return responseData;
		}
		catch(Exception e) {
			return Utils.buildResponse(e);
		}
	}

	/**
	 * Accessed on /rest/meta/. Updates LEMONGRAPH "meta" field for a set of job IDs.
	 *
	 * @param body a JSONObject formatted string representing keys and values for fields to add/update to the job
	 * 'meta' field in LEMONGRAPH. Must contain a "meta" key with a JSONObject value. Must contain an "ids" key
	 * with a JSONArray of job IDs to update.
	 * @return Returns a Response object with a standard job return of a JSONObject of job data keyed by job IDs.
	 */
	@PUT
	@Path("/meta")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response updateJob(String body) {
		String user = null;
		JSONObject params;
		try {
			params = Utils.getRequestParameters(request, body);
			if(params.has("user")) {
				JSONArray temp = params.getJSONArray("user");
				user = Utils.encodeOnce(temp.getString(0));
			}
			if(!params.has("meta") || Utils.isJSONObject(params.get("meta").toString())) {
				String message = "Invalid body. Expected: {\"ids\":[\"00000000-0000-0000-0000-000000000000\"], \"meta\":{\"key\":\"value\"}}";
				return Utils.buildResponse(Utils.BAD_REQUEST, message);
			}
			else if(params.has("ids")) {
				JSONArray metaArray =  params.getJSONArray("meta");
				String metaString = metaArray.getString(0);
				JSONObject meta = new JSONObject(metaString);
				Response response = Utils.updateMetas(request, meta, params);
				return response;
			}
			else {
				return Utils.buildResponse(Utils.REQUEST_TOO_LARGE, "Job IDs must be specified for bulk meta update.");
			}
		}
		catch(HttpException e) {
			return Utils.buildResponse(e);
		}
		catch(Exception e) {
			return Utils.buildResponse(Utils.INTERNAL_SERVER_ERROR, e.getMessage());
		}
	}

	/**
	 * Accessed on /rest/meta/{id}. Fetch meta data for a given jobID.
	 *
	 * @param jobId The job ID of the job in LEMONGRAPH to fetch meta data from.
	 * @return Returns a Response object with the standard job return and meta info as the "data" field.
	 */
	@POST
	@Path("/meta/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response postMeta(@PathParam("id") String jobId) {
		return getMeta(jobId);
	}

	/**
	 * Accessed on /rest/meta/{id}. Fetch meta data for a given jobID.
	 *
	 * @param jobId The job ID of the job in LEMONGRAPH to fetch meta data from.
	 * @return Returns a Response object with the standard job return and meta info as the "data" field.
	 */
	@GET
	@Path("/meta/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getMeta(@PathParam("id") String jobId) {
		JSONArray ids = new JSONArray()
				.put(jobId);
		JSONObject params = Utils.getRequestParameters(request);
		params.put("ids", ids);
		Response res = getAllMeta(params.toString());
		return res;
	}

	/**
	 * Accessed on /rest/meta/. Fetch meta data for a given jobID.
	 *
	 * @param body String formatted as a JSONObject.  If present, must contain "ids" key of JSONArray of IDs to
	 * fetch 'meta' data for. Optionally, IDs can be passed as params in the URL {@literal e.g. /?ids="id1"&ids="id2"}
	 * @return Returns a Response object with the standard JSONObject job return keyed on job IDs with meta info as the
	 * "data" field.
	 */
	@POST
	@Path("/meta")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response postAllMeta(String body) {
		return getAllMeta(body);
	}

	/**
	 * Accessed on /rest/meta/. Fetch meta data for a given jobID.
	 *
	 * @param body String formatted as a JSONObject.  If present, must contain "ids" key of JSONArray of IDs to
	 * fetch 'meta' data for. Optionally, IDs can be passed as params in the URL, {@literal e.g. /?ids="id1"&ids="id2"}
	 * @return Returns a Response object with the standard JSONObject job return keyed on job IDs with meta info as the
	 * "data" field.
	 */
	@GET
	@Path("/meta")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response getAllMeta(String body) {
		String user = null;
		JSONObject params;
		JSONObject jobs;
		try {
			int status = Utils.OK;
			params = Utils.getRequestParameters(request, body);
			Utils.addChildIDs(params);
			if(params.has("ids") && params.getJSONArray("ids").length() > 0) {
				status = Utils.MULTI_STATUS;
			}
			if(params.has("user")) {
				JSONArray temp = params.getJSONArray("user");
				user = Utils.encodeOnce(temp.getString(0));
			}
			if(params.has("ids")) {
				jobs = Utils.getLemongraphJobs(request, params.getJSONArray("ids"), user);
			}
			else {
				jobs = Utils.getLemongraphJob(request, "", user); //no need to use futures for 1 request
			}
			Iterator jobsIterator = jobs.keys();
			JSONObject metaReturns = new JSONObject();
			while(jobsIterator.hasNext()) {
				String jobId = jobsIterator.next().toString();
				JSONObject job = jobs.getJSONObject(jobId);
				JSONObject data = new JSONObject();
				if(job.has("data") && job.getJSONObject("data").has("meta")) {
					data = job.getJSONObject("data").getJSONObject("meta");
				}
				job.put("data", data);
				metaReturns.put(jobId, job);
			}
			return Utils.buildResponse(status, metaReturns);
		} catch (Exception e) {
			e.printStackTrace();
			return Utils.buildResponse(e);
		}
	}

	/**
	 * Accessed on /rest/job/{id}. Fetch job data for a given jobID.
	 * <p>
	 * Job data returned contains 'active_task_count', 'reason', 'activity', 'size', 'meta', 'id', 'created_date',
	 * 'error_count', 'status', and 'task_count'.
	 * <p>
	 * Takes optional do=errors, do=graph params to add these items to return.
	 *
	 * @param jobId The job ID of the job in LEMONGRAPH to fetch job data from.
	 * @return Returns a Response object with the standard job return and job data as the "data" field.
	 */
	@GET
	@Path("/job/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getJobById(@PathParam("id") String jobId) {
		try {
			JSONObject params = Utils.getRequestParameters(request);
			params.put("ids", new JSONArray().put(jobId));
			return Utils.fetchJobsByIds(request, params);
		} catch (Exception e) {
			String message = "Failed to fetch standard job for job_id:"+jobId+". Exception:"+e.getMessage();
			log.error(message);
			return Utils.buildResponse(Utils.INTERNAL_SERVER_ERROR, message);
		}
	}

	/**
	 * Accessed on /rest/job/. Fetches job data for a given set of job IDs.
	 * <p>
	 * Job data returned contains 'active_task_count', 'reason', 'activity', 'size', 'meta', 'id', 'created_date',
	 * 'error_count', 'status', and 'task_count'.
	 * <p>
	 * Provide "ids" params for specified IDs in JSONobject body under key "ids" with JSONArray of IDs. These may also
	 * be provided as arguments in the URL, {@literal e.g. /?ids="id1"&ids="id2"}
	 * <p>
	 * Accepts optional do=error and do=graph params.
	 *
	 * @return Returns a Response object with the standard job return and job data as the "data" field.
	 */
	@GET
	@Path("/jobs")
	@Produces(MediaType.APPLICATION_JSON)
	public Response postJobsByIds() { //body should be {ids:[],...}
		Long startTime = Utils.startTime("/rest/jobs");
		JSONObject params = Utils.getRequestParameters(request);
		Response response = Utils.fetchJobsByIds(request, params);
		Utils.duration(startTime, "/rest/jobs");
		return response;
	}

	/**
	 * Accessed on /rest/job/. Fetches job data for a given set of job IDs.
	 * <p>
	 * Job data returned contains 'active_task_count', 'reason', 'activity', 'size', 'meta', 'id', 'created_date',
	 * 'error_count', 'status', and 'task_count'.
	 * <p>
	 * Provide "ids" params for specified IDs in JSONobject body under key "ids" with JSONArray of IDs. These may also
	 * be provided as arguments in the URL, {@literal e.g. /?ids="id1"&ids="id2"}
	 * <p>
	 * Accepts optional do=error and do=graph, and do=delete params.
	 *
	 * @return Returns a Response object with the standard job return and job data as the "data" field.
	 */
	@POST
	@Path("/jobs")
	@Produces(MediaType.APPLICATION_JSON)
	public Response postJobsByIds(String body) { //body should be {"ids":["id1","id2"], "status":""}
		try {
			JSONObject params = Utils.getRequestParameters(request, body);
			if(Utils.deleteCheck(params)) {return Utils.doDelete(params);}//if param do=delete, instead perform a delete
			Response response = Utils.fetchJobsByIds(request, params);
			return response;
		} catch (HttpException e) {
			return Response.status(e.getStatus()).entity(e.getMessage()).build();
		} catch(Exception e) {
			e.printStackTrace();
			return Utils.buildResponse(Utils.INTERNAL_SERVER_ERROR, e.getMessage());
		}
	}

	/**
	 * Accessed on /rest/reset/. Fetches job data for a given set of job IDs.
	 * <p>
	 * Requires "ids" param. Provide "ids" params for specified IDs in JSONobject body under key "ids" with JSONArray
	 * of IDs. These may also be provided as arguments in the URL, {@literal e.g. /?ids="id1"&ids="id2"}
	 * <p>
	 * If do=overwrite reset reason can be overwritten. Optional "reason" param.
	 *
	 * @param body Accepts a JSONObject with key "ids" for IDs to be reset. IDs can be input via URL params instead.
	 * @return Returns a Response object with the standard job return and reset results as the "data" field.
	 */
	@POST
	@Path("/reset")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response reset(String body) {
		try {
			JSONObject params = Utils.getRequestParameters(request, body);
			Utils.addChildIDs(params);
			JSONArray ids;
			if(params.has("ids")) {
				ids = params.getJSONArray("ids");
				if(ids.length() == 0) {return Utils.buildResponse(Utils.MULTI_STATUS, new JSONObject());}//shortcut empty ID array
				String reason = "";
				boolean overwrite = false;
				if(params.has("reason") && params.getJSONArray("reason").length() > 0) {
					reason = params.getJSONArray("reason").get(0).toString();
				}
				if(params.has("do")) {
					JSONArray doValues = params.getJSONArray("do");
					HashSet set = Utils.getHashSet(doValues);
					if (set.contains("overwrite")) {
						overwrite = true;
					}
				}
				JSONObject returnObject = Utils.resetHelper(ids, reason, overwrite);
				return Utils.buildResponse(Utils.MULTI_STATUS, returnObject);
			}
			return Utils.buildResponse(Utils.BAD_REQUEST, "No job IDs provided. E.g. {\"ids\":[\"id1\", \"id2\", ...]}");
		} catch (Exception e) {
			log.error("Error processing job reset. "+e.getMessage());
			e.printStackTrace();
			return Utils.buildResponse(Utils.INTERNAL_SERVER_ERROR, e.getMessage());
		}
	}

	/**
	 * Provides a PYTHON script to be ran on LEMONGRAPH. This is an advanced feature.
	 *
	 * @param body This should be a Python script to be ran on LEMONGRAPH
	 * @return Returns a Response forwarded from LEMONGRAPH with the result of the request.
	 */
	@POST
	@Path("/exec")
	@Consumes("application/python")
	@Produces(MediaType.APPLICATION_JSON)
	public Response exec(String body) {
		Response response = Utils.lemongraphProxy(request, "graph/exec", body);
		return response;
	}

	@GET
	@Path("/jobs/reset")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getResetJobIds() {
		try {
			Set<String> ids = MongoDBStore.getResetJobs();
			return Utils.buildResponse(Utils.OK, Utils.toJson(ids).toString());
		}
		catch(Exception e) {
			return Utils.buildResponse(Utils.INTERNAL_SERVER_ERROR, e.getMessage());
		}
	}

	@GET
	@Path("/jobs/expired")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getExpireJobIds() {
		try {
			Set<String> ids = MongoDBStore.getExpiredJobs();
			return Utils.buildResponse(Utils.OK, Utils.toJson(ids).toString());
		}
		catch(Exception e) {
			return Utils.buildResponse(Utils.INTERNAL_SERVER_ERROR, e.getMessage());
		}
	}

	@DELETE
	@Path("/jobs/reset")
	@Produces(MediaType.APPLICATION_JSON)
	public Response resetExpiredJobIds() {
		try {
			Set<String> ids = MongoDBStore.getResetJobs();
			if(ids.size() == 0) {
				return Utils.buildResponse(Utils.ACCEPTED, new JSONObject().toString());
			}

			JSONObject returnObject = Utils.resetHelper(Utils.toJson(ids), "", false);
			return Utils.buildResponse(Utils.MULTI_STATUS, returnObject.toString());
		}
		catch(Exception e) {
			return Utils.buildResponse(Utils.INTERNAL_SERVER_ERROR, e.getMessage());
		}
	}

	@DELETE
	@Path("/jobs/expired")
	@Produces(MediaType.APPLICATION_JSON)
	public Response deleteExpireJobIds() {
		try {
			Set<String> ids = MongoDBStore.getExpiredJobs();
			Iterator iterator = ids.iterator();
			if(ids.size() == 0) {
				return Utils.buildResponse(Utils.ACCEPTED, new JSONObject().toString());
			}

			JSONObject params = Utils.getRequestParameters(request);
			JSONObject responses = new JSONObject();
			//TODO: make delete async
			while (iterator.hasNext()) {
				String id = iterator.next().toString();
				params.put("ids", new JSONArray().put(id));
				try {
					Utils.deleteHelper(params);
					responses.put(id, Utils.getStandardReturn(Utils.OK, null, ""));
				}
				catch(Exception e) {
					JSONObject standard = Utils.getStandardReturn(Utils.BAD_REQUEST, null, e.getMessage());
					responses.put(id, standard);
				}
			}

			return Utils.buildResponse(Utils.MULTI_STATUS, responses.toString());
		}
		catch(Exception e) {
			return Utils.buildResponse(Utils.INTERNAL_SERVER_ERROR, e.getMessage());
		}
	}
}

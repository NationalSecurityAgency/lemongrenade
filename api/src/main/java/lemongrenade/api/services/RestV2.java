package lemongrenade.api.services;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.Iterator;

@Path("/rest/v2")
public class RestV2 {
	@Context
	HttpServletRequest request;
	@Context
	HttpServletResponse response;
	final Utils UTILS = new Utils();

	private static final Logger log = LoggerFactory.getLogger(RestV2.class);

	public RestV2() {}

	/**
	 * Accessed on /rest/v2/info/{id}. Returns job info for an input job ID.
	 * <p>
	 * This endpoint returns a JSONObject formatted Response body keyed by the input job ID. Info items returned
	 * include "reason", "maxID", "size", "created", "meta", "graph", "errors", and "status".
	 * <p>
	 * @param jobID This represents a job ID present in LEMONGRAPH and MongoDB
	 * @return Returns a Response with the standard job JSONObject body containing job info.
	 */
	@GET
	@Path("/info/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response singleJobInfo(@PathParam("id") String jobID) {
		JSONObject returnObject = new JSONObject();
		JSONObject standard;
		Integer status;
		try {
			JSONObject data = UTILS.getStatusObject(request, jobID);
			String statusString = data.getString("status");
			if(statusString.equals("404")) { //error case
				status = 404;
				String error = "Unable to fetch job from MongoDB.";
				standard = Utils.getStandardReturn(404, null, error);
			}
			else { //working case
				status = 200;
				standard = Utils.getStandardReturn(200, data, new JSONArray());
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			String message = e.getMessage();
			status = 500;
			standard = Utils.getStandardReturn(500, null, message);
		}
		returnObject.put(jobID, standard);
		return Response.status(status).entity(returnObject.toString()).build();
	}

	/**
	 * Test endpoint at /rest/v2/test. Returns 200 and the body provided to the endpoint
	 * @param body Accepts any body and replays it in the Response.
	 * @return Always returns status 200 and the same body as provided.
	 */
	@POST
	@Path("/test")
	@Produces(MediaType.APPLICATION_JSON)
	public Response postTest(String body) {
		return Utils.buildResponse(200, body.toString());
	}

	/**
	 * Sets a job to STOPPED and then deletes it. Endpoint available at /rest/v2/delete/{id}
	 * @return Returns a Response object with body as standard job return format containing result of delete request.
	 */
	@DELETE
	@Path("/delete/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response delete(@PathParam("id") String jobId) throws Exception {
		JSONObject params = Utils.getRequestParameters(request);
		params.put("ids", new JSONArray().put(jobId));
		Utils.addChildIDs(params);
		params.append("do", "delete");
		return Utils.doDelete(params);
	}

	/**
	 * Sets a job to STOPPED and then deletes it. Endpoint available at /rest/v2/delete/{id}
	 * @return Returns a Response object with body as standard job return format containing result of delete request.
	 */
	@DELETE
	@Path("/delete_user_jobs/{user}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response userDelete(@PathParam("user") String user) throws Exception {
		log.info("Deleting all jobs for user:"+user);
		String userEnc = URLEncoder.encode(user);
		Response graphResponse = Utils.lemongraphProxy(request, "GET", "graph/?role=owner&user=" + userEnc, "");
		int status = graphResponse.getStatus();
		if(status != 200) {
			return Utils.buildResponse(status, "Invalid return from LEMONGRAPH.");
		}
		String body = graphResponse.readEntity(String.class);
		JSONArray jobs = new JSONArray(body);
		if(jobs.length() == 0) {
			return Utils.buildResponse(new Exception("No jobs returned from LEMONGRAPH owned by user:"+user));
		}
		log.info("Fetched "+jobs.length()+" jobs for user:"+user+" to delete.");
		HashSet ids = new HashSet();
		Iterator iterator = jobs.iterator();
		while(iterator.hasNext()) {
			JSONObject job = (JSONObject) iterator.next();
			String id = job.getString("graph");
			ids.add(id);
		}
		JSONObject params = Utils.getRequestParameters(request);
		if(ids.size() == 0) {
			Utils.buildResponse(new Exception("No job IDs found for delete."));
		}
		params.put("ids", Utils.toJson(ids));
		params.append("do", "delete");
		params.append("children", "false"); //no need to check for children since all jobs are being deleted.
		log.info("Starting user jobs delete for user:"+user);
		return Utils.doDelete(params);
	}

	/**
	 * Sets a job to STOPPED . Endpoint available at /rest/v2/stop/{id}
	 * @return Returns a Response object with body as standard job return format containing result of stop request.
	 */
	@PUT
	@Path("/stop/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response stop(@PathParam("id") String jobId) {
		JSONObject returnObject = new JSONObject();
		Integer status;
		String error = null;
		try {
			JSONObject params = Utils.getRequestParameters(request);
			params.put("ids", new JSONArray(jobId));
			JSONObject item = UTILS.cancelHelper(params);
			status = item.getInt("status");
			if(item.has("error"))
				error = item.get("error").toString();
		} catch (Exception e) {
			status = 500;
			error = e.getMessage();
			e.printStackTrace();
		}
		JSONObject standard = Utils.getStandardReturn(status, null, error);
		returnObject.put(jobId, standard);
		return Utils.buildResponse(status, returnObject.toString());
	}

	public static void main(final String[] args) {}

}

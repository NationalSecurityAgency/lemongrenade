package lemongrenade.api.services;

import lemongrenade.core.coordinator.AdapterManager;
import lemongrenade.core.database.mongo.LGAdapterURLsDAO;
import lemongrenade.core.database.mongo.LGAdapterURLsDAOImpl;
import lemongrenade.core.database.mongo.MorphiaService;
import lemongrenade.core.models.LGAdapterModel;
import lemongrenade.core.models.LGAdapterURLs;
import lemongrenade.core.util.LGProperties;
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

/**
 * See /docs/coordinator-api.txt for documentation
 */
@Path("/api/")
public class Adapter {
    @Context
    HttpServletRequest request;
    @Context
    HttpServletResponse response;
    @Context
    ServletContext context;

    private final Logger log = LoggerFactory.getLogger(getClass());
    private static final AdapterManager adapterManager = new AdapterManager();
    private static final MorphiaService ms = new MorphiaService();
    private static final LGAdapterURLsDAO lgAdapterURLsDAO = new LGAdapterURLsDAOImpl(LGAdapterURLs.class,ms.getDatastore());

    public Adapter() {
    }

    /**
     * adapter should have been /adapters.  This is deprecated and will be removed in future release
     * See GetAdapters below for new call
     */
    @GET
    @Path("adapter")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAdapter() {
        return getAdapters();
    }


    @GET
    @Path("adapters")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAdapters() {
        JSONObject adapters = adapterManager.getAdapterListJson();

        // We switched RabbitMQ to HTTPS and can't get it to work
        String doRabbitMqMetrics = LGProperties.get("rabbit.gatheradminpetrics", "false");
        if (doRabbitMqMetrics.equalsIgnoreCase("true")) {
            JSONArray ret = getMessageQueueInformation();
            for (int i = 0; i < ret.length(); i++) {
                JSONObject queueData = ret.getJSONObject(i);
                String fullname = queueData.getString("name");
                if (adapters.has(fullname)) {
                    JSONObject jo = adapters.getJSONObject(fullname);
                    jo.append("queue", queueData);
                    adapters.put(fullname, jo);
                }
            }
        }
        return Response.status(200).entity(adapters.toString(1)).build();
    }


    /**
     * Returns a clean list of available adapter names available without UUID markup.
     * If there's more than one type of adapter available, the name is only returned once,
     * since the end user does not care about how many adapters are available only what
     * ones are available.
     *
     * For example, if there exists:
     *    0000-0000-0000-0000-00001-Adapter1 , 0000-0000-0000-0000-00002-Adapter1, 0000-0000-0000-0000-00003-Adapter2
     *
     * This function will return
     *     JSONArray("Adapter1", "Adapter2")
     *
     *     Note /adapter/names is DEPRECATED, use /adapters/names instead
     * */
    @GET
    @Path("adapter/names")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAdapterNames() {
        JSONArray adapters = adapterManager.getAdapterListNamesOnlyJson();
        return Response.status(200).entity(adapters.toString()).build();
    }
    @GET
    @Path("adapters/names")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAdaptersNames() {
        JSONArray adapters = adapterManager.getAdapterListNamesOnlyJson();
        return Response.status(200).entity(adapters.toString()).build();
    }


    /** Doesn't work with SSL admin rabbitmq */
    @GET
    @Path("adapters/queues")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAdapterQueues() {
        String doRabbitMqMetrics = LGProperties.get("rabbit.gatheradminpetrics", "false");
        JSONArray ret = new JSONArray();
        if (doRabbitMqMetrics.equalsIgnoreCase("true")) {
            ret =getMessageQueueInformation();
        } else {
            ret.put(new JSONObject().put("error","metrics_disabled"));
        }
        return Response.status(200).entity(ret.toString()).build();
    }




    /** Helper Method */
    private JSONArray getMessageQueueInformation() {
        JSONArray retArray = new JSONArray();
        /* COMMENT OUT UnTIL WE CAN FIX THIS
        // Query RabbitMQ for Message states
        String rabbitHost = LGProperties.get("rabbit.hostname", "localhost");
        int rabbitPort = LGProperties.getInteger("rabbit.adminport", 15672);
        String rabbitUser = LGProperties.get("rabbit.adminuser", "guest");
        String rabbitPass = LGProperties.get("rabbit.adminpassword", "guest");

        try {
            String creds = new String(rabbitUser + ":" + rabbitPass);
            byte[] credbytes = creds.getBytes();
            byte[] base64bytes = Base64.encodeBase64(credbytes);
            String base64creds = new String(base64bytes);
            String authString = new String("Basic " + base64creds);
            Request request = client.newRequest(rabbitHost, rabbitPort);
            request.path("/api/queues/");
            request.header(HttpHeader.AUTHORIZATION, authString);
            request.method(HttpMethod.GET);
            ContentResponse res = request.send();

            // Parse results
            JSONArray astatus = new JSONArray(res.getContentAsString());
            if (astatus.length() > 0) {
                for (int i = 0; i < astatus.length(); i++) {
                    JSONObject adata = astatus.getJSONObject(i);
                    JSONObject q = new JSONObject();
                    q.put("name",           adata.getString("name"));
                    q.put("messages_ready", adata.getInt("messages_ready"));
                    q.put("messages_rate",  adata.getJSONObject("messages_details").getInt("rate"));
                    q.put("idle_since",     adata.getString("idle_since"));
                    retArray.put(q);
                }
            } else {
                log.error("Unable to get information from RabbitMQ admin server");
            }
        } catch (Exception e) {
            log.error("Unable to contact RabbitMQ admin port: " + e.getMessage());
        }
        */
        return retArray;
    }


    /** */
    @GET
    @Path("/adapter/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getHTMLData(@PathParam("id") String id) {
        JSONObject adapters = adapterManager.getAdapterListJson();
        JSONObject adapter;
        try {
            adapter = (JSONObject) adapters.get(id);
            if (null == adapter) {
                return Response.status(Utils.NOT_FOUND).entity("Not found").build();
            }
        }
        catch (Exception e) {
            return Response.status(Utils.NOT_FOUND).entity("Not found").build();
        }

        return Response.status(Utils.OK).entity(adapter.toString(1)).build();
    }

    /** */
    @GET
    @Path("/adapterBaseURL/{adapterName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getBaseURLbyAdapterName(@PathParam("adapterName") String adapterName) {

        LGAdapterURLs lgAdapterURLs = null;
        try {
            lgAdapterURLs = lgAdapterURLsDAO.getLGAdapterURLByAdapter(adapterName);

            if (null == lgAdapterURLs) {
                return Response.status(Utils.NOT_FOUND).entity("Not found").build();
            }
        } catch (Exception e) {
            return Response.status(Utils.NOT_FOUND).entity("Not found").build();
        }

        return Response.status(Utils.OK).entity(lgAdapterURLs.getBaseUrl()).build();
    }


    /** */
    @GET
    @Path("/adapterBaseURL/allBaseURLs")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllBaseURLs() {

        JSONObject lgAdapterURLs = null;
        try {
            lgAdapterURLs = lgAdapterURLsDAO.getAllJSON();

            if (null == lgAdapterURLs) {
                return Response.status(Utils.NOT_FOUND).entity("Not found").build();
            }
        } catch (Exception e) {
            return Response.status(Utils.NOT_FOUND).entity("Not found").build();
        }

        return Response.status(Utils.OK).entity(lgAdapterURLs.toString(1)).build();
    }

    @PUT
    @Path("/adapterBaseURL/{adapterName}/")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response upDateBaseURL(@PathParam("adapterName") String adapterName, String body) {
        JSONObject ret = new JSONObject();
        try {
            lgAdapterURLsDAO.saveNewBaseURL(adapterName, body);

            ret.put("NEW_url",true);
        } catch (Exception e) {
            ret = new JSONObject();
            ret.put("NEW_url","Failed: "+e);
            return Response.status(Utils.NOT_FOUND).entity(ret.toString()).build();
        }

        return Response.status(Utils.OK).entity(ret.toString()).build();
    }


    @PUT
    @Path("/adapterBaseURL/saveMultipleBaseURLs/")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response upDateMultipleBaseURLs(String body) {
        JSONObject ret = new JSONObject();
        try {
            lgAdapterURLsDAO.saveNewMultipleBaseURLs(body);

            ret.put("NEW_url",true);
        } catch (Exception e) {
            ret = new JSONObject();
            ret.put("NEW_url","Failed: "+e);
            return Response.status(Utils.NOT_FOUND).entity(ret.toString()).build();
        }

        return Response.status(Utils.OK).entity(ret.toString()).build();
    }

    /** */
    @DELETE
    @Path("/adapter/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteAdapterById(@PathParam("id") String id) {
        LGAdapterModel adapter;
        try {
            adapter = adapterManager.getAdapterById(id);
            if (null == adapter) {
                return Response.status(Utils.NOT_FOUND).entity("Adapter Not Found").build();
            }
        }
        catch (Exception e) {
            return Response.status(Utils.NOT_FOUND).entity("Not found").build();
        }

        adapterManager.deleteAdapter(adapter);

        JSONObject ret = new JSONObject();
        ret.put("adapter_id",id);
        ret.put("deleted",true);
        return Response.status(Utils.OK).entity(ret.toString()).build();


    }

    /** */
    public static void main(String args[]) {
        Adapter a = new Adapter();
        JSONArray ja = a.getMessageQueueInformation();
        System.out.println(ja.toString());
    }

}
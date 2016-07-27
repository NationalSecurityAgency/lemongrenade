package lemongrenade.core.api.services;

import lemongrenade.core.coordinator.AdapterManager;
import lemongrenade.core.util.LGProperties;
import org.apache.storm.shade.org.apache.commons.codec.binary.Base64;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.http.HttpHeader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * See /docs/coordinator-api.txt for documentation
 */

@Path("/api/")
public class Adapter {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private AdapterManager am;
    private HttpClient client;

    public Adapter() {
        am = new AdapterManager();
        try {
            client = new HttpClient();
            client.setConnectBlocking(false);
            client.start();
        }
        catch (Exception e) {
            log.error("Unable to http client (used to contact RabbitMQ admin)");
        }

    }

    /** */
    @GET
    @Path("adapter")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAdapters() {
        JSONObject adapters = am.getAdapterListJson();

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
        return Response.status(200).entity(adapters.toString()).build();
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
     * */
    @GET
    @Path("adapter/names")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAdapterNames() {
        JSONArray adapters = am.getAdapterListNamesOnlyJson();
        return Response.status(200).entity(adapters.toString()).build();
    }


    /** Doesn't work with SSL admin rabbitmq */
    @GET
    @Path("adapter/queues")
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
        return retArray;
    }


    /** */
    @GET
    @Path("/adapter/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getHTMLData(@PathParam("id") String id) {
        JSONObject adapters = am.getAdapterListJson();
        JSONObject adapter;
        try {
            adapter = (JSONObject) adapters.get(id);
            if (null == adapter) {
                return Response.status(404).entity("Not found").build();
            }
        }
        catch (Exception e) {
            return Response.status(404).entity("Not found").build();
        }

        return Response.status(200).entity(adapter.toString()).build();
    }

    /** */
    public static void main(String args[]) {
        Adapter a = new Adapter();
        JSONArray ja = a.getMessageQueueInformation();
        System.out.println(ja.toString());
    }

}
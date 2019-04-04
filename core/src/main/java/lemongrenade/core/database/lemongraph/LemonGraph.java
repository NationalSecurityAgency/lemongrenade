package lemongrenade.core.database.lemongraph;

import lemongrenade.core.models.LGJob;
import lemongrenade.core.models.LGPayload;
import lemongrenade.core.util.LGConstants;
import lemongrenade.core.util.LGProperties;
import lemongrenade.core.util.RequestResult;
import lemongrenade.core.util.Requests;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Handles communication layer to LemonGraph
 *
 * * {
 *   "nodes":[],
 *   "meta":{"job_id":"b202cca8-f03a-11e5-8ba5-000000000000", priority:255, enabled:0/1},
 *   "edges":[]
 * }
 *
 * boolean isConnected()
 * boolean isApiUp()   - Performs actual ping
 * JSONObject getGraph(String jobId)
 * JSONObject queryBasedOnPattern(String jobId, String adapterPattern, int lastId)
 * JSONObject deleteGraph(String graphId)
 * LemonGraphResponse createGraph()
 * LemonGraphResponse postToGraph(String graphId, LemonGraphObject graph)
 *
 */
public class LemonGraph {
    public final static int NOT_FOUND = 404;
    private final static Logger log = LoggerFactory.getLogger(LemonGraph.class);
    private final static int RETRY_MAX_ATTEMPT = 5;
    private final static int RETRY_SLEEP_TIME  = 5000;
    public static String restUrl = LGProperties.get("lemongraph_url");
    public static HttpClient client;

    static {
        if (!restUrl.endsWith("/")) {
            restUrl += "/";
        }
    }

    public LemonGraph() {
        openConnection();
    }

    //Opens a connection if one isn't already open
    public static void openConnection() {
        if(!isConnected()) {
            try {
                client = new HttpClient();
                client.setConnectBlocking(false);
                client.start();
            } catch (Exception e) {
                log.error("Error:" + e.getLocalizedMessage());
                log.error("Unable to open client connection to " + restUrl);
                e.printStackTrace();
            }
        }
    }

    public static boolean isConnected() {
        if(client == null) {
            return false;
        }
        return client.isStarted();
    }

    /**
     *  Attempts to gracefully close API connection
     */
    public static void close() {
        try {
            if(client != null)
                client.stop();
        }
        catch (Exception e) {
            log.error("Unable to close client connection to "+restUrl);
            // TODO: What to do here.
        }
    }

    /**
     * Returns true if the LemonGraph server is up and running. If it's not, it attempts
     * to reconnect.. Sleeping RETRY_SLEEP_TIME between RETRY_MAX_ATTEMPT.  Use this
     * before you start doing lemongraph operations.
     */
    private static boolean testConnection() {
        if (!isConnected()) {
            String connectLG = LGProperties.get("lemongraph_url");
            int count = 0;
            while (!isConnected() && count <= RETRY_MAX_ATTEMPT) {
                count++;
                isApiUp();
                if (isConnected()) {
                    log.info("Lemongraph is online.");
                } else {
                    try {
                        log.warn("Sleeping "+count+" of "+RETRY_MAX_ATTEMPT+" between reconnect");
                        Thread.sleep(RETRY_SLEEP_TIME);
                    }
                    catch(InterruptedException e) {
                        log.warn("Sleep interrupted : "+e.getMessage());
                    }
                }
            }
            if (count >= RETRY_MAX_ATTEMPT) {
                log.error("Lemongraph is NOT connected :"+connectLG);
            }
        }
        return true;
    }

    /**
     * This is a cheat because /ping doesn't yet exist in Lemongraph. Here we just check something
     * is listening on the restUrl port.
     *
     * @return boolean  up or down
     */
    public static boolean isApiUp() {
        openConnection();
        try {
            String url = restUrl+"ping/";
            RequestResult result = Requests.get(url);
            int status = result.status_code;
            log.info("Ping Response ["+status+"]");
        }
        catch (Exception e) {
            log.error("Caught exception in ping " + e.getMessage());
            return false;
        }

        // Send 'ping'
        return true;
    }

    /**
     * Takes multiple patterns and places them into a single LemonGraph query
     *
     * @param jobId    - job/graph to parse
     * @param adapterPatterns  Mash map of "pattern","adapter" to send to LemongRPha
     * @param startId  - this is the starting id (usually currentGraphId)
     * @return JSONObject of items
     */
    public static JSONObject queryBasedOnPatterns(String jobId, HashMap<String, String> adapterPatterns, int startId) {
        JSONObject ret = new JSONObject();
        StringBuilder params = new StringBuilder();
        adapterPatterns.forEach((adapterId, adapterQuery) -> {
            try {
                String encodedAdapterPattern = URLEncoder.encode(adapterQuery, "UTF-8");
                if (params.length() == 0) {
                    params.append("q=");
                } else {
                    params.append("&q=");
                }
                params.append(encodedAdapterPattern);
            } catch (UnsupportedEncodingException e) {
                // TODO: throw exception?
                log.error("Unable to URL Encode exception: " + e.getMessage());
            }
        });

        String url = restUrl + "graph/" + jobId + "?" + params.toString() + "&start=" + startId;
        try {
            openConnection();
            RequestResult result = Requests.get(url);
            int status = result.status_code;
            if ((status == 400) || (status == 404)) {
                log.error("Received error code communicating to lemongraph ("+status+")");
                return ret;
            }
            try {
                String fixJson = "{ \"data\":"+ result.response_msg + "}";
                ret = new JSONObject(fixJson);
            }
            catch (JSONException e) {
                log.error("Caught JSON parse exception: "+e.getMessage());
            }
        }
        catch (Exception e) {
            log.error("Exception trying to communicate to lemongraph: "+e.getMessage());
            log.info("LEMONGRAPH Query Url:" + url);
        }
        return ret;
    }

    /**
     * Takes multiple patterns and places them into a single LemonGraph query between startId and stopId
     *
     * @param jobId    - job/graph to parse
     * @param adapterPatterns  Mash map of "pattern","adapter" to send to LemonGraph
     * @param startId  - this is the starting id (usually currentGraphId)
     * @param stopId    - this is the starting id (usually currentGraphId)
     * @return JSONObject
     */
    public static JSONObject queryBasedOnPatterns(String jobId, HashMap<String, String> adapterPatterns, int startId, int stopId) {
        JSONObject ret = new JSONObject();
        StringBuilder params = new StringBuilder();
        adapterPatterns.forEach((adapterId,adapterQuery) -> {
            try {
                String encodedAdapterPattern = URLEncoder.encode(adapterQuery, "UTF-8");
                if (params.length() == 0) {
                    params.append("q=");
                } else {
                    params.append("&q=");
                }
                params.append(encodedAdapterPattern);
            } catch (UnsupportedEncodingException e) {
                // TODO: throw exception?
                log.error("Unable to URL Encode exception: " + e.getMessage());
            }
        });

        String url = restUrl + "graph/" + jobId + "?" + params.toString() + "&start=" + startId + "&stop=" + stopId;
        try {
            openConnection();
            RequestResult result = Requests.get(url);
            int status = result.status_code;
            if ((status == 400) || (status == 404)) {
                log.error("Received error code communicating to lemongraph ("+status+")");
                return ret;
            }
            try {
                String fixJson = "{ \"data\":"+ result.response_msg + "}";
                ret = new JSONObject(fixJson);
            }
            catch (JSONException e) {
                log.error("Caught JSON parse exception: "+e.getMessage());
            }
        }
        catch (Exception e) {
            log.error("Exception trying to communicate to lemongraph: "+e.getMessage());
            log.info("LEMONGRAPH Query Url:" + url);
        }
        return ret;
    }

    /**
     * Takes multiple patterns and places them into a single LemonGraph query.
     * This method does not take a starting or Ending id, and d oes not append start= to query
     * @param jobId    - job/graph to parse
     * @param adapterPatterns  Mash map of "attern","adapter" to send to LemongRPha
     * @return JSONObject
     */
    public static JSONObject queryBasedOnPatterns(String jobId, HashMap<String, String> adapterPatterns) {
        JSONObject ret = new JSONObject();
        StringBuilder params = new StringBuilder();
        adapterPatterns.forEach((adapterId,adapterQuery) -> {
            try {
                String encodedAdapterPattern = URLEncoder.encode(adapterQuery, "UTF-8");
                if (params.length() == 0) {
                    params.append("q=");
                } else {
                    params.append("&q=");
                }
                params.append(encodedAdapterPattern);
            } catch (UnsupportedEncodingException e) {
                // TODO: throw exception?
                log.error("Unable to URL Encode exception: " + e.getMessage());
            }
        });

        if (params.length() <= 0) {
            log.info("No params for query. jobid:"+jobId);
            String fixJson = "{ \"data\": [] }";
            ret = new JSONObject(fixJson);
            return ret;
        }

        String url = restUrl + "graph/" + jobId + "?" + params.toString();
        try {
            openConnection();
            RequestResult result = Requests.get(url);
            int status = result.status_code;
            if ((status == 400) || (status == 404)) {
                log.error("Received error code communicating to lemongraph ("+status+")");
                return ret;
            }
            try {
                String fixJson = "{ \"data\":"+ result.response_msg + "}";
                ret = new JSONObject(fixJson);
            }
            catch (JSONException e) {
                log.error("Caught JSON parse exception: "+e.getMessage());
            }
        }
        catch (Exception e) {
            log.error("Exception trying to communicate to lemongraph: "+e.getMessage());
            log.info("LEMONGRAPH Query Url:"+url);
        }
        return ret;
    }


    /**
     * This method takes results back from LemonGraph and builds a hashmap that is easy to process and send
     * to appropriate adapters. It also does a fair amount of error checking on the data coming back to make
     * sure it's in the expected format.
     *
     * Results come back from LemonGraph in this format:
     * [
     *   ["query1","query2", "query3"],
     *   [query_index, [node_result1, node_result2, ...]
     *    query_index, [node_result1, node_result2, ...]
     *    query_index, [node_result1, node_result2, ...]
     *   ]
     *  ]
     *
     * We takes these results and look at each result query and see what adapters match that query and build
     * a hash map of:
     *     (query, list_of_results_to_send_for_that_query)
     *
     * If an adapter matches multiple queries, all the results are combined into a single entry for that adapterId
     * For example, if adapter1 matched query1 and query2, the hashmap would have all those results in a single
     * line of hashmap for that adapter.
     *
     * @param resultdata Json data that's returned from a LemonGraph query  {'data': 'lemongraph_raw_result'}
     * @return Hashmap A map of {query, all_results_for_that_query}
     */
    public static HashMap parseLemonGraphResult(JSONObject resultdata) {
        HashMap<String, JSONArray> parseMap = new HashMap<String, JSONArray>();

        // If no data field, something went wrong
        if (!resultdata.has("data")) {
            log.error("ParseLemonGraphResult received invalid data format from LemonGraph. Ignoring!");
            return parseMap;
        }

        // Make sure data is an JSONArray
        Object testType = resultdata.get("data");
        if (!(testType instanceof JSONArray)) {
            log.error("ParseLemonGraphResult received invalid data format from LemonGraph. Data was not JSONArray!");
            log.error(testType.toString());
            return parseMap;
        }

        JSONArray data = resultdata.getJSONArray("data");
        if (data.length() <=  0) {
            log.error("Data field empty in parseLemonGraphResult "+resultdata.toString());
            return parseMap;
        }

        // Look for "queries" index. This is the first part of the result from LemonGraph that lists all the
        // queries that ran and have results. We use this List as an index when parsing the results section.
        JSONArray queries;
        try {
            queries = data.getJSONArray(0);
        }
        catch(Exception e) {
            log.error("Missing queries index "+data.toString());
            return parseMap;
        }

        // For each element in the results array, we parse the result entry, look up the query index and find
        // which adapters match that query. All results are "appended" to the adapter results (if there are multiple
        // matching results for an adapter)
        int count = 0;
        for (Object o : data) {
            if (o instanceof JSONArray) {
                JSONArray result = (JSONArray) o;
                // If count is 0, skip the Queries section - we already parsed that above
                if (count >= 1) {
                    int index = result.getInt(0);
                    JSONArray nodes = result.getJSONArray(1);
                    String query = queries.getString(index);
                    JSONArray tmpNodes;
                    if (!parseMap.containsKey(query)) {
                       tmpNodes = new JSONArray();
                    }  else {
                        tmpNodes = parseMap.get(query);
                    }
                    // Append nodes (from incoming) to the query parse map
                    for(int x=0; x < nodes.length(); x++) {
                        tmpNodes.put(nodes.getJSONObject(x));
                    }
                    parseMap.put(query, tmpNodes);
                }
            }
            count++;
        }
        return parseMap;
    }

    /**
     * Reset tells LemonGraph to remove all 'learned' data from adapters from the graph but keeps the
     * initial seed data so the job can be RERAN by a user.
     *
     * @param graphId String for graph ID
     * @return JSONObject
     * @throws InvalidGraphException thrown when failing to call reset in LemonGraph
     */
    public static JSONObject resetGraph(String graphId) throws InvalidGraphException {
        JSONObject ret = new JSONObject();
        try {
            openConnection();
            ContentResponse res = client.newRequest(restUrl + "reset/" + graphId)
                    .method(HttpMethod.PUT)
                    .send();
            int status = res.getStatus();
            ret.put("status", status);
            ret.put("message", res.getContentAsString());
        }
        catch (Exception e) {
            log.error("Caught exception in reset Graph "+e.getMessage());
            throw new InvalidGraphException(e.getMessage());
        }
        return ret;
    }

    /**
     * Deletes the entire graph from the LemonGraph database.
     *
     * @param graphId String of graph ID
     * @return JSONObject
     * @throws InvalidGraphException thrown when failing to hit LemonGraph graph/
     */
    public static JSONObject deleteGraph(String graphId) throws InvalidGraphException {
        JSONObject ret = new JSONObject();
        try {
            openConnection();
            ContentResponse res = client.newRequest(restUrl+"graph/"+graphId)
                 .method(HttpMethod.DELETE)
                 .agent("Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:17.0) Gecko/20100101 Firefox/17.0")
                 .send();
            int status = res.getStatus();
            ret.put("status", status);
            ret.put("message", res.getContentAsString());
            return ret;
        }
        catch (Exception e) {
            log.error("Caught exception in deleteGraph "+e.getMessage());
            throw new InvalidGraphException(e.getMessage());
        }
    }

    public static String createGraph(JSONObject meta) throws Exception {
        return createGraph(null, meta);
    }

    /**
     * Looks to see if the graph exists in LemonGraph, If not, it creates it.
     * @param meta is the 'meta' data to store in the graph at create time
     * @return Job UUID as defined by LEMONGRAPH
     * @throws Exception when failing to connect to LEMONGRAPH or failing to hit createGraph
     */
    public static String createGraph(String graphId, JSONObject meta) throws Exception {
        if(graphId != null && graphId.length() > 0 && graphId.length() != LGJob.JOB_ID_LENGTH) {
            throw new Exception("Invalid length for graph ID:"+graphId);
        }
        LemonGraphResponse lgr;
        if (!testConnection()) {
            log.error("Lemongraph is not connected... Error");
            throw new Exception("Unable to connect to LEMONGRAPH. Create Failed.");
        }
        lgr = createGraphHelper(graphId, meta);
        if (!lgr.getSuccess()) {
            throw new Exception("LEMONGRAPH API call to createGraph Failed!!!");
        }
        return lgr.getJobId();
    }

    /**
     *  Lemongraph creates the graph and assign a UUID itself-
     *  LemonGrenade no longer assigns a jobId itself.
     *
     * @param meta posts the meta data to the graph at create time
     * @return LemonGraphResponse  the result that LemonGraph returns
     */
    private static LemonGraphResponse createGraphHelper(String graphId, JSONObject meta) {
        JSONObject metaWrapper = new JSONObject();
        metaWrapper.put("meta",meta);
        metaWrapper.put("seed",true);
        LemonGraphResponse lgr = new LemonGraphResponse();
        ContentResponse res = null;
        try {
            openConnection();
            Request req;
            if(graphId != null && graphId.length() > 0) { //ensure this is a 'real' graphId
                req = client.POST(restUrl + "graph/" + graphId + "?create=true");
            }
            else {
                req = client.POST(restUrl + "graph/");
            }
            req.content(new StringContentProvider(metaWrapper.toString()), "application/json");
            res = req.send();
            lgr.parseContentResponse(res);
        }
        catch (Exception e) {
            log.error("Error trying to postToGraph "+e.getMessage());
            lgr.setSuccess(false);
        }
        return lgr;
    }

    /**
     * @param graphId String for graph ID
     * @param graph LemonGraphObject
     * @return LemonGraphResponse
     * @throws InvalidGraphException thrown when failing to connect and hit graph/
     */
    public static LemonGraphResponse postToGraph(String graphId, LemonGraphObject graph) throws InvalidGraphException {
        LemonGraphResponse lgr = new LemonGraphResponse();
        ContentResponse res = null;
        try {
            openConnection();
            String url = restUrl + "graph/" + graphId;
            Request req = client.POST(url);
            JSONObject graphContent  = graph.get();
            req.content(new StringContentProvider(graphContent.toString()), "application/json");
            res = req.send();
            lgr.parseContentResponse(res);   // TODO: check responseCode to be 204?
        }
        catch (Exception e) {
            log.error("Error trying to postToGraph "+e.getMessage());
            lgr.setSuccess(false);
        }
        return lgr;
    }

    /**
     * Adds the response nodes/edges from LGPayload into LEMONGRAPH.
     * @param payload LGPayload
     * @return Returns a LemonGraphResponse
     */
    public static LemonGraphResponse postToGraph(LGPayload payload) throws InvalidGraphException {
        LemonGraphObject lgo = buildLemonGraphFromPayloadNodesandEdges(payload.getResponseNodes(),
                payload.getResponseEdges());
        if (payload.getPayloadType().equals(LGConstants.LG_PAYLOAD_TYPE_COMMAND)) {lgo.setSeed(true);}
        return postToGraph(payload.getJobId(), lgo);
    }

    public static String getContent(String uri) throws InvalidGraphException {
            DefaultHttpClient client = new DefaultHttpClient();
            HttpGet httpGet = new HttpGet(uri);
            HttpResponse response;
        try {
            response = client.execute(httpGet);
            StatusLine statusLine = response.getStatusLine();
            int status = statusLine.getStatusCode();
            StringBuilder builder;
            HttpEntity entity = response.getEntity();
            InputStream content = entity.getContent();
            builder = new StringBuilder();
            BufferedReader reader = new BufferedReader(new InputStreamReader(content));
            String line = reader.readLine();
            while(line != null) {
                builder.append(line);
                line = reader.readLine();
            }
            String body = builder.toString();
            if(status == 400 || status == 404) {
                client.close();
                throw new InvalidGraphException(body);
            }
            client.close();
            return body;
        } catch (IOException e) {
            e.printStackTrace();
            client.close();
            throw new InvalidGraphException(e.getMessage());
        }
    }

    public static JSONObject getGraph(String jobId) throws InvalidGraphException {
        JSONObject ret;
        String uri = restUrl + "graph/" + jobId;
        String body = getContent(uri);
        ret = new JSONObject(body);
        return ret;
    }

    public static JSONObject getGraphCytoscape(String jobId) throws InvalidGraphException {
        JSONObject ret;
        String uri = restUrl + "cytoscape/"+jobId;
        String body = getContent(uri);
        ret = new JSONObject(body);
        return ret;
    }

    /**
     * @param jobId String for job ID
     * @return JSONObject
     * @throws InvalidGraphException thrown when failing to get content
     */
    public static JSONObject getGraphD3(String jobId) throws InvalidGraphException {
        JSONObject ret;
        String uri = restUrl + "d3/"+jobId;
        String body = getContent(uri);
        ret = new JSONObject(body);
        return ret;
    }

    /**
     * @param jobId String for job ID
     * @param start int
     * @param stop int
     * @return JSONObject
     * @throws InvalidGraphException thrown when failing to get content.
     */
    public static JSONObject getGraphD3(String jobId, int start, int stop) throws InvalidGraphException {
        JSONObject ret;
        String uri = restUrl + "d3/"+jobId+"?start="+start+"&stop="+stop;
        String body = getContent(uri);
        ret = new JSONObject(body);
        return ret;
    }

    /**
     * Build a Lemongraph object based on a payloads response nodes and edges
     * @param inNodes List of JSONObject of input nodes
     * @param inEdges List of JSONArray for input edges
     * @return LemonGraphObject
     */
    public static LemonGraphObject buildLemonGraphFromPayloadNodesandEdges(List<JSONObject> inNodes, List<JSONArray> inEdges) {
        JSONArray newNodes = new JSONArray();
        JSONArray newChains= new JSONArray();

        for (JSONObject payloadNode : inNodes) { newNodes.put(payloadNode);  }

        // Submit adapter writer output as chains
        for (JSONArray payloadEdge: inEdges)   { newChains.put(payloadEdge); }

        // Meta - is the job_config and is only set on the initial post/create graph
        JSONObject meta = new JSONObject();

        // Post to graph
        return (new LemonGraphObject(false, meta, newNodes, null, newChains));
    }

    /**
     * @param args Unused. Standard main args.
     */
    public static void main(String[] args) {
        LemonGraph lg = new LemonGraph();
        boolean isUp = lg.isApiUp();

        int stressCount = 100;   // Set to 1 if you only one to do 1 test
        int threadCount = 10;

        if (!isUp) {
            System.out.println("   LemonGraph is DOWN");
            System.exit(0);
        }

        String jobId = "";
        try {
            JSONObject meta = new JSONObject();
            meta.put("somemeta",3);
            jobId = lg.createGraph(meta);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.exit(0);
        }

        ArrayList<StressThread> threads = new ArrayList<StressThread>();
        long startTime = System.currentTimeMillis();
        for(int c= 1; c<= threadCount; c++) {
            String name = "Thread #"+c;
            StressThread temp= new StressThread(name, stressCount);
            threads.add(temp);
            temp.start();
        }

        // Wait for threads to complete
        for (StressThread thread : threads) {
            try {
                thread.join();
            }
            catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }
        }

        long endTime = System.currentTimeMillis();
        long total = (endTime - startTime)/1000;
        System.out.println("Runtime seconds  : "+total);
        System.out.println("Threads          : "+threadCount);
        System.out.println("Cycles per thread: "+stressCount);
        System.out.println("Done");

        // Delete Graph
        try {
            lg.deleteGraph(jobId);
        } catch (Exception e) {
            System.out.println("Delete failed message: "+e.getMessage());
        }
    }
}



package lemongrenade.core.database.lemongraph;

import lemongrenade.core.util.LGProperties;
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
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Handles communication layer to LemonGraph
 *
 * * {
 *   "nodes":[],
 *   "meta":{"job_id":"b202cca8-f03a-11e5-8ba5-a088b48dbc68", priority:255, enabled:0/1},
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
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final static int RETRY_MAX_ATTEMPT = 5;
    private final static int RETRY_SLEEP_TIME  = 5000;
    private String restUrl;
    private HttpClient client;
    private boolean connected = false;

    public LemonGraph() {
        restUrl = LGProperties.get("lemongraph_url");
        if (!restUrl.endsWith("/")) {
            restUrl += "/";
        }
        try {
            client = new HttpClient();
            client.setConnectBlocking(false);
            client.start();
        }
        catch (Exception e) {
            log.error("Unable to open client connection to "+restUrl);
        }
    }

    public boolean isConnected() { return connected; }

    /**
     *  Attempts to gracefully close API connection
     */
    public void close() {
        try {
            client.stop();
        }
        catch (Exception e) {
            log.error("Unable to close client connection to "+restUrl);
            // TODO: What to do here.
        }
    }

    /**
     * returns true if the LemonGraph server is up and running. If it's not, it attempts
     * to reconnect.. Sleeping RETRY_SLEEP_TIME between RETRY_MAX_ATTEMPT.  Use this
     * before you start doing lemongraph operations.
     */
    private boolean testConnection() {
        if (!isConnected()) {
            String connectLG = LGProperties.get("lemongraph_url");
            int count = 0;
            while (!isConnected() && count <= RETRY_MAX_ATTEMPT) {
                count++;
                isApiUp();
                if (isConnected()) {
                    log.info("Lemongraph is online.");
                    connected = true;
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
    public boolean isApiUp() {
        try {
            ContentResponse res = client.newRequest(restUrl+"ping/")
                    .method(HttpMethod.GET)
                    .send();
            int status = res.getStatus();
            log.info("Ping Response ["+status+"]");
            connected = true;
        }
        catch (Exception e) {
            log.error("Caught exception in ping "+e.getMessage());
            connected = false;
            return false;
        }

        // Send 'ping'
        return true;
    }


    /** takes multiple patterns and places them into a single query */
    public JSONObject queryBasedOnPatterns(String jobId, HashMap<String,String> adapterPatterns, int lastId) {
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

        String url = restUrl + "graph/" + jobId + "?" + params.toString() + "&start=" + lastId;
        try {
            ContentResponse res = client.GET(url);
            int status = res.getStatus();
            if ((status == 400) || (status == 404)) {
                log.error("Received error code communicating to lemongraph ("+status+")");
                return ret;
            }
            try {
                String fixJson = "{ \"data\":"+ res.getContentAsString() + "}";
                ret = new JSONObject(fixJson);
            }
            catch (JSONException e) {
                log.error("Caught JSON parse exception: "+e.getMessage());
            }
        }
        catch (Exception e) {
            log.error("Exception trying to communicate to lemongraph "+e.getMessage());
        }
        return ret;
    }

    /** Does not append start= to query */
    public JSONObject queryBasedOnPatterns(String jobId, HashMap<String,String> adapterPatterns) {
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
        // chatty log.info("Lemongraph URL = "+ URLDecoder.decode(url));
        try {
            ContentResponse res = client.GET(url);
            int status = res.getStatus();
            if ((status == 400) || (status == 404)) {
                log.error("Received error code communicating to lemongraph ("+status+")");
                return ret;
            }
            try {
                String fixJson = "{ \"data\":"+ res.getContentAsString() + "}";
                ret = new JSONObject(fixJson);
            }
            catch (JSONException e) {
                log.error("Caught JSON parse exception: "+e.getMessage());
            }
        }
        catch (Exception e) {
            log.error("Exception trying to communicate to lemongraph "+e.getMessage());
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
     * */
    public  HashMap parseLemonGraphResult(JSONObject resultdata) {
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
        JSONArray queries = new JSONArray();
        if (data.get(0) instanceof JSONArray) {
            queries = data.getJSONArray(0);
        } else {
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

    public JSONObject deleteGraph(String graphId)
    throws InvalidGraphException
    {
        JSONObject ret = new JSONObject();
        try {
            ContentResponse res = client.newRequest(restUrl+"graph/"+graphId)
                                                 .method(HttpMethod.DELETE)
                                                 .agent("Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:17.0) Gecko/20100101 Firefox/17.0")
                                                 .send();
            //int status = res.getStatus();
        }
        catch (Exception e) {
            log.error("Caught exception in deleteGraph "+e.getMessage());
            throw new InvalidGraphException(e.getMessage());
        }
        return ret;

    }

    /**
     * Looks to see if the graph exists in LemonGraph, If not, it creates it.
     * @param meta is the 'meta' data to store in the graph at create time
     * @return Job UUID as defined by LEMONGRAPH
     **/
    public String createGraph(JSONObject meta)
    throws Exception{
        LemonGraphResponse lgr;
        if (!testConnection()) {
            log.error("Lemongraph is not connected... Error");
            throw new Exception("Unable to connect to LEMONGRAPH. Create Failed.");
        }
        lgr = createGraphHelper(meta);
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
     * */
    private LemonGraphResponse createGraphHelper(JSONObject meta) {
        JSONObject metaWrapper = new JSONObject();
        metaWrapper.put("meta",meta);
        metaWrapper.put("seed",true);
        LemonGraphResponse lgr = new LemonGraphResponse();
        ContentResponse res = null;
        try {
            Request req = client.POST(restUrl + "graph/");
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

    /** */
    public LemonGraphResponse postToGraph(String graphId, LemonGraphObject graph)
    throws InvalidGraphException {

        LemonGraphResponse lgr = new LemonGraphResponse();
        ContentResponse res = null;
        try {
            Request req = client.POST(restUrl + "graph/"+graphId);
            req.content(new StringContentProvider(graph.get().toString()), "application/json");
            res = req.send();
            lgr.parseContentResponse(res);   // TODO: check responseCode to be 204?
        }
        catch (Exception e) {
            log.error("Error trying to postToGraph "+e.getMessage());
            lgr.setSuccess(false);
        }
        return lgr;
    }

    public JSONObject getGraph(String jobId)
    throws InvalidGraphException {
        JSONObject ret = new JSONObject();

        try {
            ContentResponse res = client.GET(restUrl + "graph/"+jobId);
            int status = res.getStatus();
            if ((status == 400) || (status == 404)) {
                throw new InvalidGraphException(res.getContentAsString());
            }
            ret = new JSONObject(res.getContentAsString());
        }
        catch (Exception e) {
            throw new InvalidGraphException(e.getMessage());
        }
        return ret;
    }

    public JSONObject getGraphCytoscape(String jobId)
            throws InvalidGraphException {
        JSONObject ret = new JSONObject();
        try {
            ContentResponse res = client.GET(restUrl + "cytoscape/"+jobId);
            int status = res.getStatus();
            if ((status == 400) || (status == 404)) {
                throw new InvalidGraphException(res.getContentAsString());
            }
            ret = new JSONObject(res.getContentAsString());
        }
        catch (Exception e) {
            throw new InvalidGraphException(e.getMessage());
        }
        return ret;
    }

    /** Build a Lemongraph object based on a payloads response nodes and edges */
    public LemonGraphObject buildLemonGraphFromPayloadNodesandEdges(List<JSONObject> inNodes, List<JSONArray> inEdges) {
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
     *
     * @param args
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
    }
}



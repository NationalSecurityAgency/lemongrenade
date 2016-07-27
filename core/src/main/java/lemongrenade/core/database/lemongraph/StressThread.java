package lemongrenade.core.database.lemongraph;

import org.json.JSONArray;
import org.json.JSONObject;
import java.util.HashMap;
import java.util.Map;

 class StressThread extends Thread {
    int runCount = 1;
    public StressThread (String s, int runCount) {
        super(s);
        this.runCount = runCount;
    }

    public void run() {
        LemonGraph lg = new LemonGraph();
        System.out.println("Thread "+getName()+" Starting ");
        for (int c=1; c<= runCount; c++) {
            doTest(lg, c);
        }
        System.out.println("Thread "+getName()+" Complete");
    }

    public void doTest(LemonGraph lg, int c) {
        String jobId ="";
        try {
            JSONObject meta = new JSONObject();
            jobId =lg.createGraph(meta);

        } catch (Exception e) {
            System.out.println("  Create GRAPH FAILED!!! Aborting....");
            System.exit(1);
        }
        System.out.println("Create job "+jobId);
        String graphId = jobId;
        System.out.println("["+c+"] Output from createNewGraph(create) graphId:" + graphId);

        // Build graph
        JSONObject node = new JSONObject();
        node.put("type", "foo");
        node.put("value", "bar");

        JSONObject node2 = new JSONObject();
        node2.put("type", "foo");
        node2.put("value", "baz");

        JSONArray nodes = new JSONArray();
        nodes.put(node);
        nodes.put(node2);

        JSONObject meta = new JSONObject();
        JSONArray edges = new JSONArray();

        JSONObject edge1 = new JSONObject();
        // Can use id
        edge1.put("type", "edge");
        edge1.put("value", "e1");
        JSONObject e1src = new JSONObject();
        e1src.put("type", "foo");
        e1src.put("value", "bar");
        JSONObject e1tgt = new JSONObject();
        e1tgt.put("type", "foo");
        e1tgt.put("value", "baz");
        edge1.put("src", e1src);
        edge1.put("tgt", e1tgt);
        edges.put(edge1);

        LemonGraphObject lgo = new LemonGraphObject(true, meta, nodes, edges);
        LemonGraphResponse lgr2 = null;
        try {
            lgr2 = lg.postToGraph(graphId, lgo);
        } catch (InvalidGraphException e) {
            System.out.println("Error can't lookup graph id " + graphId);
        }
        if (lgr2.didCallSucceed() == false) {
            System.out.println("  POST GRAPH DATA FAILED!!! Aborting....");
            System.out.println("    " + lgr2.toString());
            System.exit(1);
        }

        // ------------------------------------------------------------------------------------------
        // Emulate Adapters pattern
        int lastCount = (lgr2.getMaxId() - lgr2.getUpdateCount()) + 1;
        String adapterPattern = "n(value~/baz/i)";
        HashMap <String,String> adapterPatterns = new HashMap<String,String>();
        adapterPatterns.put("001",adapterPattern);
        JSONObject newgraph = lg.queryBasedOnPatterns(graphId, adapterPatterns, lastCount);
        JSONArray d= newgraph.getJSONArray("data");

        HashMap<String, JSONArray> resultMap = lg.parseLemonGraphResult(newgraph);
        int id = 0;
        for (Map.Entry<String, JSONArray> entry : resultMap.entrySet()) {
            String query = entry.getKey();
            JSONArray nodes2 = entry.getValue();
            JSONObject d3 = (JSONObject) nodes2.get(0);
            id = d3.getInt("ID");
        }

        // Send that data to the 'adapter'
        // Post more data to the graph
        // Add a property to Node ID 4
        JSONObject node1 = new JSONObject();
        node1.put("ID", id);
        JSONObject prop1 = new JSONObject();
        prop1.put("prop1", "fooprop");
        JSONArray props = new JSONArray();
        props.put(prop1);
        node1.put("properties", props);
        JSONArray nodes2 = new JSONArray();
        nodes2.put(node1);

        // Post changes to graph
        LemonGraphObject lgo2 = new LemonGraphObject(true, meta, nodes2, edges);
        try {
            LemonGraphResponse lgr3 = lg.postToGraph(graphId, lgo2);
        } catch (InvalidGraphException e) {
            System.out.println("Unable to post changes to graph " + graphId + " aborting...");
            System.out.println(lgr2.toString());
            System.exit(1);
        }

        // Pull the graph again and display?
        /*
        JSONObject results4 = new JSONObject();
        try {
            results4 = lg.getGraph(graphId);
        } catch (InvalidGraphException e) {
            System.out.println("Error can't lookup graph id " + graphId+"  "+e.getMessage());
            System.out.println("Aborting...");
            System.exit(1);
        }*/

        // Delete Graph
        try {
            lg.deleteGraph(graphId);
        } catch (Exception e) {
            System.out.println("Delete failed message: "+e.getMessage());
        }
       // try {
            //Thread.sleep(1);
        //}
       // catch (InterruptedException e) {}

    }
}

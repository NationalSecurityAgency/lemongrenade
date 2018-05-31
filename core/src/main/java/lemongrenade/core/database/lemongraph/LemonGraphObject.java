package lemongrenade.core.database.lemongraph;

import org.json.JSONArray;
import org.json.JSONObject;
/**
 * 	(Content-Type: application/json, application/x-msgpack)
 * body should be object w/ the optional kv pairs below
 * {
 *     "seed": true,  // mark any referenced nodes/edges as seeds
 *     "meta": {},    // same structure as POST /graph, merges graph-level kv pairs
 *     'chains': [],  // list of node[to edge to node]* object chains
 *     'nodes': [],   // list of nodes
 *     'edges': [],   // list of edges
 * }
 */
public class LemonGraphObject {
    private boolean isSeed;
    private JSONObject meta;
    private JSONArray nodes;
    private JSONArray edges;
    private JSONArray chains;

    public JSONObject get() {
        JSONObject graph = new JSONObject();
        graph.put("nodes", nodes);
        graph.put("edges", edges);
        graph.put("seed",  isSeed);
        graph.put("meta",  meta);
        graph.put("chains", chains);
        return graph;
    }

    public JSONArray getNodes() { return nodes; }
    public void setNodes(JSONArray nodes) { this.nodes = nodes; }
    public JSONArray getEdges() { return edges; }
    public void setEdges(JSONArray edges) { this.edges = edges; }
    public JSONArray getChains() { return chains; }
    public void setChains(JSONArray chains) { this.edges = chains; }
    public JSONObject getMeta() { return meta; }
    public void setMeta(JSONObject meta) { this.meta = meta; }
    public void setSeed(boolean seed) { this.isSeed = seed;}
    public boolean getSeed() { return this.isSeed; }

    public LemonGraphObject(boolean isSeed, JSONObject meta, JSONArray nodes, JSONArray edges) {
        this.isSeed = isSeed;
        this.meta   = meta;
        this.nodes  = nodes;
        this.edges  = edges;
    }

    /**
     * Constructor that takes in chains
     * @param isSeed boolean. 'true' for a seed
     * @param meta JSONObject of meta data items
     * @param nodes JSONArray of JSONObject nodes
     * @param edges JSONArray of JSONArray triplets of JSONObjects
     * @param chains JSONArray
     */
    public LemonGraphObject(boolean isSeed, JSONObject meta, JSONArray nodes, JSONArray edges, JSONArray chains) {
        this.isSeed = isSeed;
        this.meta   = meta;
        this.nodes  = nodes;
        this.edges  = edges;
        this.chains = chains;
    }
}
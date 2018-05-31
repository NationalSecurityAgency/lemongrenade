package lemongrenade.core.models;

import lemongrenade.core.util.LGConstants;
import org.apache.storm.shade.org.apache.commons.lang.SerializationUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LGPayload implements Serializable {

    private String jobId;
    private String taskId;
    private JSONObject requests;
    private JSONObject responses;
    private JSONObject jobConfig; //Settings for this entire job, including lemongrenade.adapters to limit the run to.
    private String payloadType;
    private static final Logger log = LoggerFactory.getLogger(LGPayload.class);

    public JSONObject getRequests() {
        return requests;
    }

    public JSONObject getResponses() {
        return responses;
    }

    public static JSONObject empty_nodes_edges() { //returns default response/request object
        JSONObject response =  new JSONObject().put("nodes", new JSONArray()).put("edges", new JSONArray());
        return response;
    }

    //create LGPayload with no args
    public LGPayload() {
        //this(UUID.randomUUID().toString(), "", new JSONObject());//calls LGPayload(String jobId)
        this("", "", new JSONObject());//calls LGPayload(String jobId)
    }

    public LGPayload(String job_id) {//Create LGPayload using just the job_i
        this(job_id, "", new JSONObject());
    }

    public LGPayload(JSONObject jobConfig) {
        String jobId = "";
        if (jobConfig.has("job_id")) {
            jobId = jobConfig.getString("job_id");
        }
        initialize(jobId, "", jobConfig, LGPayload.empty_nodes_edges());
    }

    public LGPayload(String jobId, String taskId, JSONObject jobConfig) {
        initialize(jobId, taskId, jobConfig, LGPayload.empty_nodes_edges());
    }

    public LGPayload(String jobId, String taskId, JSONObject jobConfig, JSONObject responses) {
        initialize(jobId, taskId, jobConfig, responses);
    }

    private void initialize (String jobId, String taskId, JSONObject jobConfig, JSONObject responses) {
        this.jobId = jobId;
        this.taskId = taskId;
        this.responses = responses;
        this.payloadType    = LGConstants.LG_PAYLOAD_TYPE_ADAPTERRESPONSE; // always adapter response by default
        this.requests = LGPayload.empty_nodes_edges();
        this.setJobConfig(jobConfig);
    }

    public static LGPayload deserialize(byte[] serialized) {
        return (LGPayload) SerializationUtils.deserialize(serialized);
    }

    public List<JSONObject> getRequestNodes() {
        List<JSONObject> obj = new ArrayList<>();
        this.requests.getJSONArray("nodes").forEach(element -> obj.add((JSONObject) element));
        return obj;
    }

    public List<JSONArray> getRequestEdges() {
        List<JSONArray> obj = new ArrayList<>();
        this.requests.getJSONArray("edges").forEach(element -> obj.add((JSONArray) element));
        return obj;
    }

    public void addResponseNodes(JSONArray nodes) {
        Iterator iterator = nodes.iterator();
        while(iterator.hasNext()) {
            JSONObject node = (JSONObject) iterator.next();
            addResponseNode(node);
        }
    }

    public List<JSONObject> getResponseNodes(){
        List<JSONObject> obj = new ArrayList<>();
        this.responses.getJSONArray("nodes").forEach(element -> obj.add((JSONObject) element));
        return obj;
    }

    public void updateResponseNodes (int index, JSONObject node) {
        this.responses.getJSONArray("nodes").put(index, node);
    }

    public List<JSONArray> getResponseEdges(){
        List<JSONArray> obj = new ArrayList<>();
        this.responses.getJSONArray("edges").forEach(element -> obj.add((JSONArray) element));
        return obj;
    }

    public void addRequestNodes(JSONArray nodes)    {
        Iterator iterator = nodes.iterator();
        while(iterator.hasNext()) {
            JSONObject node = (JSONObject) iterator.next();
            addRequestNode(node);
        }
    }

    //Creates and returns a clone of the input JSONObject
    public JSONObject newJSONObject(JSONObject input) {
        return new JSONObject(input.toString());
    }

    public void addRequestNode(JSONObject node){
        this.requests.getJSONArray("nodes").put(newJSONObject(node));
    }

    public void addRequestEdge(JSONObject sourceNode, JSONObject edge, JSONObject targetNode){
        this.requests.getJSONArray("edges")
                .put(new JSONArray()
                        .put(newJSONObject(sourceNode))
                        .put(newJSONObject(edge))
                        .put(newJSONObject(targetNode)))
        ;
    }

    public void addResponseNode(JSONObject node){
        this.responses.getJSONArray("nodes")
                .put(newJSONObject(node));
    }

    public void addResponseEdge(JSONObject sourceMetadata, JSONObject edgeMetadata, JSONObject targetMetadata){
        this.responses.getJSONArray("edges")
                .put(new JSONArray()
                        .put(newJSONObject(sourceMetadata))
                        .put(newJSONObject(edgeMetadata))
                        .put(newJSONObject(targetMetadata)))
                ;
    }

    // We need to allow setting of jobId because LemonGraph is now the generator of jobId values
    // and we need a way to update it
    public void setJobId(String jobId) {
        this.jobId = jobId;
        if (jobConfig.has("job_id")) {
            jobConfig.put("job_id",jobId);
        }
    }

    public String getJobId(){
        return this.jobId;
    }

    public String getTaskId() { return this.taskId; }

    public void setTaskId(String taskId) { this.taskId = taskId; }

    public void delTaskId() { this.setTaskId("");}

    public String getPayloadType() { return this.payloadType; }

    public void setPayloadType(String payloadType) {
        this.payloadType = payloadType;
    }

    //writes the Long for # of nodes then writes that many JSONObjects
    private void writeNodes(ObjectOutputStream out, JSONArray nodes) throws IOException {
        out.writeInt(nodes.length());
        Iterator iterator = nodes.iterator();
        while(iterator.hasNext()) {
            JSONObject node = (JSONObject) iterator.next();
            writeAny(out, node);
        }
    }

    private void writeEdges(ObjectOutputStream out, JSONArray edges) throws IOException {
        out.writeInt(edges.length());
        Iterator iterator = edges.iterator();
        while(iterator.hasNext()) {
            JSONArray edge = (JSONArray) iterator.next();
            writeAny(out, edge.getJSONObject(0));
            writeAny(out, edge.getJSONObject(1));
            writeAny(out, edge.getJSONObject(2));
        }
    }

    private void writeNodesAndEdges(java.io.ObjectOutputStream out, JSONObject nodesAndEdges) throws IOException {
        if(nodesAndEdges.has("nodes")) {
            writeNodes(out, nodesAndEdges.getJSONArray("nodes"));
        }
        else {
            writeNodes(out, new JSONArray());
        }
        if(nodesAndEdges.has("edges")) {
            writeEdges(out, nodesAndEdges.getJSONArray("edges"));
        }
        else {
            writeNodes(out, new JSONArray());
        }

    }

    //Takes an Object, converts to String, converts to UTF-8 byteArray, writes the length, then writes the byte[].
    private void writeAny(java.io.ObjectOutputStream out, Object input) throws IOException {
            try {
                byte[] data = input.toString().getBytes("UTF-8");
                out.writeInt(data.length);
                out.write(data);
            } catch (UTFDataFormatException e) {
                System.out.println("ERROR UNABLE TO WRITE UTF for input:" + input.toString());
                e.printStackTrace();
                byte[] data = "".getBytes("UTF-8");
                out.writeInt(data.length);
                out.write(data);
            }
    }

    private JSONObject readNodesAndEdges(ObjectInputStream in) throws IOException {
        JSONObject ret = new JSONObject();
        JSONArray nodes = readNodes(in);
        JSONArray edges = readEdges(in);
        ret.put("nodes", nodes);
        ret.put("edges", edges);
        return ret;
    }

    //reads a node count then reads that many JSONObjects
    private JSONArray readNodes(ObjectInputStream in) throws IOException {
        int length = in.readInt();
        JSONArray nodes = new JSONArray();
        for(int i = 0; i < length; i++) {
            JSONObject node = readJSONObject(in);
            nodes.put(node);
        }
        return nodes;
    }

    //reads an edge count then reads that many edges
    private JSONArray readEdges(ObjectInputStream in) throws IOException {
        int length = in.readInt();
        JSONArray edges = new JSONArray();
        for(int i = 0; i < length; i++) {
            JSONArray edge = readEdge(in);
            edges.put(edge);
        }
        return edges;
    }

    //reads 3 nodes
    private JSONArray readEdge(ObjectInputStream in) throws IOException {
        JSONArray edge = new JSONArray();
        edge.put(readJSONObject(in));
        edge.put(readJSONObject(in));
        edge.put(readJSONObject(in));
        return edge;
    }

    //reads a string and converts to JSONObject
    private JSONObject readJSONObject(java.io.ObjectInputStream in) throws IOException {
        String str = readString(in);
        JSONObject jObject = new JSONObject(str);
        return jObject;
    }

    //reads a UTF-8 byteArray to String and returns
    private String readString(ObjectInputStream in) throws IOException {
        int length = in.readInt();
        byte[] data = new byte[length];
        in.readFully(data);
        String str = new String(data, "UTF-8");
        return str;
    }

    // ----- BINARY SERIALIZATION METHODS -----
    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        /* Warning: Using writeUTF for large amounts of data (greater than 64KB) can result in
        java.io.UTFDataFormatException. */
        out.writeUTF(this.jobId);
        out.writeUTF(this.taskId);
        writeNodesAndEdges(out, this.requests);
        writeNodesAndEdges(out, this.responses);
        writeAny(out, this.jobConfig);
        out.writeUTF(this.payloadType);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException {
        this.jobId = in.readUTF();
        this.taskId= in.readUTF();
        this.requests = readNodesAndEdges(in);
        this.responses = readNodesAndEdges(in);
        this.jobConfig = readJSONObject(in);
        this.payloadType = in.readUTF();
    }

    public byte[] toByteArray() {
        return SerializationUtils.serialize(this);
    }

    // ----- JSON CONVERSION METHODS -----
    public static LGPayload fromJson(JSONObject obj) {
        JSONObject job_config = obj.getJSONObject("job_config");
        String job_id = obj.getString("job_id");
        String taskId = obj.getString("task_id");
        LGPayload payload = new LGPayload(job_id, taskId, job_config);
        payload.requests = obj.getJSONObject("requests");
        payload.responses = obj.getJSONObject("responses");
        payload.payloadType = obj.getString("payloadtype");
        return payload;
    }

    public String toJsonString(){
        return new JSONObject()
                .put("job_id", this.jobId)
                .put("task_id", this.taskId)
                .put("requests", this.requests)
                .put("responses", this.responses)
                .put("job_config", this.jobConfig)
                .put("payloadtype", this.payloadType)
                .toString();
    }

    @Override public String toString() { return this.toJsonString(); }

    public List<JSONObject> getRequestNodesAndEdges() {
        return getNodesAndEdges(this.getRequests());
    }

    public List<JSONObject> getResponseNodesAndEdges() {
        return getNodesAndEdges(this.getResponses());
    }

    //Returns a copy of all nodes and edges in a List
    public static List<JSONObject> getNodesAndEdges(JSONObject items) {
        //Gather 4 node sets. 1 nodes and 3 "edges" nodes.
        List<JSONObject> nodes = new ArrayList();
        JSONArray nodeSet = items.getJSONArray("nodes");
        Iterator nodeSetIterator = nodeSet.iterator();
        //Adds each node in "nodes"
        while(nodeSetIterator.hasNext()) {
            JSONObject node = (JSONObject) nodeSetIterator.next();
            nodes.add(node);
        }

        JSONArray edges = items.getJSONArray("edges");
        Iterator edgeIterator = edges.iterator();
        while(edgeIterator.hasNext()) {
            JSONArray edgeSet = (JSONArray) edgeIterator.next();
            if (edgeSet.length() == 3) {
                nodes.add(edgeSet.getJSONObject(0));//get source nodes
                nodes.add(edgeSet.getJSONObject(1));//get edges
                nodes.add(edgeSet.getJSONObject(2));//get destination nodes
            }
        }
        return nodes;
    }

    public static void main(String[] args) throws Exception {
        // Create lemongrenade.core.test object
        LGPayload test = new LGPayload();
        test.addRequestNode(new JSONObject().put("hello", "world"));
        test.addRequestNode(new JSONObject().put("hello", "world2"));
        test.addRequestNode(new JSONObject().put("hello", "world3"));

        // GLEntity -> byte[]
        byte[] data = test.toByteArray();

        // byte[] -> GLEntity
        LGPayload test2 = LGPayload.deserialize(data);

        // Verify results
        System.out.println(test2.jobId);
        System.out.println(test2.toJsonString());
        test2.getRequestNodes().forEach(request -> System.out.println(request.toString()));
    }

    public void setJobConfig(JSONObject job_config) {
         this.jobConfig = job_config;
    }

    public JSONObject getJobConfig() {
        return this.jobConfig;
    }
}

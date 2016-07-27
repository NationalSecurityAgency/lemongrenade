package lemongrenade.core.models;

import org.apache.storm.shade.org.apache.commons.lang.SerializationUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import java.io.IOException;
import java.io.Serializable;
import java.io.UTFDataFormatException;
import java.util.ArrayList;
import java.util.List;
import lemongrenade.core.util.LGConstants;

public class LGPayload implements Serializable {

    private String jobId;
    private String taskId;
    private JSONObject requests;
    private JSONObject responses;
    private JSONObject jobConfig; //Settings for this entire job, including lemongrenade.adapters to limit the run to.
    private String payloadType;

    public static JSONObject empty_nodes_edges() { //returns default response/request object
        JSONObject response =  new JSONObject().put("nodes", new JSONArray()).put("edges", new JSONArray());
        return response;
    }

    public LGPayload() { //create LGPayload with no args
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
        this.payloadType    = LGConstants.LG_PAYLOAD_TYPE_ADAPTERRESPONSE; // always adapterresponse by default
        this.requests = LGPayload.empty_nodes_edges();
        this.setJobConfig(jobConfig);
    }

    public static LGPayload deserialize(byte[] serialized) {
        return (LGPayload) SerializationUtils.deserialize(serialized);
    }

    public List<JSONObject> getRequestNodes(){
        List<JSONObject> obj = new ArrayList<>();
        this.requests.getJSONArray("nodes").forEach(element -> obj.add((JSONObject) element));
        return obj;
    }

    public List<JSONArray> getRequestEdges(){
        List<JSONArray> obj = new ArrayList<>();
        this.requests.getJSONArray("edges").forEach(element -> obj.add((JSONArray) element));
        return obj;
    }

    public void addResponseNodes(JSONArray nodes)    { this.responses.put("nodes",nodes);}
    public List<JSONObject> getResponseNodes(){
        List<JSONObject> obj = new ArrayList<>();
        this.responses.getJSONArray("nodes").forEach(element -> obj.add((JSONObject) element));
        return obj;
    }

    public List<JSONArray> getResponseEdges(){
        List<JSONArray> obj = new ArrayList<>();
        this.responses.getJSONArray("edges").forEach(element -> obj.add((JSONArray) element));
        return obj;
    }

    public void addRequestNodes(JSONArray nodes)    { this.requests.put("nodes",nodes);}
    public void addRequestNode(JSONObject metadata){
        this.requests.getJSONArray("nodes").put(metadata);
    }

    public void addRequestEdge(JSONObject sourceMetadata, JSONObject edgeMetadata, JSONObject targetMetadata){
        this.requests.getJSONArray("edges")
                .put(new JSONArray().put(sourceMetadata).put(edgeMetadata).put(targetMetadata));
    }

    public void addResponseNode(JSONObject metadata){
        this.responses.getJSONArray("nodes").put(metadata);
    }

    public void addResponseEdge(JSONObject sourceMetadata, JSONObject edgeMetadata, JSONObject targetMetadata){
        this.responses.getJSONArray("edges")
                .put(new JSONArray().put(sourceMetadata).put(edgeMetadata).put(targetMetadata));
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
        // TODO: Type check
        this.payloadType = payloadType;
    }

    //writes an Int indicating the number of nodes being written then those nodes
    private void writeNodes(java.io.ObjectOutputStream out, JSONArray nodes) throws IOException {
        out.writeInt(nodes.length());
        for(int i = 0; i < nodes.length(); i++) {
            JSONObject node = nodes.getJSONObject(i);
            try {
                out.writeUTF(node.toString()); //writes a node(JSONObject)
            } catch (UTFDataFormatException e) {
                System.out.println("ERROR UNABLE TO WRITE UTF for NODE:"+node.toString());
            }
        }
    }

    //writes an Int indicating the number of edges being written followed by those edges
    private void writeEdges(java.io.ObjectOutputStream out, JSONArray edges) throws IOException {
        out.writeInt(edges.length());
        for(int i = 0; i < edges.length(); i++) {
            JSONArray edge = edges.getJSONArray(i);
            out.writeUTF(edge.toString()); //writes a edge(JSONArray)
        }
    }

    private JSONArray readNodes(java.io.ObjectInputStream in) throws IOException {
        int count = in.readInt();
        JSONArray nodes = new JSONArray();
        for(int i = 0; i < count; i++) {
            JSONObject node = new JSONObject(in.readUTF());
            nodes.put(node); //add this node to nodes
        }
        return nodes;
    }

    private JSONArray readEdges(java.io.ObjectInputStream in) throws IOException {
        int count = in.readInt();
        JSONArray edges = new JSONArray();
        for(int i = 0; i < count; i++) {
            JSONArray edge = new JSONArray(in.readUTF());
            edges.put(edge); //add this node to nodes
        }
        return edges;
    }

    // ----- BINARY SERIALIZATION METHODS -----
    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        /* Warning: Using writeUTF for large amounts of data (greater than 64KB) can result in
        java.io.UTFDataFormatException. If this ever occurs, further split up the node/edge data. */
        out.writeUTF(this.jobId);
        out.writeUTF(this.taskId);
        writeNodes(out, this.requests.getJSONArray("nodes"));
        writeEdges(out, this.requests.getJSONArray("edges"));
        writeNodes(out, this.responses.getJSONArray("nodes"));
        writeEdges(out, this.responses.getJSONArray("edges"));
        out.writeUTF(this.jobConfig.toString());
        out.writeUTF(this.payloadType);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        this.jobId = in.readUTF();
        this.taskId= in.readUTF();
        this.requests = new JSONObject()
                .put("nodes", readNodes(in))
                .put("edges", readEdges(in));
        this.responses = new JSONObject()
                .put("nodes", readNodes(in))
                .put("edges", readEdges(in));
        this.jobConfig = new JSONObject(in.readUTF());
        this.payloadType = in.readUTF();
    }

    public byte[] toByteArray() {
        return SerializationUtils.serialize(this);
    }

    // ----- JSON CONVERSION METHODS -----

    public static LGPayload fromJson(JSONObject obj){
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

    @Override
    public String toString() {
        return this.toJsonString();
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

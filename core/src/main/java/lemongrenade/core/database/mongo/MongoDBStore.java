package lemongrenade.core.database.mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import lemongrenade.core.util.LGProperties;
import org.bson.Document;
import org.json.JSONObject;
import java.util.HashMap;
import java.util.Map;

public class MongoDBStore {

    private MongoClient client;
    private MongoDatabase db;
    private FindOneAndReplaceOptions UPSERT;

    public MongoDBStore(){
        client = new MongoClient(LGProperties.get("mongo.hostname"), 27017);
        db = client.getDatabase("lemongrenade");
        UPSERT = new FindOneAndReplaceOptions().upsert(true);
    }

    private String getDbKey(String job_id, String key){
        return job_id + key;
    }

    public boolean hasNode(String job_id, String key){
        return db.getCollection("nodes").find(Filters.eq("_id", getDbKey(job_id, key))).first() != null;
    }

    public JSONObject getNodeByKey(String job_id, String key){
        JSONObject ret = new JSONObject();
        db.getCollection("nodes").find(Filters.eq("_id", getDbKey(job_id, key))).first()
                .forEach((prop, value) -> ret.put(prop, value));
        return ret;
    }

    public void saveNode(String job_id, String key, JSONObject metadata){
        Map<String, Object> props = new HashMap<>();
        metadata.keySet().forEach(prop -> props.put(prop, metadata.get(prop)));
        props.put("_id", getDbKey(job_id, key));
        props.put("_job_id", job_id);
        db.getCollection("nodes").findOneAndReplace(Filters.eq("_id", getDbKey(job_id, key)), new Document(props), UPSERT);
    }

    public boolean hasEdge(String job_id, String key){
        return db.getCollection("edges").find(Filters.eq("_id", getDbKey(job_id, key))).first() != null;
    }

    public JSONObject getEdgeByKey(String job_id, String key){
        JSONObject ret = new JSONObject();
        db.getCollection("edges").find(Filters.eq("_id", getDbKey(job_id, key))).first()
                .forEach((prop, value) -> ret.put(prop, value));
        return ret;
    }

    public void saveEdge(String job_id, String key, String srcKey, String dstKey, JSONObject metadata){
        Map<String, Object> props = new HashMap<>();
        metadata.keySet().forEach(prop -> props.put(prop, metadata.get(prop)));
        props.put("_id", getDbKey(job_id, key));
        props.put("_job_id", job_id);
        props.put("_src", srcKey);
        props.put("_dst", dstKey);
        db.getCollection("edges").insertOne(new Document(props));
    }

    private JSONObject documentToJSON(Document doc){
        JSONObject ret = new JSONObject();
        doc.forEach((prop, value) -> ret.put(prop, value));
        return ret;
    }

    public JSONObject getGraphByJobId(String job_id){
        JSONObject ret = new JSONObject().put("nodes", new JSONObject()).put("edges", new JSONObject());
        Iterable<Document> nodes = db.getCollection("nodes").find(Filters.eq("_job_id", job_id));
        Iterable<Document> edges = db.getCollection("edges").find(Filters.eq("_job_id", job_id));
        nodes.forEach(document -> {
            JSONObject json = documentToJSON(document);
            String id = json.getString("_id");
            ret.getJSONObject("nodes").put(id, json);
        });
        edges.forEach(document -> {
            JSONObject json = documentToJSON(document);
            String id = json.getString("_id");
            ret.getJSONObject("edges").put(id, json);
        });
        return ret;
    }

    public static void main(String[] args) {
        MongoDBStore store = new MongoDBStore();
        System.out.println(store.getGraphByJobId("fc41ae2f-a1b1-4cd5-b4a9-bfaf69a02644").toString(4));
    }
}

package lemongrenade.core.database.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.QueryBuilder;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOptions;
import lemongrenade.core.models.LGJob;
import lemongrenade.core.util.LGProperties;
import org.bson.Document;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.conversions.Bson;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.include;

public class MongoDBStore {

    private static final Logger log = LoggerFactory.getLogger(MongoDBStore.class);
    private static FindOneAndReplaceOptions UPSERT = new FindOneAndReplaceOptions().upsert(true);
    static private MongoClient client = null;
    static private MongoDatabase db;
    static private MongoCollection<Document> jobs;
    static private MongoCollection<Document> tasks;
    static private MongoCollection<Document> dbValues;

    static {
        open();
    }

    //When declaring a new instance of MongoDBStore, make sure the connection is open
    public MongoDBStore(){
        open();
    }

    //Re-open connections if client is null
    public static void open() {
        if(client == null) {
            client = new MongoClient(LGProperties.get("mongo.hostname"), 27017);
            db = client.getDatabase("lemongrenade");
            jobs = db.getCollection("jobs");
            tasks = db.getCollection("tasks");
            dbValues = db.getCollection("dbValues");
        }
    }

    //Close the current connection then nulls storage fields
    public static void close() {
        try {
            if(client != null) {
                log.info("Closing connection to MongoDB.");
                client.close();
                client = null;
                db = null;
                jobs = null;
                tasks = null;
                dbValues = null;
            }
        }
        catch(Exception e) {
            log.error("Error closing connection to Mongo.");
            e.printStackTrace();
        }
    }

    public static MongoDatabase getDatabase() {return db;}

    public static void deleteJob(String jobId) {
        open();
        deleteItem(jobs, "_id", jobId);
    }

    public static void deleteTask(String taskId) {deleteItem(tasks, "_id", taskId);}

    public static void deleteTasksByJob(String jobId) {deleteTasks("jobId", jobId);}

    //deletes all tasks for key->value
    public static void deleteTasks(String key, String value) {deleteMany(tasks, key, value);}

    public static void deleteItem(MongoCollection collection, String key, String value) {
        BasicDBObject query = new BasicDBObject();
        query.append(key, value);
        collection.deleteOne(query);
    }

    public static void deleteMany(MongoCollection collection, String key, String value) {
        BasicDBObject query = new BasicDBObject();
        query.append(key, value);
        collection.deleteMany(query);
    }

    public static ArrayList<Document> getTasksFromJob(String jobId) {return getDocuments(tasks, "jobId", jobId);}

    public static ArrayList<Document> getJob(String jobId) {return getDocuments(jobs, "_id", jobId);}

    public static ArrayList<Document> getJobs(JSONArray jobIds) {return getDocuments(jobs, "_id", jobIds);}

    //get all jobs
    public static ArrayList<Document> getJobs() {
        FindIterable<Document> docResponse = jobs.find();
        Iterator documentIterator = docResponse.iterator();
        ArrayList<Document> documents = new ArrayList();
        while(documentIterator.hasNext()) {
            Document document = (Document) documentIterator.next();
            documents.add(document);
        }
        return documents;
    }

    public static ArrayList<Document> getDocuments(MongoCollection collection, String key, String value) {
        ArrayList documents = getDocuments(collection, key, new JSONArray().put(value));
        return documents;
    }

    //Creates 1 request for multiple values
    public static ArrayList<Document> getDocuments(MongoCollection collection, String key, JSONArray input) {
        ArrayList<Document> documents = new ArrayList();
        try {
            String[] values = convertArray(input);
            QueryBuilder builder = new QueryBuilder();
            builder.put(key).in(values);
            BasicDBObject query = (BasicDBObject) builder.get();
            FindIterable<Document> docResponse = collection.find(query);
            Iterator documentIterator = docResponse.iterator();
            while (documentIterator.hasNext()) {
                Document document = (Document) documentIterator.next();
                documents.add(document);
            }
        }
        catch(Exception e) {
            log.info("Failed to get any documents from collection:"+collection+" for key:"+key);
            log.error(e.getMessage());
        }
        return documents;
    }

    public static JSONObject toJSON(ArrayList<Document> input) {
        JSONArray documents = toJSON((Iterable) input);
        JSONObject ret = new JSONObject();
        Iterator iterator = documents.iterator();
        while(iterator.hasNext()) {
            JSONObject document = (JSONObject) iterator.next();
            String id = document.get("_id").toString();
            ret.put(id, document);
        }
        return ret;
    }

    static JSONObject toJSON(Map input) {
        Iterator keys = input.keySet().iterator();
        JSONObject item = new JSONObject();
        while(keys.hasNext()) {
            String key = keys.next().toString();
            Object value = input.get(key);
            if(value instanceof Map<?,?>) {
                value = toJSON((Map) value);
            }
            else if(value instanceof Iterable) {
                value = toJSON((Iterable) value);
            }
            item.put(key, value);
        }
        return item;
    }

    static JSONArray toJSON(Iterable input) {
        Iterator inputItems = input.iterator();
        JSONArray ret = new JSONArray();
        while(inputItems.hasNext()) {
            Object value = inputItems.next();
            if(value instanceof Map<?,?>) {
                value = toJSON((Map) value);
            }
            else if(value instanceof Iterable) {
                value = toJSON((Iterable) value);
            }
            ret.put(value);
        }
        return ret;
    }

    static String[] convertArray(JSONArray input) {
        int length = input.length();
        String[] array = new String[length];
        for(int i = 0; i < length; i++) {
            String item = input.get(i).toString();
            array[i] = item;
        }
        return array;
    }

    public static void printDocuments(Iterable<Document> iterable) {
        Iterator taskIterator = iterable.iterator();
        while(taskIterator.hasNext()) {
            Document document = (Document) taskIterator.next();
            log.info(document.toString());
        }
    }

    private static String getDbKey(String job_id, String key){return job_id + key;}

    public static boolean hasNode(String job_id, String key){
        return db.getCollection("nodes").find(eq("_id", getDbKey(job_id, key))).first() != null;
    }

    public static Set<Document> getPastJobs(String field) {//gets set of all jobIds whose passed "field" is a long set to before the current time in MS
        Set<Document> documents = new HashSet();
        Long currentMS = System.currentTimeMillis();
        Document lteCurrentMS = new Document(field, new Document("$lte", currentMS));
        FindIterable<Document> iterable = jobs.find(lteCurrentMS);
        Iterator iterator = iterable.iterator();
        while(iterator.hasNext()) {
            Document document = (Document) iterator.next();
            documents.add(document);
        }
        return documents;
    }

    //Gets jobs whose reset time is past who are not in the RESET status
    public static Set<String> getResetJobs() {
        Set<Document> documents = new HashSet();
        Long currentMS = System.currentTimeMillis();
        Document query = new Document("resetDate", new Document("$lte", currentMS));
        query.append("status", new Document("$ne", LGJob.STATUS_RESET));
        Document returnFields = new Document("_id", true);
        FindIterable<Document> iterable = jobs.find(query).projection(returnFields);
        Iterator iterator = iterable.iterator();
        while(iterator.hasNext()) {
            Document document = (Document) iterator.next();
            documents.add(document);
        }

        //Legacy check. Reset old jobs.
        Long resetStartTime = currentMS - LGJob.DAY*LGJob.DEFAULT_RESET_DAYS;
        query = new Document("startTime", new Document("$lte", resetStartTime).append("$ne", 0));
        query.append("status", new Document("$ne", LGJob.STATUS_RESET));
        Bson projection = Projections.fields(include("_id", "startTime"));
        iterable = jobs.find(query).projection(projection);
        iterator = iterable.iterator();
        while(iterator.hasNext()) {
            Document document = (Document) iterator.next();
            documents.add(document);
        }

        return getIDs(documents);
    }

    //Gets jobs 'expiredDate' field has passed.
    public static Set<String> getExpiredJobs() {
        Set<Document> documents = new HashSet();
        Long currentMS = System.currentTimeMillis();
        Document query = new Document("expireDate", new Document("$lte", currentMS).append("$ne", 0));
        Bson projection = Projections.fields(include("_id", "expireDate"));
        FindIterable<Document> iterable = jobs.find(query).projection(projection);
        Iterator iterator = iterable.iterator();
        while(iterator.hasNext()) {
            Document document = (Document) iterator.next();
            documents.add(document);
        }

        //Legacy check. Expire old jobs.
        Long expireEndTime = currentMS - LGJob.DAY*LGJob.DEFAULT_EXPIRE_DAYS;
        query = new Document("endTime", new Document("$lte", expireEndTime).append("$ne", 0));
        projection = Projections.fields(include("_id", "endTime"));//, gte("expireDate", new Long("1")));
        iterable = jobs.find(query).projection(projection);
        iterator = iterable.iterator();
        while(iterator.hasNext()) {
            Document document = (Document) iterator.next();
            documents.add(document);
        }

        //Error job check. Expire old jobs that have "0" for their endTime
        Long expireStartTime = currentMS - LGJob.DAY*LGJob.DEFAULT_EXPIRE_DAYS;
        query = new Document("startTime", new Document("$lte", expireStartTime));
        Document zero = new Document("$eq", 0);
        query.append("endTime", zero);
        projection = Projections.fields(include("_id", "endTime"));//, gte("expireDate", new Long("1")));
        iterable = jobs.find(query).projection(projection);
        iterator = iterable.iterator();
        while(iterator.hasNext()) {
            Document document = (Document) iterator.next();
            documents.add(document);
        }

        return getIDs(documents);
    }

    public static JSONObject getNodeByKey(String job_id, String key){
        JSONObject ret = new JSONObject();
        db.getCollection("nodes").find(eq("_id", getDbKey(job_id, key))).first()
                .forEach((prop, value) -> ret.put(prop, value));
        return ret;
    }

    public static void saveNode(String job_id, String key, JSONObject metadata){
        Map<String, Object> props = new HashMap<>();
        metadata.keySet().forEach(prop -> props.put(prop, metadata.get(prop)));
        props.put("_id", getDbKey(job_id, key));
        props.put("_job_id", job_id);
        db.getCollection("nodes").findOneAndReplace(eq("_id", getDbKey(job_id, key)), new Document(props), UPSERT);
    }

    public static boolean hasEdge(String job_id, String key){
        return db.getCollection("edges").find(eq("_id", getDbKey(job_id, key))).first() != null;
    }

    //Returns 'true' if collection has a value for _id
    public static boolean hasId(String collection, String _id){
        return db.getCollection(collection).find(eq("_id", _id)).first() != null;
    }

    public static JSONObject getEdgeByKey(String job_id, String key){
        JSONObject ret = new JSONObject();
        db.getCollection("edges").find(eq("_id", getDbKey(job_id, key))).first()
                .forEach((prop, value) -> ret.put(prop, value));
        return ret;
    }

    public static void saveEdge(String job_id, String key, String srcKey, String dstKey, JSONObject metadata){
        Map<String, Object> props = new HashMap<>();
        metadata.keySet().forEach(prop -> props.put(prop, metadata.get(prop)));
        props.put("_id", getDbKey(job_id, key));
        props.put("_job_id", job_id);
        props.put("_src", srcKey);
        props.put("_dst", dstKey);
        db.getCollection("edges").insertOne(new Document(props));
    }

    private static JSONObject documentToJSON(Document doc){
        JSONObject ret = new JSONObject();
        doc.forEach((prop, value) -> ret.put(prop, value));
        return ret;
    }

    public static JSONObject getGraphByJobId(String job_id){
        JSONObject ret = new JSONObject().put("nodes", new JSONObject()).put("edges", new JSONObject());
        Iterable<Document> nodes = db.getCollection("nodes").find(eq("_job_id", job_id));
        Iterable<Document> edges = db.getCollection("edges").find(eq("_job_id", job_id));
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

    public static void appendToDBValues(String collection, String _id, String key, Object listItem) {
        log.debug("Updating collection:"+collection+" _id:"+_id+" key:"+key+" item:"+listItem.toString());
        if(listItem instanceof JSONObject) {
            listItem = createObject((JSONObject) listItem, null);
        }
        MongoCollection<Document> coll = db.getCollection(collection);

        coll.updateOne(eq("_id", _id), new Document("$push",
                new Document("dbValues." + key, listItem)), (new UpdateOptions().upsert(true)));
    }

    public static void appendToTask(String taskId, String key, Object update) {
        appendToDocument("tasks", taskId, key, update);
    }

    public static void appendToAdapters(String _id, String key, Object update) {
        appendToDocument("adapters", _id, key, update);
    }

    public static void appendToJob(String jobId, String key, Object update) {
        appendToDocument("jobs", jobId, key, update);
    }

    //Updates a job if it exists; else creates a new job with update
    public static void updateJob(String jobId, String key, Object update) throws CodecConfigurationException {
        open();//ensure connection is open
        Document doc = new Document();
        doc.put("_id", jobId);
        Boolean present = hasId("jobs", jobId);
        if(present) {
            appendToJob(jobId,key,update);
        }
        else {
            doc.put(key,update);
            insertDocument("jobs", doc);//insert a new document
        }
    }



    //insertToObject
    public static void insertToObject(String collection, String _id, String objectName, JSONObject value) {
        DBObject listItem = createObject(value, objectName);
        appendToDocument(collection, _id, listItem);
    }

    //Push Object to List
    public static void appendToList(String collection, String _id, String key, Object listItem) {
        appendToList(collection, _id, key, listItem, false);
    }

    //Push Objec to List. Set upsert to true or false.
    public static void appendToList(String collection, String _id, String key, Object listItem, boolean upsert) {
        log.debug("Updating collection:"+collection+" _id:"+_id+" key:"+key+" item:"+listItem.toString());
        if(listItem instanceof JSONObject) {
            listItem = createObject((JSONObject) listItem, null);
        }
        Document doc = new Document(key, listItem);
        doc = new Document("$push", doc);
        MongoCollection<Document> coll = db.getCollection(collection);
        UpdateOptions options = new UpdateOptions().upsert(upsert);
        coll.updateOne(eq("_id", _id), doc, options);
    }

    //Pull Entry from array
    public static void removeFromList(String collection, String _id, String key, Object listItem) {
        if(listItem instanceof JSONObject) {
            listItem = createObject((JSONObject) listItem, null);
        }
        Document doc = new Document(key, listItem);
        doc = new Document("$pull", doc);
        MongoCollection<Document> coll = db.getCollection(collection);
        coll.updateOne(eq("_id", _id), doc);
    }

    //Create object to be inserted to existing data item.
    public static BasicDBObject createObject(JSONObject object, String insert) {
        Iterator iterator = object.keySet().iterator();
        BasicDBObject dbObject = new BasicDBObject();
        while(iterator.hasNext()) {
            String key = iterator.next().toString();
            Object value = object.get(key);
            if(insert != null) {
                key = insert+"."+key;
            }
            dbObject.append(key, value);
        }
        return dbObject;
    }

    //Not all Objects for update are valid. A CodecConfigurationException is thrown for invalid Object types. E.g JSONObject
    public static void appendToDocument(String collection, String _id, Object input) throws CodecConfigurationException {
        Document doc = new Document("$set", input);
        updateDocument(collection, _id, doc);
    }

    //Not all Objects for update are valid. A CodecConfigurationException is thrown for invalid Object types. E.g JSONObject
    public static void appendToDocument(String collection, String _id, String key, Object update) throws CodecConfigurationException {
        Document doc = new Document("$set", new Document(key, update));
        updateDocument(collection, _id, doc);
    }

    //Updates the document under '_id'. Will not create an entry if a document with "_id" doesn't exist.
    public static void updateDocument(String collection, String _id, Document doc) throws CodecConfigurationException {
        open();//ensure connection is open
        MongoCollection<Document> coll = db.getCollection(collection);
        coll.updateOne(eq("_id", _id), doc);
        coll.updateOne(eq("_id", _id), doc);
    }

    //Inserts the document if none exists
    public static void insertDocument(String collection, Document doc) throws CodecConfigurationException {
        open();//ensure connection is open
        MongoCollection<Document> coll = db.getCollection(collection);
        coll.insertOne(doc);
    }

    //If "_id" exists, perform a findOneAndReplace, otherwise perform an insert
    public static void saveDocument(String collection, String _id, Document doc) throws CodecConfigurationException {
        open();//ensure connection is open
        doc.put("_id", _id);
        Boolean present = hasId(collection, _id);
        if(present) {
            db.getCollection(collection).findOneAndReplace(eq("_id", _id), doc);//replace existing document
        }
        else {
            insertDocument(collection, doc);//insert a new document
        }
    }

    public static Set<String> getIDs(Set<Document> input) {
        Set<String> IDs = new HashSet();
        Iterator iterator = input.iterator();
        while(iterator.hasNext()) {
            Document document = (Document) iterator.next();
            String id = document.get("_id").toString();
            IDs.add(id);
        }
        return IDs;
    }

}

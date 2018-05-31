package lemongrenade.core.database.mongo;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.WriteResult;
import lemongrenade.core.models.LGdbValue;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONObject;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.query.Query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class LGdbValueDAOImpl extends BasicDAO<LGdbValue, ObjectId>
implements LGdbValueDAO {

    public LGdbValueDAOImpl(Class<LGdbValue> entityClass, Datastore ds) {super(entityClass, ds);}

    public ArrayList<String> getAllJobIDsForDbValueKey(String dbValueKey) {
        List<LGdbValue> lGdbValues = getAll();
        ArrayList<String> jobIDs = new ArrayList<>();
        for (LGdbValue lGdbValue : lGdbValues) {
            if (lGdbValue.getDbValues().containsKey(dbValueKey)) {
                jobIDs.add(lGdbValue.getJobId());
            }
            System.out.println(lGdbValue.toJson());
        }
        return jobIDs;
    }

    public ArrayList<LGdbValue> getAllJobsThatHaveDbValueKey(String dbValueKey) {
        ArrayList<String> allJobIdsWithDbValueKey = getAllJobIDsForDbValueKey(dbValueKey);
        ArrayList<LGdbValue> allJobs = new ArrayList<>();
        for (String jobId: allJobIdsWithDbValueKey) {
            allJobs.add(getDbValuesByJobIdandKey(jobId, dbValueKey));
        }
        return allJobs;
    }

    public JSONArray getAllJobsThatHaveDbValueKeyJSONArray(String dbValueKey) {
        ArrayList<LGdbValue> lGdbValues = getAllJobsThatHaveDbValueKey(dbValueKey);
        JSONArray allJobs = new JSONArray();
        for (LGdbValue lGdbValue : lGdbValues) {
            lGdbValue = getDbValuesByJobIdandKey(lGdbValue.getJobId(),dbValueKey);
            allJobs.put(lGdbValue.toJson());
        }
        return allJobs;
    }

    public LGdbValue getDbValuesByJobIdandKey(String jobId, String key) {
        Query<LGdbValue> query = createQuery().field("_id").equal(jobId);
        LGdbValue mylGdbValue = query.get(), lGdbValue = null;
        if (null == mylGdbValue) {
            return mylGdbValue;
        }

        if (!mylGdbValue.containsKey(key.toLowerCase())){
            return lGdbValue;
        }
        ArrayList<String> valuesByKey = mylGdbValue.getDbValues(key.toLowerCase());
        lGdbValue = new LGdbValue(jobId,key.toLowerCase(),valuesByKey);
        return lGdbValue;
    }

    public void removeAllDocuments () {
        DBCollection lgDBValue = createQuery().getCollection();
        DBCursor cursor = lgDBValue.find();
        while (cursor.hasNext()) {
            lgDBValue.remove(cursor. next());
        }
    }

    public List<LGdbValue> getAll() {
        return createQuery().asList();
    }

    public LGdbValue getDbValuesByJobId(String jobId) {
        Query<LGdbValue> query = createQuery().field("_id").equal(jobId);

        LGdbValue lGdbValue = query.get();

        if (null == lGdbValue) {
            return lGdbValue;
        }

        return lGdbValue;
    }

    /**
     * @param jobId String for job ID
     * @param key String for key
     * @param dbValue String for db value
     */
    public void addDbValueToJob(String jobId, String key, String dbValue) {
        MongoDBStore mongoDBStore = new MongoDBStore();
        mongoDBStore.appendToDBValues("dbValues", jobId, key.toLowerCase(), dbValue);
    }

    /**
     * @param jobId String for job ID
     * @param key String for key
     * @param dbValues String for db values
     * TODO: Write a function in MongoDBStore to append multiple values in one write
     */
    public void addDbValueToJob(String jobId, String key, JSONArray dbValues) {
        MongoDBStore mongoDBStore = new MongoDBStore();
        for (Object dbValue :dbValues) {
            mongoDBStore.appendToDBValues("dbValues", jobId, key.toLowerCase(), dbValue);
        }
    }

    /**
     *
     * @param jobId String for job ID
     * @return WriteResult
     */
    @Override public WriteResult delete(String jobId) {
        LGdbValue LGdbValue = getDbValuesByJobId(jobId);
        return super.delete(LGdbValue);
    }

    /**
     * @param LGdbValue LGdbValue
     */
    public void saveDbValues(LGdbValue LGdbValue) {
        getDatastore().save(LGdbValue);
    }

    /**
     * main
     * @param args Standard main args, unused here.
     */
    public static void main(String[] args) {
        int z = -1;
        String key = "VALUES";
        LGdbValueDAOImpl dao;
        MorphiaService ms;
        ms = new MorphiaService();
        dao = new LGdbValueDAOImpl(LGdbValue.class, ms.getDatastore());
        long start = -1;
        LGdbValue lookup = null;
        long seconds = -1;
        JSONObject lookupJSON = null;

        dao.removeAllDocuments();



        String jobId = UUID.randomUUID().toString();
        HashMap<String,ArrayList<String>> testValueJO = new HashMap<>();
        ArrayList<String> values = new ArrayList<>();

        System.out.println("Test 1:  Add values to DB and read them back from DB.");

        z = 4;

        for (int i = 0; i < 4; i++){
            values.add(UUID.randomUUID().toString());
        }

        testValueJO.put(key.toLowerCase(), values);

        LGdbValue testValue = new LGdbValue(jobId, testValueJO);

        System.out.println("toJson = " + testValue.toJson());

        dao.save(testValue);

        System.out.println("Added new Query Job " + testValue.toString());
        start = System.currentTimeMillis();
        lookup = dao.getDbValuesByJobIdandKey(jobId, key);
        seconds = (System.currentTimeMillis() - start) ;
        System.out.println("Lookup "+lookup.toString());

        lookupJSON = lookup.toJson();

        System.out.println("toJson = " + lookupJSON.toString());

        System.out.println("Query complete. Seconds :" + seconds);

        System.out.println("Test 1 is complete.\n\n");


        z = 4;

        System.out.println("Test 2:  Adding new VALUE ");

        z = 4;

        dao.addDbValueToJob(jobId, key, UUID.randomUUID().toString());
        dao.addDbValueToJob(jobId,"cat", UUID.randomUUID().toString());
        dao.addDbValueToJob(jobId,"dog", UUID.randomUUID().toString());
        dao.addDbValueToJob(jobId,"cat", UUID.randomUUID().toString());
        dao.addDbValueToJob(jobId,"dog", UUID.randomUUID().toString());


        start = System.currentTimeMillis();
        lookup = dao.getDbValuesByJobIdandKey(jobId, key);
        seconds = (System.currentTimeMillis() - start) ;
        System.out.println("Lookup "+lookup.toString());

        lookupJSON = lookup.toJson();

        System.out.println("toJson = "+lookupJSON.toString());

        System.out.println("Query complete. Seconds :" + seconds);

        System.out.println("Test 2 is complete.\n\n");


        z = 4;

        System.out.println("Test 3:test adding array of VALUES");

        z = 4;

        JSONArray testValues = new JSONArray();

        for (int i = 0; i < 4; i++) {
            testValues.put(UUID.randomUUID().toString());
        }

        jobId = UUID.randomUUID().toString();

        dao.addDbValueToJob(jobId,key, testValues);


        start = System.currentTimeMillis();
        lookup = dao.getDbValuesByJobIdandKey(jobId,key);
        seconds = (System.currentTimeMillis() - start) ;
        System.out.println("Lookup "+lookup.toString());

        lookupJSON = lookup.toJson();

        System.out.println("toJson = "+lookupJSON.toString());

        System.out.println("Query complete. Seconds :" + seconds);

        System.out.println("Test 3 is complete.\n\n");


        z = 4;


        System.out.println("Test 4: test adding array of different types");

        z = 4;

        JSONArray blobs = new JSONArray();
        JSONArray cats = new JSONArray();
        JSONArray BIGDOGS = new JSONArray();
        JSONArray elephants = new JSONArray();



        for (int i = 0; i < 4; i++) {
            blobs.put(UUID.randomUUID().toString());
            cats.put(UUID.randomUUID().toString());
            BIGDOGS.put(UUID.randomUUID().toString());
            elephants.put(UUID.randomUUID().toString());

        }

        dao.addDbValueToJob(jobId,"blobs", blobs);
        dao.addDbValueToJob(jobId,"cats", cats);
        dao.addDbValueToJob(jobId,"BIGDOGS", BIGDOGS);
        dao.addDbValueToJob(jobId,"elephants", elephants);


        start = System.currentTimeMillis();
        lookup = dao.getDbValuesByJobId(jobId);
        seconds = (System.currentTimeMillis() - start) ;
        System.out.println("Lookup "+lookup.toString());

        lookupJSON = lookup.toJson();

        System.out.println("toJson = "+lookupJSON.toString());

        System.out.println("Query complete. Seconds :" + seconds);

        System.out.println("Test 4 is complete.\n\n");


        z = 4;

        System.out.println("Test 5: test updating and retrieving one data value type.");

        z = 4;

        dao.addDbValueToJob(jobId, "BIGDOGS", "44444");

        LGdbValue lookupDogs = dao.getDbValuesByJobIdandKey(jobId, "BIGDOGS");

        System.out.println(lookupDogs.toJson().toString());

        System.out.println("Test 5: test complete.");

        z = 4;

        System.out.println("Test 6: Test retrieving everything");

        z = 4;


        LGdbValue lookupAll = dao.getDbValuesByJobId(jobId);

        System.out.println(lookupAll.toJson().toString());

        System.out.println("Test 6: test complete.");

        z = 4;

        System.out.println("Test 7:  get all jobs for a particular dbValue (value)");

        //add a non-value JobId to test that it does not end up on our value list.

        jobId = UUID.randomUUID().toString();
        key = "DAWGPOUND";
        HashMap<String,ArrayList<String>> testdbValueJO = new HashMap<>();
        ArrayList<String> dbValues = new ArrayList<>();


        for (int i = 0; i < 4; i++){
            dbValues.add(UUID.randomUUID().toString());
        }

        testdbValueJO.put(key.toLowerCase(), dbValues);

        LGdbValue testdbValue = new LGdbValue(jobId, testdbValueJO);

        dao.save(testdbValue);

        //This tests the function that returns all jobs that have dbValues
        ArrayList<LGdbValue> i = dao.getAllJobsThatHaveDbValueKey("values");


        for (LGdbValue lGdbValue : i) {
            System.out.println("i == "+lGdbValue.getJobId());
        }
        System.out.println("Test 7:  Done.");


        System.out.println("Test 8:  Get JSON that has JOBids/values ");
        JSONArray joy = dao.getAllJobsThatHaveDbValueKeyJSONArray("values");
        System.out.println(joy);
        System.out.println("Test 8:  Done.");


    }

}
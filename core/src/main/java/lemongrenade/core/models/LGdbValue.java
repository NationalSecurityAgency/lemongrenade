package lemongrenade.core.models;

import org.json.JSONArray;
import org.json.JSONObject;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;


import java.util.*;

/**
 * DbValues are always stored in database until a job is "DELETED" or "PURGED"
 */
@Entity(value = "dbValues", noClassnameStored = true)

public class LGdbValue {

    @Id
    private String jobId;
    private Map<String,ArrayList<String>> dbValues;
    /**
     * Constructors
     */

    public LGdbValue () {


    }
    public LGdbValue(String jobId) {
        this.jobId = jobId;
        this.dbValues = new HashMap<String,ArrayList<String>>();
    }

    public LGdbValue(String jobId, HashMap dbValues) {
        this.jobId = jobId;
        this.dbValues = dbValues;
    }

    public LGdbValue (String jobId, String key, JSONArray dbValues){
        this.jobId = jobId;
        this.dbValues = new HashMap<String,ArrayList<String>>();

        ArrayList<String> arrayList = new ArrayList<String>();

        for (int i = 0; i < dbValues.length(); i++) {
            arrayList.add(dbValues.getString(i));
        }
        this.dbValues.put(key.toLowerCase(),arrayList);

    }

    public LGdbValue(String jobId, String key, ArrayList<String> valuesByKey) {

        this.jobId = jobId;
        this.dbValues = new HashMap<>();
        this.dbValues.put(key.toLowerCase(),valuesByKey);
    }


    /**
     *
     * @return dbValues
     */
    public HashMap<String, ArrayList<String>> getDbValues() {
        if (dbValues == null)
            return new HashMap<String,ArrayList<String>>();
        return (HashMap<String, ArrayList<String>>) dbValues;
    }


    public ArrayList<String> getDbValues(String key) {

        return dbValues.get(key.toLowerCase());
    }

    /**
     * @param dbValues Map object to set dbValues to
     */
    public void setDbValues(final Map<String, ArrayList<String>> dbValues){
        this.dbValues = dbValues;
    }

    /**
     *
     * @param key String key
     * @param dbValue String dbValue
     */
    public void addDbValue(String key, String dbValue){

        if (this.dbValues.containsKey(key.toLowerCase())) {
            if (!this.dbValues.get(key.toLowerCase()).contains(dbValue)) {
                this.dbValues.get(key.toLowerCase()).add(dbValue);
            }
        }
        else {
            ArrayList<String> newValues = new ArrayList<String>();
            newValues.add(dbValue);
            this.dbValues.put(key.toLowerCase(),newValues);
        }
    }

    public void addDbValue(String key, JSONArray values){

            for (int i = 0; i < values.length(); i++) {
                addDbValue(key,values.get(i).toString());
            }
    }


    /**
     *
     * @return dbValuesByJobID
     */

    public JSONObject toJson(){

        JSONObject lgdbValue = new JSONObject()
                .put(this.jobId,this.dbValues);

        return lgdbValue;
    }


    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public boolean containsKey(String key) {
        return this.dbValues.containsKey(key.toLowerCase());
    }
}

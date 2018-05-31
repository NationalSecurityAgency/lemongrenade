package lemongrenade.core.coordinator;

import lemongrenade.core.database.mongo.LGAdapterDAOImpl;
import lemongrenade.core.database.mongo.MorphiaService;
import lemongrenade.core.models.LGAdapterModel;
import lemongrenade.core.models.LGJob;
import lemongrenade.core.util.JSONUtils;
import lemongrenade.core.util.LGProperties;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * AdapterManager doesn't ever make changes to the Mongo Adapter fields. It is read-only.
 * (with the exception of status information) It's up to the adapter itself to write heartbeat data
 * to the database and to register itself at startup to the Adapter table(s).
 *
 * Adapters don't keep an action count (current running tasks) because they are only ever processing one
 * task at a time. Currently, the only way to see how many jobs are queued for an adapter is to view the
 *
 * Each lemongrenade.core.coordinator can only see the jobs it has given to a adapter. So, a future
 * update should look at the RabbitMQ size + the current action count to get an idea of a load balancing metric.
 * Another possibility is to pull information from nimbus (storm) directly. Nimbus looks like it provides a
 * 'reliability' metric as well. More research is needed on that.
 */
public class AdapterManager {
    private static final Logger log = LoggerFactory.getLogger(AdapterManager.class);
    final static int MAX_ADAPTER_HEARTBEAT_TIME = LGProperties.getInteger("max_adapter_heartbeat_time",300);

    static private MorphiaService MS = null;
    static private LGAdapterDAOImpl ADAPTER_DAO = null;
    static private HashMap<String, LGAdapterModel> ADAPTER_CACHE = null;

    static {
        open();
    }

    /** */
    public AdapterManager() {open();}

    public static void open() {
        if(MS == null) {
            MS = new MorphiaService();
            ADAPTER_DAO = new LGAdapterDAOImpl(LGAdapterModel.class, MS.getDatastore());
            ADAPTER_CACHE = new HashMap<String, LGAdapterModel>();
        }
    }

    public static void close() {
        try {
            if(MS != null) {
                MS.close();
                ADAPTER_DAO = null;
                ADAPTER_CACHE = null;
            }
        }
        catch(Exception e) {
            log.error("Error trying to close mongo connection.");
            e.printStackTrace();
        }
    }

    /** *
     * @param adapter Adapter to save to adapter data access object
     */
    public static void addAdapter(LGAdapterModel adapter) {
        ADAPTER_DAO.save(adapter);
    }

    /**
     * @param id Adapter ID to fetch adapter
     * @return Returns LGAdapterModel for the input adapter ID
     * */
    public static LGAdapterModel getAdapterById(String id) {
        // Look in Cache first
        if (ADAPTER_CACHE.containsKey(id)) {
            return (LGAdapterModel) ADAPTER_CACHE.get(id);
        } else {
            LGAdapterModel a = ADAPTER_DAO.getById(id);
            if (a != null) {
                ADAPTER_CACHE.put(id, a);
                return a;
            }
        }

        return null;
    }

    /**
     * @param adapter LGAdapterModel to delete from DAO
     * @return Returns 'true' if successful
     * */
    public boolean deleteAdapter(LGAdapterModel adapter) {
        ADAPTER_DAO.delete(adapter);
        return true;
    }

    /**
     * @return 'true' if delete works
     * */
    public static boolean deleteAll() {
        List<LGAdapterModel> adapters = ADAPTER_DAO.getAll();
        for(LGAdapterModel adapter : adapters) {
            ADAPTER_DAO.delete(adapter);
        }
        return true;
    }

    /**
     * Given the adapter UUID, returns the adapter name from the lemongrenade.adapters table
     * @param adapterId The adapter id to fetch name for
     * @return String adapterName
     */
    public static String getAdapterNameById(String adapterId) {
        LGAdapterModel lgAdapter = getAdapterById(adapterId);
        if (lgAdapter == null) {
            log.error("Unable to find adapter with ID "+adapterId);
            return null;
        }
        return lgAdapter.getName();
    }

    /** TODO: it would be better to call adapter.getQueueName()
     * @param adapterId The adapter ID to get queue name from
     * @return String name of adapter
     * */
    public static String getAdapterQueueNameById(String adapterId) {
        LGAdapterModel lgAdapter = getAdapterById(adapterId);
        return lgAdapter.getUniqueName();
    }

    /**
     * @return Returns a hashmap of UUID to Name
     * */
    public static HashMap<String,String> getAdapterList() {
        HashMap <String,String> ret = new HashMap();
        List<LGAdapterModel> adapters = ADAPTER_DAO.getAll();
        for(LGAdapterModel adapter : adapters) {
            ret.put(adapter.getId(), adapter.getName());
        }
        return ret;
    }

    /**
     * @return Returns a List of LGAdapterModel of all in adapter DAO
     * */
    public List<LGAdapterModel> getAll() {
        return ADAPTER_DAO.getAll();
    }

    /** Name-UID mapped to UID/type/hb
     * @return Returns JSONObject of names to JSONObjects of adapter models
     * */
    public JSONObject getAdapterListJson() {
        JSONObject jo = new JSONObject();
        List<LGAdapterModel> adapters = ADAPTER_DAO.getAll();
        for(LGAdapterModel a : adapters) {
            JSONObject d = a.toJson();
            jo.put(a.getUniqueName(),d);
        }
        return jo;
    }

    /**
     * helper method to extract adapter list from the job_config JSON blob.
     * Will throw an exception if any of the adapters are invalid.
     * (TODO: Create an invalidAdapterException)
     *
     * @param jobConfig JSONObject of adapter job config
     * @return Returns an ArrayList of Strings of all approved adapters
     * @throws Exception when an unknown adapter is in job request.
     */
    public static ArrayList<String> parseAdaptersListFromJobConfig(JSONObject jobConfig) throws Exception {
        return parseAdaptersListFromJobConfig(jobConfig, true);
    }

    //Parses adapter list from jobConfig. Throws an exception of an adapter isn't known by the system.
    public static ArrayList<String> parseAdaptersListFromJobConfig(JSONObject jobConfig, boolean toLower) throws Exception {
        ArrayList<String> approvedAdapters = safeParseAdaptersListFromJobConfig(jobConfig, toLower);
        String adaptersSystemKnowsAbout = getAdapterListNamesOnlyJson().toString();
        for (String a : approvedAdapters) {
            if (!adaptersSystemKnowsAbout.contains("\""+a.toLowerCase()+"\"")) {
                throw new Exception("Unknown Adapter in job request: "+a);
            }
        }
        return JSONUtils.toArrayList(approvedAdapters); //prevent duplicate entries for the same adapter
    }

    //Parses adapter list from jobConfig, doesn't throw an exception.
    public static ArrayList<String> safeParseAdaptersListFromJobConfig(JSONObject jobConfig, boolean toLower) {
        HashSet<String> approvedAdapters = new HashSet<>();
        if (jobConfig.has("adapters")) {
            JSONObject adapterListObject = jobConfig.getJSONObject("adapters");
            if (adapterListObject.names() != null) {
                for (int i = 0; i < adapterListObject.names().length(); i++) {
                    if(toLower == true) {
                        approvedAdapters.add(adapterListObject.names().getString(i).toLowerCase());
                    }
                    else {
                        approvedAdapters.add(adapterListObject.names().getString(i));
                    }
                }
            }
        }
        return JSONUtils.toArrayList(approvedAdapters); //prevent duplicate entries for the same adapter
    }

    /**
     * This populates Mongo table with the adapter information.
     * @param adapterId String of adapter ID
     * @param adapterName String of adapter name
     * @param graphQuery graph query
     * @param graphDepth graph depth
     * @param requiredKeys HashMap of required keys
     * @param maxNodesPerTask int for max nodes per task
     * @param extraParams JSONObject of extra params
     */
    public static void buildAdapterDataStructure(String adapterId, String adapterName
            , String graphQuery, int graphDepth
            , HashMap<String, String> requiredKeys, int maxNodesPerTask, JSONObject extraParams)
    {
        LGAdapterModel a = new LGAdapterModel(adapterId, adapterName, graphQuery, graphDepth, maxNodesPerTask, extraParams);
        // Only bother with requiredkeys if set
        if (requiredKeys != null) {
            a.setRequiredKeys(requiredKeys);
        }
        ADAPTER_DAO.save(a);
    }

    /**
     * @param adapterId Adapter ID to set
     * @param t long of time of last heart beat
     * */
    public static void setHeartBeat(String adapterId, long t) {
        LGAdapterModel lgAdapter = ADAPTER_DAO.getById(adapterId);
        lgAdapter.setLastheartBeat(t);
        ADAPTER_DAO.save(lgAdapter);
    }

    /**
     * @param adapterId String of adapter ID
     * @param status int indicating adapter status
     * */
    public void setStatus(String adapterId, int status) {
        LGAdapterModel lgAdapter = ADAPTER_DAO.getById(adapterId);
        if (status == lgAdapter.getStatus()) { return; }   // Only bother if it changed
        lgAdapter.setStatus(status);
        ADAPTER_DAO.save(lgAdapter);
    }

    /**
     * @param adapterId String of adapter ID
     * @param taskId String of task ID to set
     * */
    public void setCurrentTaskId(String adapterId, String taskId) {
        LGAdapterModel lgAdapter = ADAPTER_DAO.getById(adapterId);
        lgAdapter.setCurrentTaskId(taskId);
        ADAPTER_DAO.save(lgAdapter);
    }

    /**
     * @param adapterId String of adapter ID to unset
     */
    public void unsetCurrentTaskId(String adapterId) {
        LGAdapterModel lgAdapter = ADAPTER_DAO.getById(adapterId);
        lgAdapter.setCurrentTaskId("");
        ADAPTER_DAO.save(lgAdapter);
    }

    /**
     * @param adapterId String of adapter ID to increment task count
     */
    public static void incrementTaskCount(String adapterId) {
        LGAdapterModel lgAdapter = ADAPTER_DAO.getById(adapterId);
        int tc = lgAdapter.getTaskCount();
        tc++;
        lgAdapter.setTaskCount(tc);
        ADAPTER_DAO.save(lgAdapter);
    }

    /**
     * Loop through all lemongrenade.adapters and see if hb has crossed a threshold
     *   set it to HB_FAILED
     *   OR if HB_FAILED and heartbeat has returned set back to ONLINE
     */
    private void checkAdapterHeartBeat() {
        List<LGAdapterModel> adapters = ADAPTER_DAO.getAll();
        for(LGAdapterModel adapter : adapters) {
           long diffTime = System.currentTimeMillis() - adapter.getLastHeartBeat();
            if (diffTime > MAX_ADAPTER_HEARTBEAT_TIME) {
                log.error("Adapter has gone over max heartbeat time "+diffTime+"/"+MAX_ADAPTER_HEARTBEAT_TIME);
                // TODO: update status? what to do here?
            } else {
                // TODO f status is HB_FAILED, see if heartbeat is back and then set the status back
            }
        }
    }

    /**
     * Give an adapter name, such as 'helloworld' , 'plusbang' or whatever other
     * adapter there might be. It looks for a list of all registed lemongrenade.adapters by that name in the
     * mongo "lemongrenade.adapters' table
     *
     * After building that list, it then looks at several metrics for each adapter
     * last heartbeat, 'load', and currentTaskCount and returns the "best" available adapter
     * that it can find
     * @param  adapterName - the "name" or "type" of the adapter we are looking
     * @return String adapterId
     */
    public static String findBestAdapterByAdapterName(String adapterName) {
        // Loop through the lemongrenade.adapters table, building a list of ALL
        // lemongrenade.adapters IDs that have the name 'adapterName'
        ArrayList<String> matchingAdapterList = new ArrayList<String>();
        List<LGAdapterModel> adapters = ADAPTER_DAO.getAll();
        for(LGAdapterModel a : adapters) {
            if (adapterName.equalsIgnoreCase(a.getName())) {
                if (a.getStatus() != LGAdapterModel.STATUS_OFFLINE) {
                    matchingAdapterList.add(a.getId());
                }
            }
        }

        // TODO: No matches, what do we do?
        if (matchingAdapterList.size() == 0) {
            // TODO: throw exception?
            return "";
        }
        /// If we find only one adapter that matches, just return that
        if (matchingAdapterList.size() == 1) { return matchingAdapterList.get(0); }

        // Now search the adaptersdata for each in the list and see what is best option
        for (String id: matchingAdapterList) {
            // TODO: Finish this logic, right now,just returning first match we come across
            return id;
        }
        return ""; //TODO: Throw exception?
    }


    //  Take the graphquery given by the adapter writer and append depth if we have it.
    //  Eg.  query = "n(status~/new/)"  and depth =3
    //       "n(status~/new/),1(depth<=3)"
    //       else
    //       "n(status~/new/)"
    /**
     * @param adapterId String of adapter ID
     * @param job LGJob
     * @param applyDepth boolean. 'true' to apply depth
     * @return Returns graph query
     */
    public static String getGraphQueryForAdapter(String adapterId, LGJob job, boolean applyDepth) {
        LGAdapterModel lgAdapter = getAdapterById(adapterId);
        if (lgAdapter == null) {
            // TODO: throw exception
            log.error("Unable to find graphQuery for adapter ["+adapterId+"]");
            return "";
        }
        // Append depth to the query if we have it (and if we are told to do so)
        int graphDepth = lgAdapter.getGraphDepth(); //only apply depth for adapters with positive graph depth
        if (applyDepth && graphDepth >= 0) {
            int depth = findDepth(adapterId, job);
            if (depth >= 0) {
                String rQuery = lgAdapter.getGraphQuery() + ",1(depth<=" + depth + ")";
                return rQuery;
            }
        }
        return lgAdapter.getGraphQuery();
    }

    /**
     * Returns -1 if can't find a depth
     *
     * Depth:
     *     if
     *          // If job_config gave us a depth for adapterName
     *          job_config()."depth_adapterName= 2"
     *     else
     *          // job_config global depth
     *          job_config()."depth = 1"
     *     else
     *          // adapter writer gave us a depth to use
     *          adapter.getDepth()
     *     else
     *          // don't append a depth
     *          no_depth
     *
     * @param adapterId
     * @param job
     * @return
     */
    private static int findDepth(String adapterId, LGJob job) {
        LGAdapterModel lgAdapter = getAdapterById(adapterId);
        JSONObject config = job.getJobConfigAsJSON();
        if (lgAdapter == null) {
            log.warn("Unable to find adapter in findDepth for adapterId:"+adapterId);
        } else {
            // Check for job_config { adapters : adaptername: { depth = 3 } }
            if ((config != null) && (config.has("adapters"))) {
                JSONObject adapters = config.getJSONObject("adapters");
                if (adapters.has(lgAdapter.getName())) {
                    JSONObject adapter = adapters.getJSONObject(lgAdapter.getName());
                    if (adapter.has("depth")) {
                        return adapter.getInt("depth");
                    }
                }
            }
        }

        // Look for job_config "default depth level"    {   depth = 3 }
        if ((config != null) && (config.has("depth"))) {
            return config.getInt("depth");
        }

        // If the adapter writer gave us a depth, we use it
        if (lgAdapter != null) {
            int d = lgAdapter.getGraphDepth();
            if (d >= 1) {
                return d;
            }
        }

        // return a NO_DEPTH values
        return - 1;
    }

    /**
     * For INTERNAL_MODE usage
     *
     * @param diff JSONObject list of attributes to match for job
     * @return List of uniqueadapters IDs that match the 'diff' of requiredKeys
     */
    public static List<String> buildUniqueAdapterListBasedOnRequiredKeys(JSONObject diff) {
        List<String> uniqueAdapterList = new ArrayList<>();

        List<LGAdapterModel> adapters = ADAPTER_DAO.getAll();
        for(LGAdapterModel a : adapters) {
            if (a.getRequiredKeys() != null) {
                JSONArray requiredAttrs = new JSONArray(a.getRequiredKeys().keySet());
                for (Object attr : requiredAttrs) {
                    if (diff.has(attr.toString())) {
                        String requiredValue = a.getRequiredKeys().get(attr);
                        if (requiredValue == null) {
                            log.error("Unable to find requiredValue for adapter[" +a+ "] Key[" + attr.toString() + "]");
                        } else {
                            String foundValue = diff.getString(attr.toString());
                            Pattern p = Pattern.compile(requiredValue.toString());
                            Matcher m = p.matcher(foundValue);
                            if (m.matches()) {
                                String adapterType =a.getName();
                                if (!uniqueAdapterList.contains(adapterType)) {
                                    uniqueAdapterList.add(adapterType);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Now look for a suitable adapter ID for each adapter type/name
        List<String> uniqueAdapterListById = new ArrayList<>();
        for(String adapterType: uniqueAdapterList) {
            String adapterId = findBestAdapterByAdapterName(adapterType);
            uniqueAdapterListById.add(adapterId);
        }
        return uniqueAdapterListById;
    }

    /**
     * Returns a clean list of available adapter names available without UUID markup.
     * If there's more than one type of adapter available, the name is only returned once,
     * since the end user does not care about how many adapters are available only what
     * ones are available.
     *
     * For example, if there exists:
     *    0000-0000-0000-0000-00001-Adapter1 , 0000-0000-0000-0000-00002-Adapter1, 0000-0000-0000-0000-00003-Adapter2
     *
     * This function will return
     *     JSONArray("Adapter1", "Adapter2")
     *
     * @return JSONArray
     */
    public static JSONArray getAdapterListNamesOnlyJson() {
        JSONArray ja = new JSONArray();
        HashMap<String,String> list = getAdapterList();
        list.forEach((k, v) -> {
            if (!ja.toString().contains("\"" + v + "\"")) {
                ja.put(v.toLowerCase());
            }
        });
        return ja;
    }

    /**
     * @param args Unused. Standard for 'main' function.
     * */
    public static void main(String[] args) {
        /** See JUNIT for more information */

        AdapterManager.deleteAll();

        ArrayList req2= new ArrayList<String>();
        req2.add(new JSONObject().put("status",".*"));
        req2.add(new JSONObject().put("someother",".*"));
        LGAdapterModel a1 = new LGAdapterModel("001","testadapter","query1",1,0, new JSONObject());
        HashMap<String, String> requiredKeys = new HashMap<String, String>();
        requiredKeys.put("hello", ".*");
        requiredKeys.put("hello2", ".*");
        a1.setRequiredKeys(requiredKeys);
        AdapterManager.addAdapter(a1);

        LGAdapterModel a2 = new LGAdapterModel("002","testadapter","query1",2,0, new JSONObject());
        a2.setRequiredKeys(requiredKeys);
        AdapterManager.addAdapter(a2);


        LGAdapterModel a3 = new LGAdapterModel("003","testadapter3","query3",3,0, new JSONObject());
        AdapterManager.addAdapter(a3);

        // Test required keys
        JSONObject diff = new JSONObject("{\"type\":\"id\",\"value\":\"e2ec2796-8d5f-404d-83dd-f839b8d3874c\",\"hello\":\"world\"}");
        List adapterList= AdapterManager.buildUniqueAdapterListBasedOnRequiredKeys(diff);
        System.out.println(adapterList.toString());

        AdapterManager.deleteAll();
        AdapterManager.close();
    }
}                                                                                                                      ;
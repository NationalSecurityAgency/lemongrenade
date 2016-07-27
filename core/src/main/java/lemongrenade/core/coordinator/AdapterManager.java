package lemongrenade.core.coordinator;

import lemongrenade.core.models.LGJob;
import lemongrenade.core.util.LGConstants;
import lemongrenade.core.util.LGProperties;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * Adapters register to redis when spun up.
 *
 * lemongrenade.adapters table
 *       UUID:name     -> Maps adapterId to adapterName
 *
 * adaptersdata table
 *      adapter_UUID:requiredkeys  -> required_keys
 *      adapter_UUID:hb            -> last_heartbeat time from adapter
 *      adapter_UUID:starttime     -> timestamp of when adapter came online
 *      adapter_UUID:tc            -> amount of tasks adapter has handled since startup time
 *
 * AdapterManager doesn't ever make changes to the REDIS fields. It is a read-only
 * situation. It's up to the adapter itself to write new data.
 *
 * Contains a snapshot of the lemongrenade.adapters from REDIS. If we think an adapter has
 * gone offline, we can mark it in memory/send log/ or send to another adapter
 * (if running multiple instances of the same adapter type)
 *
 * Adapters don't keep an action count because they are only ever processing one
 * task at a time. Another method would be just look at the size of the rabbitmq
 * for each adapter?
 *
 * Each lemongrenade.core.coordinator can only see the jobs it has given to a adapter. So, a future
 * update should look at the RabbitMQ size + the current action count to get an idea
 * of load_balancing metric.
 *
 * Coordinators decide on their own the status of an Adapter and don't share that information
 * amongst themselves. This keeps the coordinators simple and separate from one another.
 *
 */
public class AdapterManager {
    private final Logger log = LoggerFactory.getLogger(getClass());
    final static int MAX_ADAPTER_HEARTBEAT_TIME = LGProperties.getInteger("max_adapter_heartbeat_time",300);
    private transient Jedis jedis;

    public AdapterManager() {
        jedis = new Jedis(LGProperties.get("redis.hostname"));
    }

    /**
     * Given the adapter UUID, returns the adapter name from the redis lemongrenade.adapters table
     *
     * @param adapterId
     * @return String adapterName
     */
    public String getAdapterNameById(String adapterId) {
        String adapterName = jedis.hget(LGConstants.LGADAPTERS, adapterId);
        return adapterName;
    }

    /** TODO: it would be better to call adapter.getQueueName() */
    public String getAdapterQueueNameById(String adapterId) {
        String adapterName = jedis.hget(LGConstants.LGADAPTERS, adapterId);
        return adapterName+"-"+adapterId;
    }

    public HashMap<String,String> getAdapterList() {
        HashMap <String,String> ret = new HashMap<String,String>();
        jedis.hkeys(LGConstants.LGADAPTERS).forEach(adapter -> {
            String name = jedis.hget(LGConstants.LGADAPTERS, adapter);
            ret.put(adapter,name);
        });
        return ret;
    }

    /** Name-UID mapped to UID/type/hb */
    public JSONObject getAdapterListJson() {
        JSONObject jo = new JSONObject();

        jedis.hkeys(LGConstants.LGADAPTERSDATA).forEach(adapter -> {
            if (adapter.endsWith(":hb")) {
                String s[] = adapter.split(":");
                String adapterId = s[0];
                long lastHeartBeat = Long.parseLong(jedis.hget(LGConstants.LGADAPTERSDATA, adapter));
                long diffTime = (System.currentTimeMillis() - lastHeartBeat)/ 1000;
                JSONObject d = new JSONObject();
                d.put("id",adapterId);
                String name = getAdapterNameById(adapterId);
                String fullname= name + "-" +adapterId;
                d.put("name",name);
                d.put("last_hb",diffTime);
                d.put("uptime","");
                d.put("taskcount","0");  // TODO: Pull the right task count
                d.put("graphquery","");
                d.put("graphdepth","");
                jo.put(fullname,d);
            }
        });

        // Adapter type
        jedis.hkeys(LGConstants.LGADAPTERS).forEach(adapter -> {
            String s[] = adapter.split(":");
            String adapterId = s[0];
            String name = getAdapterNameById(adapterId);
            String fullname = name + "-" + adapterId;
            JSONObject j = (JSONObject) jo.get(fullname);
            j.put("type", jedis.hget(LGConstants.LGADAPTERS, adapter));
        });

        // Starttime, Endtime, TaskCount, graphQuery...
        jedis.hkeys(LGConstants.LGADAPTERSDATA).forEach(adapter -> {
            if (adapter.endsWith(":starttime")) {
                String s[] = adapter.split(":");
                String adapterId = s[0];
                long starttime = Long.parseLong(jedis.hget(LGConstants.LGADAPTERSDATA, adapter));
                long diffTime = (System.currentTimeMillis() - starttime)/1000;
                String name = getAdapterNameById(adapterId);
                String fullname= name + "-" +adapterId;
                JSONObject d = (JSONObject) jo.get(fullname);
                d.put("uptime",diffTime);
            }
            else if (adapter.endsWith(":tc")) {
                String s[] = adapter.split(":");
                String adapterId = s[0];
                String taskCount = jedis.hget(LGConstants.LGADAPTERSDATA, adapter);
                String name = getAdapterNameById(adapterId);
                String fullname= name+"-" + adapterId;
                JSONObject d = (JSONObject) jo.get(fullname);
                d.put("taskcount",taskCount);
            }
            else if (adapter.endsWith(":graphquery")) {
                String s[] = adapter.split(":");
                String adapterId = s[0];
                String graphQuery = jedis.hget(LGConstants.LGADAPTERSDATA, adapter);
                String name = getAdapterNameById(adapterId);
                String fullname= name + "-" + adapterId;
                JSONObject d = (JSONObject) jo.get(fullname);
                d.put("graphquery",graphQuery);
            }
            else if (adapter.endsWith(":graphdepth")) {
                String s[] = adapter.split(":");
                String adapterId = s[0];
                String graphDepth = jedis.hget(LGConstants.LGADAPTERSDATA, adapter);
                String name = getAdapterNameById(adapterId);
                String fullname= name + "-" + adapterId;
                JSONObject d = (JSONObject) jo.get(fullname);
                d.put("graphdepth",graphDepth);
            }
        });
        return jo;
    }

    /**
     * helper method to extract adapter list from the job_config JSON blob.
     * Will throw an exception if any of the adapters are invalid.
     * (TODO: Create an invalidAdapterException
     *
     * @param jobConfig
     * @return
     */
    public ArrayList<String> parseAdaptersListFromJobConfig(JSONObject jobConfig)
            throws Exception
    {
        ArrayList<String> approvedAdapters = new ArrayList<>();
        if (jobConfig.has("adapters")) {
            JSONObject adapterListObject = jobConfig.getJSONObject("adapters");
            if (adapterListObject.names() != null) {
                for (int i = 0; i < adapterListObject.names().length(); i++) {
                    approvedAdapters.add(adapterListObject.names().getString(i).toLowerCase());
                }
            }
        }
        String adaptersSystemKnowsAbout = getAdapterListNamesOnlyJson().toString();
        for (String a : approvedAdapters) {
            if (!adaptersSystemKnowsAbout.contains("\""+a+"\"")) {
                throw new Exception("Unknown Adapter in job request: "+a);
            }
        }
        return approvedAdapters;
    }

    /**
     * This populates Redis with the adapter information. In the past, this was being performed
     * down at the rabbitmqSpout, which shouldn't have been handling it. Moved here to adapterManager
     * because we will most likely be getting rid of Redis at some point.
     */
    public void buildAdapterDataStructure(String adapterId, String adapterName
                ,HashMap<String, String> requiredTuples
                ,String graphQuery, int graphDepth) {
        List<String> requiredKeys;

        Set<String> keySet = requiredTuples.keySet();
        requiredKeys = new ArrayList<String>();
        requiredKeys.addAll(keySet);

        // Populate ID->Name in lemongrenade.adapters table
        jedis.hset(LGConstants.LGADAPTERS,adapterId, adapterName);

        // Populate adaptersdata fields
        String requiredKeysId = adapterId + ":requiredkeys";
        jedis.hset(LGConstants.LGADAPTERSDATA, requiredKeysId, new JSONArray(requiredKeys).toString());

        // Populate regex values for each key, which is adapterId:key:keyvalue
        for(int i = 0; i < requiredKeys.size(); i++) {
            String key = requiredKeys.get(i);
            String value = requiredTuples.get(key);
            String newId = adapterId+":requiredkeys:"+key+":keyvalue";
            jedis.hset(LGConstants.LGADAPTERSDATA, newId, value);
        }

        // Populate adapterQuery
        String graphQueryKey = adapterId+":graphquery";
        jedis.hset(LGConstants.LGADAPTERSDATA, graphQueryKey, graphQuery);

        // Populate stats - empty at this stage
        String statsKey = adapterId+ ":stats";
        jedis.hset(LGConstants.LGADAPTERSDATA, statsKey, "{}");

        // Populate adapterDepth
        String graphDepthKey = adapterId+ ":graphdepth";
        if (jedis.exists(graphDepthKey)) {
            jedis.hdel(graphDepthKey);
        }
        if (graphDepth >= 1) {
            String graphDepthString = ""+graphDepth;
            jedis.hset(LGConstants.LGADAPTERSDATA, graphDepthKey, graphDepthString);
        }

        // Populate heartbeat, startime in adaptersdata
        String onlineKey = adapterId+ ":starttime";
        long currentTime = System.currentTimeMillis();
        jedis.hset(LGConstants.LGADAPTERSDATA, onlineKey, Long.toString(currentTime));
        String hbKey = adapterId+ ":hb";
        jedis.hset(LGConstants.LGADAPTERSDATA, hbKey, Long.toString(currentTime));
    }

    /** Helper method that deletes all redis keys for this adapter */
    private void deleteFromDataStructure(String adapterId ) {
        // Delete from lemongrenade.adapters table (ID->Name)
        jedis.hdel(LGConstants.LGADAPTERS, adapterId);
        // Delete adaptersdata fields
        // TODO: Scan and delete keys and keys:keyvalues
       /* String requiredKeysKey = new String(adapterId+":requiredkeys");
        jedis.hdel(LGConstants.LGADAPTERSDATA,requiredKeysKey);
        for(int i = 0; i < requiredKeys.size(); i++) {
            String key = requiredKeys.get(i);
            String value = requiredTuples.get(key);
            jedis.hdel(requiredKeysKey, key, value);
        }
        */
        // Delete GraphQuery
        String graphQueryKey = adapterId+":graphquery";
        jedis.hdel(LGConstants.LGADAPTERSDATA, graphQueryKey);
        // Delete heartbeat/starttime
        String onlineKey = adapterId+ ":starttime";
        jedis.hdel(LGConstants.LGADAPTERSDATA, onlineKey);
        String hbKey = adapterId+ ":hb";
        jedis.hdel(LGConstants.LGADAPTERSDATA, hbKey);
    }

    public void setCurrentTaskId(String adapterId, String taskId) {
        String index = adapterId + ":taskid";
        jedis.hset(LGConstants.LGADAPTERSDATA,index,taskId);
    }

    public void unsetCurrentTaskId(String adapterId) {
        String index = adapterId + ":taskid";
        jedis.hset(LGConstants.LGADAPTERSDATA,index,"");
    }

    public void incrementTaskCount(String adapterId) {
        String index = adapterId + ":tc";
        Integer taskCount = 0;
        try {
            taskCount = new Integer(jedis.hget(LGConstants.LGADAPTERSDATA, index));
        }
        catch (Exception e) {
            taskCount = 0;
        }
        taskCount = taskCount + 1;
        jedis.hset(LGConstants.LGADAPTERSDATA,index,taskCount.toString());
    }

    /**
     * Loop through all lemongrenade.adapters and see if hb has crossed a threshold
     *   set it to HB_FAILED
     *   OR if HB_FAILED and heartbeat has returned set back to ONLINE
     */
    private void checkAdapterHeartBeat() {
        jedis.hkeys(LGConstants.LGADAPTERSDATA).forEach(adapter -> {
            if (adapter.endsWith(":hb")) {
                // Data is UID:type, here we are looking for UUID:hb
                String s[] = adapter.split(":");
                String adapterId = s[0];
                long lastHeartBeat = Long.parseLong(jedis.hget(LGConstants.LGADAPTERSDATA, adapter));
                log.info(" Found heartbeat "+lastHeartBeat+" for "+adapterId);
                long diffTime = System.currentTimeMillis() - lastHeartBeat;
                if (diffTime > MAX_ADAPTER_HEARTBEAT_TIME) {
                    log.error("Adapter has gone over max heartbeat time "+diffTime+"/"+MAX_ADAPTER_HEARTBEAT_TIME);
                    // TODO: update status? what to do here?
                }
                // If status is HB_FAILED, see if heartbeat is back
            }
        });
    }

    /**
     * Give an adapter name, such as 'helloworld' , 'plusbang' or whatever other
     * adapter there might be. It looks for a list of all registed lemongrenade.adapters by that name in the
     * redis "lemongrenade.adapters' table
     *
     * After building that list, it then looks at several metrics for each adapter
     * last heartbeat, 'load', and currentTaskCount and returns the "best" available adapter
     * that it can find
     *
     * @return String adapterId
     */
    public String findBestAdapterByAdapterName(String adapterName) {

        // Loop through the lemongrenade.adapters table, building a list of ALL lemongrenade.adapters IDs that have the name 'adapterName'
        ArrayList<String> matchingAdapterList = new ArrayList<String>();
        Set<String> adapters = jedis.hkeys(LGConstants.LGADAPTERS);
        for (String adapter : adapters) {
            String inName = jedis.hget(LGConstants.LGADAPTERS, adapter);
            if (adapterName.equalsIgnoreCase(inName)) {
               matchingAdapterList.add(adapter);
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

    /** Helper method */
    private String getAdapterTypeFromAdapterRedisString(String adapter) {
        String[] s = adapter.split(":");
        //String adapterType = s[0];
        return s[0];
    }

    /**
     *  Take the graphquery given by the adapter writer and append depth if we have it.
     *
     *  Eg.  query = "n(status~/new/)"  and depth =3
     *       "n(status~/new/),1(depth<=3)"
     *       else
     *       "n(status~/new/)"
     */
    public String getGraphQueryForAdapter(String adapterName, String adapterId, LGJob job, boolean applyDepth) {
        String key = adapterId+":graphquery";
        String graphQuery = jedis.hget(LGConstants.LGADAPTERSDATA, key);
        if (graphQuery == null) {
            return "";
        }

        // Append depth to the query if we have it
        if (applyDepth) {
            int depth = findDepth(adapterName, adapterId, job);
            if (depth >= 1) {
                String rQuery = graphQuery + ",1(depth<=" + depth + ")";
                return rQuery;
            }
        }
        return graphQuery;
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
     *
     *
     * @param adapterName the name of adapter
     * @param adapterId
     * @param job
     * @return
     */
    private int findDepth(String adapterName, String adapterId, LGJob job) {
        JSONObject config = job.getJobConfigAsJSON();
        // Check for job_config { adapters : adaptername: { depth = 3 } }
        if ((config != null) && (config.has("adapters"))) {
            JSONObject adapters = config.getJSONObject("adapters");
            if (adapters.has(adapterName)) {
                JSONObject adapter = adapters.getJSONObject(adapterName);
                if (adapter.has("depth")) {
                    return adapter.getInt("depth");
                }
            }
        }

        // Look for job_config "default depth level"    {   depth = 3 }
        if ((config != null) && (config.has("depth"))) {
            return config.getInt("depth");
        }

        // If the adapter writer gave us a depth, we use it
        String key2 = adapterId+":graphdepth";

        String adapterWriterDepthStr = jedis.hget(LGConstants.LGADAPTERSDATA, key2);
        if ((adapterWriterDepthStr != null) && (!adapterWriterDepthStr.equals(""))) {
            try {
                int d = Integer.parseInt(adapterWriterDepthStr);
                if (d >= 1) {
                    return d;
                }
            } catch (NumberFormatException e) {
            }
        }
        // return a NO_DEPTH values
        return - 1;
    }

    /**
     * For INTERNAL_MODE usage
     *
     * @param diff JSONObject list of attributes to match for job
     * @return List of uniqueadapters that match the 'diff' of requiredKeys
     */
    public List<String> buildUniqueAdapterListBasedOnRequiredKeys (JSONObject diff) {
        List<String> uniqueAdapterList = new ArrayList<>();
        if (jedis.exists(LGConstants.LGADAPTERSDATA)) {
            // First Build a list of UNIQUE lemongrenade.adapters to send to. For example, if we are running
            // multiple instances of the same adapter, we only want that adapter listed once.
            jedis.hkeys(LGConstants.LGADAPTERSDATA).forEach(adapter -> {
                // Only look for "requiredkeyss" ignore other redis data keys
                if (adapter.endsWith(":requiredkeys")) {
                    JSONArray requiredAttrs = new JSONArray(jedis.hget(LGConstants.LGADAPTERSDATA, adapter));
                     for (Object attr : requiredAttrs) {
                        if (diff.has(attr.toString())) {
                            String newId = adapter+":"+attr+":keyvalue";
                            Object requiredValue = jedis.hget(LGConstants.LGADAPTERSDATA, newId);
                            if (requiredValue == null) {
                                log.error("Unable to find requiredValue for adapter[" + adapter + "] Key[" + attr.toString() + "]");
                            } else {
                                String foundValue = diff.getString(attr.toString());
                                Pattern p = Pattern.compile(requiredValue.toString());
                                Matcher m = p.matcher(foundValue);
                                if (m.matches()) {
                                    String adapterType = getAdapterTypeFromAdapterRedisString(adapter);
                                    uniqueAdapterList.add(adapterType);
                                }
                            }
                        }
                    }
                }
            });
        }
        return uniqueAdapterList;
    }

    /**
     *
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
     * @Returns JSONArray
     */
    public JSONArray getAdapterListNamesOnlyJson() {
        JSONArray ja = new JSONArray();
        HashMap<String,String> list = getAdapterList();

        list.forEach((k,v) -> {
            if (!ja.toString().contains("\""+v+"\"")) {
                ja.put(v.toLowerCase());
            }
        });
        return ja;
    }

    /** move to junit
     *
     */
    public static void main(String[] args) {
        AdapterManager am = new AdapterManager();
        // Populate redis with fake info
        am.jedis.flushAll();  // Clear ALL THE TABLES
        am.jedis.hset(LGConstants.LGADAPTERS,"001","testadapter1");
        am.jedis.hset(LGConstants.LGADAPTERS,"002","testadapter2");
        am.jedis.hset(LGConstants.LGADAPTERS,"003","testadapter2");       // two testadapter2 on purpose
        String t = new String(""+System.currentTimeMillis());
        am.jedis.hset(LGConstants.LGADAPTERSDATA,"001:hb",t);
        am.jedis.hset(LGConstants.LGADAPTERSDATA,"002:hb",t);
        am.jedis.hset(LGConstants.LGADAPTERSDATA,"003:hb","0001");
        ArrayList req= new ArrayList<String>(); req.add("hello");
        am.jedis.hset(LGConstants.LGADAPTERSDATA,"001:requiredkeys",new JSONArray(req).toString());
        String newId = "001"+":requiredkeys:"+"hello"+":keyvalue";
        am.jedis.hset(LGConstants.LGADAPTERSDATA, newId, ".*");
        ArrayList req2= new ArrayList<String>(); req2.add("status"); req2.add("someother");
        am.jedis.hset(LGConstants.LGADAPTERSDATA,"002:requiredkeys",new JSONArray(req2).toString());
        String newId2 = "002"+":requiredkeys:"+"status"+":keyvalue";
        am.jedis.hset(LGConstants.LGADAPTERSDATA, newId2, ".*");
        String newId3 = "002"+":requiredkeys:"+"someother"+":keyvalue";
        am.jedis.hset(LGConstants.LGADAPTERSDATA, newId3, ".*");
        am.jedis.hset(LGConstants.LGADAPTERSDATA,"003:requiredkeys",new JSONArray(req2).toString());
        String newId4 = "003"+":requiredkeys:"+"status"+":keyvalue";
        am.jedis.hset(LGConstants.LGADAPTERSDATA, newId4, ".*");
        String newId5 = "003"+":requiredkeys:"+"someother"+":keyvalue";
        am.jedis.hset(LGConstants.LGADAPTERSDATA, newId5, ".*");

        // call findBestAdapterByAdapterName()
        String id = am.findBestAdapterByAdapterName("testadapter1");
        // assert
        System.out.println("1: Found "+id);
        if (id.equals("001")) {
            System.out.println("Found right adapter for testadapter1");
        } else {
            System.out.println("FAIL: looking for testadapter1");
        }
        String id2= am.findBestAdapterByAdapterName("testadapter2");
        System.out.println("2: Found "+id2);
        if (id2.equals("002")) {
            System.out.println("Found right adapter for testadapter2");
        } else {
            System.out.println("FAIL: looking for testadapater2");
        }

        JSONObject diff = new JSONObject("{\"type\":\"id\",\"value\":\"e2ec2796-8d5f-404d-83dd-f839b8d3874c\",\"hello\":\"world\"}");
        List adapterList= am.buildUniqueAdapterListBasedOnRequiredKeys(diff);
        System.out.println("Reqturned list :"+adapterList.toString());
        if (adapterList.get(0).equals("001")) {
            System.out.println("Success finding adapter for hello:world");

        } else {
            System.out.println("FAIL: finding correct adapter list for hello:world");
        }
        System.out.println(" Unique list returns "+adapterList.toString());

        JSONObject diff2 = new JSONObject("{\"type\":\"id\",\"value\":\"e2ec2796-8d5f-404d-83dd-f839b8d3874c\",\"status\":\"new\"}");
        List adapterList2= am.buildUniqueAdapterListBasedOnRequiredKeys(diff2);
        System.out.println(adapterList2.toString());
        if (adapterList2.contains("002") && adapterList2.contains("003") && (!adapterList2.contains("001"))) {
            System.out.println("Success finding adapter for status:new");

        } else {
            System.out.println("FAIL: finding correct adapter list for status:new");
        }
        System.out.println(" Unique list returns "+adapterList2.toString());

        System.out.println(am.getAdapterListNamesOnlyJson());

        am.jedis.flushAll();  // Clear ALL THE TABLES
    }
}                                                                                                                      ;
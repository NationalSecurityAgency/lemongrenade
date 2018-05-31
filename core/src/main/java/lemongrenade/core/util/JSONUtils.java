package lemongrenade.core.util;

import org.json.JSONArray;
import org.json.JSONObject;
import java.io.IOException;
import java.util.*;

public class JSONUtils {

    /**
     * Tracks either additions or modifications to the existing JSON object.
     * Does not track deletions.
     * Does not track JSONArray.
     * Does not recurse in to nested JSON.
     * @param prev - the previous JSON Object
     * @param curr - the current JSON Object
     * @return
     */
    public static JSONObject diff (JSONObject prev, JSONObject curr){
        JSONObject diff = new JSONObject();
        for (String attr : curr.keySet()){
            // added
            if (!prev.has(attr)){
                diff.put(attr, curr.get(attr));
            }
            // modified
            if (prev.has(attr) && !prev.get(attr).equals(curr.get(attr))){
                diff.put(attr, curr.get(attr));
            }
        }

        return diff;
    }

    /** */
    public static JSONObject apply(JSONObject json, JSONObject patch){
        JSONObject response = new JSONObject();
        for (String attr: json.keySet()){
            response.put(attr, json.get(attr));
        }
        for (String attr : patch.keySet()){
            response.put(attr, patch.get(attr));
        }
        return response;
    }

    /**
     * Splits an existing JSONArray into smaller pieces. Returns a JSONArray of JSONArrays based on the chunkSize.
     * if Chunksize less than or equal to 1, no splitting occurs, the whole array will be at return(0).
     *
     * @param chunkSize  Size to split the existing JSONArray into
     * @param nodes      original JSONArray
     * @return  JSONArray of JSONArray's
     */
    public static JSONArray splitJsonArray(int chunkSize, JSONArray nodes) {
        JSONArray nodeGroups = new JSONArray();

        if (nodes == null) { return nodeGroups; }
        // If max size <= 0, we don't split at all
        if (chunkSize <= 0 ) {
            nodeGroups.put(0, nodes);
            return nodeGroups;
        }
        int numberOfChunks = (int)Math.ceil((double)nodes.length()/chunkSize);
        for (int i = 0; i < numberOfChunks; i++) {
            int start = i * chunkSize;
            int tmpLength = Math.min(nodes.length() - start, chunkSize);
            JSONArray tmpArray = new JSONArray();
            int tmpCount = 0;
            for (int x = start; x < (start+tmpLength); x++) {
                tmpArray.put(tmpCount++,nodes.get(x));
            }
            nodeGroups.put(i, tmpArray);
        }
        return nodeGroups;
    }

    //Checks all keys for 'sub' and swaps it with 'replace'
    public static void keyChange(Map<String, Object> map, String regex, String replace) {
        Iterator iterator = map.keySet().iterator();
        while(iterator.hasNext()) {
            String key = (String) iterator.next();
            Object value = map.get(key);
            String newKey = key.replaceAll(regex, replace);
            if(!key.equals(newKey)) { //update key
                map.remove(key);
                map.put(newKey, value);
            }
            if(value instanceof Map) {
                keyChange((Map)value, regex, replace);
            }
        }
    }

    public static ArrayList toArrayList(Collection collection) {
        ArrayList list = new ArrayList();
        Iterator iterator = collection.iterator();
        while(iterator.hasNext()) {
            Object object = iterator.next();
            list.add(object);
        }
        return list;
    }

    //Converts items in a JSONObject to Map/ArrayList
    public static Map toMap(JSONObject input) {
        Map map = new HashMap();
        Iterator iterator = input.keys();
        while(iterator.hasNext()) {
            String key = (String) iterator.next();
            Object value = input.get(key);
            if(value instanceof JSONObject) {
                value = toMap((JSONObject)value);
            }
            else if(value instanceof JSONArray) {
                value = toArray((JSONArray) value);
            }
            map.put(key, value);
        }
        return map;
    }

    //converts elements in a JSONArray to Map/ArrayList
    public static ArrayList toArray(JSONArray jsonArray) {
        Iterator iterator = jsonArray.iterator();
        ArrayList array = new ArrayList();
        while(iterator.hasNext()) {
            Object value = iterator.next();
            if(value instanceof JSONObject) {
                value = JSONUtils.toMap((JSONObject)value);
            }
            else if(value instanceof JSONArray) {
                value = toArray((JSONArray)value);
            }
            array.add(value);
        }
        return array;
    }

    public static JSONArray toJson(Set set) {
        JSONArray array = new JSONArray();
        Iterator iterator = set.iterator();
        while(iterator.hasNext()) {
            String item = iterator.next().toString();
            array.put(item);
        }
        return array;
    }

    public static JSONObject toJson(Map map) {
        JSONObject jsonObject = new JSONObject();
        Iterator iterator = map.keySet().iterator();
        while(iterator.hasNext()) {
            Object key = iterator.next();
            Object value = map.get(key);
            jsonObject.put(key.toString(), value);
        }
        return jsonObject;
    }

    /** */
    public static void main(String[] args) throws IOException {
        JSONObject first = new JSONObject()
                .put("foo", "bar")
                .put("hodor", "hodor");

        JSONObject second = new JSONObject()
                .put("foo", "bar")
                .put("hodor", "hodor!")
                .put("hey", "listen");

        JSONObject patch = JSONUtils.diff(first, second);
        JSONObject patchedVer = JSONUtils.apply(first, patch);
        System.out.println(patchedVer);
    }
}

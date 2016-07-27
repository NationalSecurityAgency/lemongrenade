package lemongrenade.core.util;

import org.json.JSONArray;
import org.json.JSONObject;
import java.io.IOException;

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
        JSONArray r = new JSONArray();

        if (nodes == null) { return r; }
        // If max size <= 0, we don't split at all
        if (chunkSize <= 1 ) {
            r.put(0,nodes);
            return r;
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
            r.put(i,tmpArray);
        }
        return r;
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

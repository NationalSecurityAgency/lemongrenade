package lemongrenade.core.util;

import junit.framework.TestCase;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

public class JSONUtilsTest extends TestCase {

    /** */
    @Test
    public void testDiff() {
        JSONObject first = new JSONObject()
                .put("foo", "bar")
                .put("hodor", "hodor");

        JSONObject second = new JSONObject()
                .put("foo", "bar")
                .put("hodor", "hodor!")
                .put("hold", "door");

        JSONObject patch = JSONUtils.diff(first, second);
        JSONObject patchedVer = JSONUtils.apply(first, patch);
        assertEquals(patchedVer.toString(),"{\"foo\":\"bar\",\"hodor\":\"hodor!\",\"hold\":\"door\"}");
    }

    /** */
    @Test
    public void testSplitJsonArrayTest() {

        JSONArray nodes =  new JSONArray();
        nodes.put(1);nodes.put(2);nodes.put(3);nodes.put(4);nodes.put(5);nodes.put(6);nodes.put(7);
        System.out.println("Original nodes ="+nodes.toString());

        // Test Odd
        JSONArray result1 = JSONUtils.splitJsonArray(3, nodes);
        assertEquals(result1.get(0).toString(), "[1,2,3]");
        assertEquals(result1.get(1).toString(), "[4,5,6]");
        assertEquals(result1.get(2).toString(), "[7]");
        assertEquals(result1.length(),3);
        System.out.println(result1.toString());

        // No split
        JSONArray result2 = JSONUtils.splitJsonArray(0, nodes);
        assertEquals(result2.length(),1);
        assertEquals(result2.get(0).toString(), "[1,2,3,4,5,6,7]");

        // Test even
        JSONArray nodesEven = new JSONArray();
        nodesEven.put(1);nodesEven.put(2);nodesEven.put(3);nodesEven.put(4);nodesEven.put(5);nodesEven.put(6);
        JSONArray result3 = JSONUtils.splitJsonArray(3, nodesEven);
        assertEquals(result3.length(),2);
        assertEquals(result3.get(0).toString(), "[1,2,3]");
        assertEquals(result3.get(1).toString(), "[4,5,6]");
        System.out.println(result3.toString());

        // Test Null
        JSONArray result4 = JSONUtils.splitJsonArray(3, null);
        assertEquals(result4.length(),0);
        System.out.println(result4.toString());

        // Test Empty
        JSONArray jj = new JSONArray();
        JSONArray result5 = JSONUtils.splitJsonArray(3, jj);
        assertEquals(result4.length(),0);
        System.out.println(result4.toString());
    }

}



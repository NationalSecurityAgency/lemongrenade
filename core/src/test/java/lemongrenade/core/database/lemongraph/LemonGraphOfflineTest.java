package lemongrenade.core.database.lemongraph;

import lemongrenade.core.database.lemongraph.LemonGraph;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

/**
 * Perform tests that don't need the LEMONGRAPH server to be actually running
 */
public class LemonGraphOfflineTest {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private LemonGraph lg;


    @Before
    public void initialize() {
        lg = new LemonGraph();
    }

    @After
    public void cleanup() {

    }


    /** Test the parser code, the parser is a little complicated. See commentBlock for lemongraph.parseLemonGraphResult
     * for more information.
     */
    @Test
    public void testLemonGraphParser() {
        StringBuilder  r = new StringBuilder();
        //r.append("{\"data\": [[\"query1\",\"query2\"],[0,[{\"type\":\"id\",\"ID\":3}]],[1,[{\"type\":\"id\",\"ID\":3}]]] }");
        r.append("{\"data\": ");
        r.append("[ [\"query1\",\"query2\"],");
        r.append("[0,[{\"type\":\"id\",\"ID\":3}]],");
        r.append("[1,[{\"type\":\"id\",\"ID\":3}]] ");
        r.append("] }");
        JSONObject data1 = new JSONObject(r.toString());
        HashMap<String,JSONArray> results1 =lg.parseLemonGraphResult(data1);
        assertEquals(results1.get("query1").length(),1);
        assertEquals(results1.get("query2").length(),1);

        // Test multiple node matches in result
        StringBuilder  r2 = new StringBuilder();
        r2.append("{\"data\": ");
        r2.append("[ [\"query1\",\"query2\"],");
        r2.append("[0,[{\"type\":\"id\",\"ID\":3}, {\"type\":\"id\",\"ID\":4}]],");
        r2.append("[1,[{\"type\":\"id\",\"ID\":3}]] ");
        r2.append("] }");
        JSONObject data2 = new JSONObject(r2.toString());
        HashMap<String,JSONArray> results2 =lg.parseLemonGraphResult(data2);
        assertEquals(results2.get("query1").length(),2);
        assertEquals(results2.get("query2").length(),1);

        // Test multiple node matches that match multiple query(s) in result
        StringBuilder  r3 = new StringBuilder();
        r3.append("{\"data\": ");
        r3.append("[ [\"query1\",\"query2\", \"query2\"],");    //  1 and 2 are set to "query2" on purpose
        r3.append("[0,[{\"type\":\"id\",\"ID\":3}, {\"type\":\"id\",\"ID\":4}]],");
        r3.append("[1,[{\"type\":\"id\",\"ID\":3}]], ");
        r3.append("[2,[{\"type\":\"id\",\"ID\":5}]] ");
        r3.append("] }");
        JSONObject data3 = new JSONObject(r3.toString());
        HashMap<String,JSONArray> results3 =lg.parseLemonGraphResult(data3);
        System.out.println(results3.toString());
        assertEquals(results3.get("query1").length(),2);
        assertEquals(results3.get("query2").length(),2);

        // Test multiple node multiple matches that match multiple query(s) in result
        StringBuilder  r4 = new StringBuilder();
        r4.append("{\"data\": ");
        r4.append("[ [\"query1\",\"query2\", \"query2\"],");    //  1 and 2 are set to "query2" on purpose
        r4.append("[0,[{\"type\":\"id\",\"ID\":3}, {\"type\":\"id\",\"ID\":4}]],");
        r4.append("[1,[{\"type\":\"id\",\"ID\":3}, {\"type\":\"id\",\"ID\":6}]],");
        r4.append("[2,[{\"type\":\"id\",\"ID\":5}]] ");
        r4.append("] }");
        JSONObject data4 = new JSONObject(r4.toString());
        HashMap<String,JSONArray> results4 =lg.parseLemonGraphResult(data4);
        assertEquals(results4.get("query1").length(),2);   // ID 3, 4
        assertEquals(results4.get("query2").length(),3);   // ID 5 and 6
    }

}
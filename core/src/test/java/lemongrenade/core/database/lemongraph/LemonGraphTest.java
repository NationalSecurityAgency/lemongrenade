package lemongrenade.core.database.lemongraph;

import lemongrenade.core.database.lemongraph.InvalidGraphException;
import lemongrenade.core.database.lemongraph.LemonGraph;
import lemongrenade.core.database.lemongraph.LemonGraphObject;
import lemongrenade.core.database.lemongraph.LemonGraphResponse;
import lemongrenade.core.models.LGPayload;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

import static org.junit.Assert.*;

/**
 *  Note: Probably leave this test out of the testSuite because these tests fail if
 *  LemonGraph.Server is not running.
 *  This test moved to api module to prevent circular dependency as it nows uses API to delete
 *  test jobs after running.
 */
public class LemonGraphTest {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private LemonGraph lg;
    boolean isDatabaseUp = false;

    @Before
    public void initialize() {
        lg = new LemonGraph();
        boolean isDatabaseUp = lg.isApiUp();
        assertTrue(isDatabaseUp);
        if (isDatabaseUp == false) {
            fail("Lemon graph is down!");
        }
    }

    @After
    public void cleanup() {

    }

    @Test
    public void testCallingGraphThatDoesnotExist() {
        String graphId = "doesnotexist";
        try {
            JSONObject resultsb = lg.getGraph(graphId);
        } catch (InvalidGraphException e) {
            assertTrue(true);
            return;
        }
        fail("Never caught exception for invalid graph: " + graphId);
    }

    @Test
    public void testPost() throws Exception {
        JSONObject meta = new JSONObject()
                .put("test", new JSONArray().put("item1").put("item2"))
                ;
        String id = LemonGraph.createGraph(meta);
        assert id.length() > 0;
        JSONArray nodes = new JSONArray()
                .put(
                new JSONObject()
                        .put("type","type")
                        .put("value", new JSONArray().put("test1").put("test2"))
                        .put("other", new JSONArray().put("test1").put("test2"))
                )
                ;
        JSONArray edges = new JSONArray();
        LemonGraphObject graph = new LemonGraphObject(true, meta, nodes, edges);
        LemonGraph.postToGraph(id, graph);
        lg.deleteGraph(id);
    }

    @Test
    public void testNewPost() throws Exception {
        JSONObject meta = new JSONObject()
                .put("test", new JSONArray().put("item1").put("item2"))
                ;
        String jobId = LemonGraph.createGraph(meta);
        lg.deleteGraph(jobId);
        JSONObject job_config = new JSONObject()
                .put("job_id", jobId)
                ;
        LGPayload payload = new LGPayload(job_config);
        jobId = payload.getJobId();
        JSONArray nodes = new JSONArray()
                .put(
                        new JSONObject()
                                .put("type","type")
                                .put("value", new JSONArray().put("test1").put("test2"))
                                .put("other", new JSONArray().put("test1").put("test2"))
                )
                ;
        payload.addResponseNodes(nodes);
        String newJobId = LemonGraph.createGraph(jobId, meta);
        assert jobId.equals(newJobId);
        JSONObject job = LemonGraph.getGraph(jobId);
        lg.deleteGraph(jobId);
    }

    @Test
    public void testDelete() throws InvalidGraphException {
        LemonGraph.deleteGraph("");
    }

    /** */
    @Test
    public void testCreateGraph() {
        String graphId = "";
        try {
            JSONObject meta = new JSONObject();
            meta.put("orig_meta", 4);
            graphId = lg.createGraph(meta);
        }
        catch (Exception e) {
            fail("Unable to create new graph in LEMONGRAPH");
        }
        try {
            UUID toUuid = UUID.fromString(graphId);
            assertEquals(toUuid.toString(), graphId);
        } catch (IllegalArgumentException e) {
            fail("Invalid graph id in testCreateGraph");
        }

        // Try Querying Graph
        try {
            JSONObject results2 = lg.getGraph(graphId);
            JSONObject meta = results2.getJSONObject("meta");
            if (meta.has("orig_meta")) {
                int v = meta.getInt("orig_meta");
                assertEquals(v,4);
            }
        } catch (InvalidGraphException e) {
            fail("ERROR getting the graph " + graphId + " " + e.getMessage());
        }

        // Delete graph
        try {
            lg.deleteGraph(graphId);
        } catch (Exception e) {
            fail("Unable to delete graph from LemonGraph " + e.getMessage());
        }

        // Make sure it's really deleted
        try {
            JSONObject deleteCheck = lg.getGraph(graphId);
            fail("Graph delete failed!");
        } catch (InvalidGraphException e) {
        }
    }

    @Test
    public void testPostToGraph() {

        String graphId = "";
        try {
            JSONObject meta = new JSONObject();
            graphId = lg.createGraph(meta);
        }
        catch(Exception e) {
            fail("Unable to create graph in lemongraph");
        }
        try {
            UUID toUuid = UUID.fromString(graphId);
            assertEquals(toUuid.toString(), graphId);
        } catch (IllegalArgumentException e) {
            fail("Invalid graph id in testCreateGraph");
        }

        JSONObject node = new JSONObject();
        node.put("type","foo");node.put("value","bar");
        JSONObject node2 = new JSONObject();
        node2.put("type","foo");node2.put("value","baz");
        JSONArray nodes = new JSONArray();
        nodes.put(node); nodes.put(node2);

        JSONObject meta  = new JSONObject();
        JSONArray  edges = new JSONArray();
        JSONObject edge1 = new JSONObject();
        edge1.put("type","edge"); edge1.put("value","e1");
        JSONObject e1src = new JSONObject();
        e1src.put("type","foo"); e1src.put("value","bar");
        JSONObject e1tgt = new JSONObject();
        e1tgt.put("type","foo"); e1tgt.put("value","baz");
        edge1.put("src",e1src); edge1.put("tgt",e1tgt);
        edges.put(edge1);

        LemonGraphObject lgo = new LemonGraphObject(true,meta,nodes,edges);
        LemonGraphResponse lgr2 = null;
        try {
            lgr2 = lg.postToGraph(graphId, lgo);
            assertEquals(8, lgr2.getMaxId());
            assertEquals(8, lgr2.getUpdateCount());
            assertEquals(true, lgr2.didCallSucceed());
            assertEquals(204, lgr2.getResponseCode());
        }
        catch(InvalidGraphException e) {
            fail("Can't lookup graph id "+graphId);
        }

        // Delete Graph
        try {
            lg.deleteGraph(graphId);
        } catch (Exception e) {
            System.out.println("Delete failed message: " + e.getMessage());
        }
    }
}
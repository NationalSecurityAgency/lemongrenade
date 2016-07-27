package lemongrenade.examples.test;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.UUID;

public class StressTest extends SingleNodeClusterTest {

    public static void main(String[] args) throws Exception {
        //  Run launch_topologies.sh first or break the start_adapters() logic
        //  into its own file. As is, I would either need to kill all adapters or
        //  recompile in between running the ./stress_test.sh script
        //  Curl adapter should not be enabled for a stress test.
//        start_adapters();
        Thread.sleep(12000);
        ArrayList<String> approvedAdapters = new ArrayList();
        approvedAdapters.add("Approval");
        approvedAdapters.add("Async");
        approvedAdapters.add("HelloWorld");
        approvedAdapters.add("HelloWorldNode");
        approvedAdapters.add("HelloWorldPython3");
        approvedAdapters.add("HelloWorldPython");
        approvedAdapters.add("PlusBang");

        JSONObject node1 = new JSONObject()
                .put("status", "new")
                .put("type", "id")
                .put("value", UUID.randomUUID());
        JSONObject node2 = new JSONObject()
                .put("status", "new")
                .put("type", "id")
                .put("value", UUID.randomUUID());
        JSONObject node3 = new JSONObject()
                .put("status", "new")
                .put("type", "id")
                .put("value", UUID.randomUUID());
        JSONObject node4 = new JSONObject()
                .put("status", "new")
                .put("type", "id")
                .put("value", UUID.randomUUID());
        JSONObject node5 = new JSONObject()
                .put("status", "new")
                .put("type", "id")
                .put("value", UUID.randomUUID());
        JSONArray nodes = new JSONArray();
        nodes.put(node1); nodes.put(node2); nodes.put(node3); nodes.put(node4);nodes.put(node5);

        feedCoordinator(-1, 1, approvedAdapters, nodes); //submit as fast as possible


    }
}
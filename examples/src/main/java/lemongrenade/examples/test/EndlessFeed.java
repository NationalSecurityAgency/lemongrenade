package lemongrenade.examples.test;

import org.json.JSONObject;

import java.util.ArrayList;
import java.util.UUID;

//Threads must be running on local cluster before calling feedCoordinator();
public class EndlessFeed extends SingleNodeClusterTest {

    public static void main(String[] args) throws Exception {
        ArrayList<String> approvedAdapters = new ArrayList();
      //  approvedAdapters.add("Approval");
      //   approvedAdapters.add("Async");
//        approvedAdapters.add("Curl");
        approvedAdapters.add("HelloWorld");
        approvedAdapters.add("HelloWorldNode");
        approvedAdapters.add("HelloWorldPython3");
        approvedAdapters.add("HelloWorldPython");
        approvedAdapters.add("PlusBang");

        JSONObject node = new JSONObject()
                .put("status", "new")
                .put("type", "id")
                .put("value", UUID.randomUUID());

        feedCoordinator(2000, -1, approvedAdapters, node);//1 request every second, no limit
    }
}

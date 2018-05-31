package lemongrenade.examples.test;

import lemongrenade.core.SubmitToRabbitMQ;
import lemongrenade.core.models.LGPayload;
import org.apache.storm.Config;
import org.json.JSONArray;
import org.json.JSONObject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import static sun.security.util.Password.readPassword;

public class SingleNodeClusterTest {
    private final static ArrayList<String> DEFAULT_APPROVED_ADAPTERS = createApprovedAdapters();

    /** approved adapters to use if we aren't given any */
    public static ArrayList<String> createApprovedAdapters() {
        ArrayList<String> adapters = new ArrayList();
        adapters.add("HelloWorld");
        adapters.add("HelloWorldPython");
        adapters.add("PlusBang");
        return adapters;
    }

    /**
     * Takes the amount of MS to sleep between feeds and the count of feeds to give total.
     * Negative value for endless results
     */
    public static void feedCoordinator(int sleep, int times ) throws Exception {
        feedCoordinator(sleep,times,DEFAULT_APPROVED_ADAPTERS, get_default_job_data());
    }

    public static void feedCoordinator(int sleep, int times, JSONObject node) throws Exception {
        feedCoordinator(sleep, times, DEFAULT_APPROVED_ADAPTERS, node);
    }

    public static void feedCoordinator(int sleep, int times, ArrayList<String> approvedAdapters, JSONArray nodes) throws Exception {
        String jobId;
        SubmitToRabbitMQ submit = new SubmitToRabbitMQ();
        for (int i = 0; i != times; i++) {
            LGPayload lgp = new LGPayload();
            lgp.addResponseNodes(nodes);
            JSONObject jobConfig = new JSONObject();
            jobConfig.put("depth","6");
            lgp.setJobConfig(jobConfig);
            submit.sendNewJobToCommandController(approvedAdapters, lgp);
            if (sleep > 0) {
                Thread.sleep(sleep); // ~500 requests/sec
            }
        }
        submit.close();
    }

    public static void feedCoordinator(int sleep, int times, ArrayList<String> approvedAdapters, JSONObject node) throws Exception {
        String jobId;
        SubmitToRabbitMQ submit = new SubmitToRabbitMQ();
        for (int i = 0; i != times; i++) {
            LGPayload lgp = new LGPayload();
            lgp.addResponseNode(node);
            JSONObject jobConfig = new JSONObject();
            jobConfig.put("depth","6");
            lgp.setJobConfig(jobConfig);
            submit.sendNewJobToCommandController(approvedAdapters, lgp);
            if (sleep > 0) {
                Thread.sleep(sleep); // ~500 requests/sec
            }
        }
        submit.close();
    }

    public static JSONObject merge_config(JSONObject job_config, Config config) {//merges Config into job_config
        Set<String> keyring = config.keySet();
        Iterator<String> keys = keyring.iterator();
        while(keys.hasNext()) {
            String key = keys.next();
            Object value = config.get(key);
            job_config.put(key, value);
        }
        return job_config;
    }

    public static String read_password(String msg) {
        System.out.println(msg);//print message for user
        char[] input = {};
        try {
            input = readPassword(System.in, false);
        } catch (IOException e) {
            System.out.println("Failed to get password from System.in.");
            e.printStackTrace();
        }
        return input.toString();
    }

    /** */
    public static JSONObject get_default_job_data() {
        JSONObject job_data = new JSONObject()
          .put("status", "new")
          .put("type", "id")
          .put("value", UUID.randomUUID());
        return job_data;
    }

    /**
     * Locally starts all lemongrenade.adapters. Feed the coordinator elsewhere.
     */
    public static void main(String[] args) throws Exception {
        LocalEngineTester test = new LocalEngineTester();
        String [] filename = {"default-topology.json"};
        test.main(filename);
    }
}

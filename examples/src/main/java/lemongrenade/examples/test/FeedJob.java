package lemongrenade.examples.test;

import lemongrenade.core.SubmitJob;
import lemongrenade.core.models.LGPayload;
import org.json.JSONObject;
import java.util.ArrayList;
import java.util.UUID;

//This test displays how to create and submit a job using a non-default start node and job configuration
public class FeedJob extends SingleNodeClusterTest {

    public static void main(String[] args) throws Exception {
        ArrayList<String> approvedAdapters = new ArrayList();
        approvedAdapters.add("HelloWorld");

        JSONObject node = new JSONObject()
                .put("type", "id")
                .put("value", UUID.randomUUID());

        //Read file of certs and create job_config object then LGPayload
        String job_id = UUID.randomUUID().toString();
        JSONObject job_config = new JSONObject() //add user_dn and issuer_dn here!
               .put("job_item", "job_value");
        LGPayload payload = new LGPayload(job_id, "", job_config);
        payload.addResponseNode(node);

        SubmitJob submitJob = new SubmitJob();
        submitJob.sendNewJobToCommandController(approvedAdapters, payload);
        Thread.sleep(500);
        submitJob.closeConnections();
        System.out.println("Job submitted.");
        System.exit(0);
    }
}

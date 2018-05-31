package lemongrenade.examples.test;

import lemongrenade.core.SubmitToRabbitMQ;
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
            .put("value", UUID.randomUUID())
            .put("status", "test")
        ;

        //Read file of certs and create job_config object then LGPayload
        String job_id = UUID.randomUUID().toString();
        JSONObject job_config = new JSONObject()
               .put("job_item", "job_value");
        LGPayload payload = new LGPayload(job_id, "", job_config);

        //Add all nodes
        payload.addResponseNode(node);

        SubmitToRabbitMQ submit = new SubmitToRabbitMQ();
        submit.sendNewJobToCommandController(approvedAdapters, payload);
        Thread.sleep(500);
        submit.close();
        System.out.println("Job submitted.");
    }
}

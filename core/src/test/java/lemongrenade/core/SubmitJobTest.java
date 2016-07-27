package lemongrenade.core;

import junit.framework.TestCase;

import lemongrenade.core.models.LGPayload;
import org.json.JSONObject;
import org.json.JSONArray;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SubmitJobTest extends TestCase {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Test
    public void testSubmitJobTestBuild() {
        SubmitJob sj = new SubmitJob();
        ArrayList<String> alist = new ArrayList();
        alist.add("adapter1"); alist.add("adapter2"); alist.add("adapter3");

        JSONObject job1 = sj.buildJobSubmitObject(alist,"0001");
        assertEquals(job1.getString("job_id"),"0001");

        // Build copy with
        LGPayload lgp = new LGPayload("0001");
        JSONObject node = new JSONObject();
        node.put("status","new");
        node.put("type","id");
        LGPayload jp1 = LGPayload.fromJson(job1.getJSONObject("payload"));
        List<JSONObject> nodes = jp1.getResponseNodes();
        JSONObject newnode = nodes.get(0);
        node.put("value",newnode.getString("value"));
        lgp.addResponseNode(node);
        JSONObject job2 = sj.buildJobSubmitObject(alist,lgp);


        // Job objects should be the same
        assertEquals(job2.getString("job_id"),"0001");
        assertEquals(job1.toString(),job2.toString());

    }

}
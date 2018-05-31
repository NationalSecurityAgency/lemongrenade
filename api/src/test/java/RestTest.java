import lemongrenade.api.services.Utils;
import lemongrenade.core.database.lemongraph.LemonGraph;
import lemongrenade.core.database.mongo.MongoDBStore;
import lemongrenade.core.models.LGJob;
import lemongrenade.core.util.RequestResult;
import lemongrenade.core.util.Requests;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

//Tests Rest endpoints. Requires LGServer to be running.
public class RestTest {
    private static final Logger log = LoggerFactory.getLogger(RestTest.class);
    public static final String TEST_ID = "00000000-0000-0000-0000-000000000000";
    String LEMONGRENADE_URL = "http://localhost:9999";

    @Test public void getExpiredJobsTest() throws Exception {
        RequestResult result = Requests.get(LEMONGRENADE_URL + "/rest/jobs/expired");
        assert result.status_code == Utils.OK;
        JSONArray IDs = new JSONArray(result.response_msg);
        assert IDs.length() >= 0;
    }

    @Test public void getResetJobsTest() throws Exception {
        RequestResult result = Requests.get(LEMONGRENADE_URL + "/rest/jobs/reset");
        assert result.status_code == Utils.OK;
        JSONArray IDs = new JSONArray(result.response_msg);
        assert IDs.length() >= 0;
    }

    //Creates a "NEW" job and verifies cancel helper sets its status to STOPPED
    @Test public void testJobCancel() throws Exception {
        String id = Utils.seedJob();
        try {
            Document job = null;
            for(int i = 0; i < 3; i++) {
                ArrayList<Document> jobs = MongoDBStore.getJob(id);//fails of job ID isn't present
                if(jobs.size() == 1) {
                    job = jobs.get(0);
                    int status = job.getInteger("status");
                    if(status == LGJob.STATUS_FINISHED_WITH_ERRORS) {
                        break;
                    }
                }
                Thread.sleep(Utils.SECOND/2);
            }

            assert job.getInteger("status") == LGJob.STATUS_FINISHED_WITH_ERRORS;//no tasks job seed, so it's FINISHED_WITH_ERRORS
            Document doc = new Document();
            doc.put("status", LGJob.STATUS_NEW);//sets the status to NEW
            MongoDBStore.saveDocument("jobs", id, doc);
            Utils.cancelHelper(id);
            ArrayList<Document> jobs = MongoDBStore.getJob(id);
            job = jobs.get(0);
            assert job.getInteger("status") == LGJob.STATUS_STOPPED;
            Utils.deleteHelper(id);
        }
        catch(Exception e) {
            Utils.deleteHelper(id);
            throw e;
        }
    }

}

package lemongrenade.core.models;

import junit.framework.TestCase;
import lemongrenade.core.coordinator.JobManager;
import lemongrenade.core.database.lemongraph.InvalidGraphException;
import lemongrenade.core.database.lemongraph.LemonGraph;
import lemongrenade.core.database.mongo.MongoDBStore;
import org.bson.Document;
import org.json.JSONObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Set;

public class LGJobTest {
    public static final String TEST_ID = "00000000-0000-0000-0000-000000000000";
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Test public void testResetExpireDate() throws InvalidGraphException {
        LemonGraph.deleteGraph(TEST_ID);
        MongoDBStore.deleteJob(TEST_ID);
        JSONObject job_config = new JSONObject()
                .put("expire_date", "2017-03-09T23:59:59.000Z")//date job is deleted from system
                .put("reset_date", "2016-03-07T00:00:00.000Z")//data job data is reset
                ;
        LGJob job = new LGJob(TEST_ID, new ArrayList<>(), job_config);
        long reset = job.getResetDate();
        long expire = job.getExpireDate();
        assert reset == new Long("1457308800000").longValue();
        assert expire == new Long("1489103999000").longValue();

        try {
            JobManager.addJob(job);
        }
        catch(Exception e) {
            MongoDBStore.deleteJob(TEST_ID);
            throw e;
        }
        MongoDBStore.deleteJob(TEST_ID);
    }

    @Test public void testValidJobDates() {
        LGJob job1 = new LGJob();
        LGJob job2 = new LGJob("0000");
        long time = job1.getResetDate();
        long time2 = job1.getExpireDate();
        long difference = time2-time;
        assert difference == LGJob.DAY*60;
        time = job2.getResetDate();
        time2 = job2.getExpireDate();
        difference = time2-time;
        assert difference == LGJob.DAY*60;
    }

    @Test public void testFetchExpireJobs() {
        MongoDBStore.deleteJob(TEST_ID);

        //Test ID that should return
        ArrayList<Document> jobDocs = MongoDBStore.getJob(TEST_ID);
        assert jobDocs.size() == 0;
        long expiredTime = new Long("1500123456789");//expired date
        Document jobDoc = new Document();
        jobDoc.put("_id", TEST_ID);
        jobDoc.put("expireDate", expiredTime);
        MongoDBStore.insertDocument("jobs", jobDoc);
        Set<String> expiredIDs = MongoDBStore.getExpiredJobs();
        assert expiredIDs.contains(TEST_ID); //assert the known expired job is present
        MongoDBStore.deleteJob(TEST_ID);

        //Test ID that should not return
        long currentDate = new Long("9900123456789");//current date
        jobDoc = new Document();
        jobDoc.put("_id", TEST_ID);
        jobDoc.put("expireDate", currentDate);
        MongoDBStore.insertDocument("jobs", jobDoc);
        expiredIDs = MongoDBStore.getExpiredJobs();
        assert !expiredIDs.contains(TEST_ID); //assert the current date is NOT present
        MongoDBStore.deleteJob(TEST_ID);

        //Test special 0 case ID that should not return
        long specialDate = new Long("0");//current date
        jobDoc = new Document();
        jobDoc.put("_id", TEST_ID);
        jobDoc.put("expireDate", specialDate);
        MongoDBStore.insertDocument("jobs", jobDoc);
        expiredIDs = MongoDBStore.getExpiredJobs();
        assert !expiredIDs.contains(TEST_ID); //assert the current date is NOT present
        MongoDBStore.deleteJob(TEST_ID);

        //Test current case
        long endTime = new Long("1400123456789");//distant past start time
        jobDoc = new Document();
        jobDoc.put("_id", TEST_ID);
        jobDoc.put("endTime", endTime);
        MongoDBStore.insertDocument("jobs", jobDoc);
        expiredIDs = MongoDBStore.getExpiredJobs();
        assert expiredIDs.contains(TEST_ID); //assert the date with endTime should be Expired
        MongoDBStore.deleteJob(TEST_ID);

        //Test current case
        endTime = new Long("9900123456789");//distant future end time
        jobDoc = new Document();
        jobDoc.put("_id", TEST_ID);
        jobDoc.put("startTime", endTime);
        MongoDBStore.insertDocument("jobs", jobDoc);
        expiredIDs = MongoDBStore.getExpiredJobs();
        assert !expiredIDs.contains(TEST_ID); //assert the date with endTime should NOT be expired
        MongoDBStore.deleteJob(TEST_ID);

        //Test current case
        Long startTime = new Long("0000123456789");//distant past start time
        jobDoc = new Document();
        jobDoc.put("_id", TEST_ID);
        jobDoc.put("startTime", startTime);
        jobDoc.put("endTime", new Long("0"));
        MongoDBStore.insertDocument("jobs", jobDoc);
        expiredIDs = MongoDBStore.getExpiredJobs();
        assert expiredIDs.contains(TEST_ID); //assert the date with long startTime and 0 endTime should be expired
        MongoDBStore.deleteJob(TEST_ID);
    }

    @Test public void testFetchResetJobs() {
        MongoDBStore.deleteJob(TEST_ID);
        //Test reset case
        ArrayList<Document> jobDocs = MongoDBStore.getJob(TEST_ID);
        assert jobDocs.size() == 0;
        long resetTime = new Long("1500123456789");//expired date
        Document jobDoc = new Document();
        jobDoc.put("_id", TEST_ID);
        jobDoc.put("resetDate", resetTime);
        MongoDBStore.insertDocument("jobs", jobDoc);
        Set<String> resetIDs = MongoDBStore.getResetJobs();
        assert resetIDs.contains(TEST_ID); //assert the known expired job is present
        MongoDBStore.deleteJob(TEST_ID);

        //Test reset case, but already in reset status
        jobDocs = MongoDBStore.getJob(TEST_ID);
        assert jobDocs.size() == 0;
        resetTime = new Long("1500123456789");//expired date
        jobDoc = new Document();
        jobDoc.put("_id", TEST_ID);
        jobDoc.put("resetDate", resetTime);
        jobDoc.put("status", LGJob.STATUS_RESET);//job is already in reset, should NOT be present
        MongoDBStore.insertDocument("jobs", jobDoc);
        resetIDs = MongoDBStore.getResetJobs();
        assert !resetIDs.contains(TEST_ID); //assert the known expired job is NOT present
        MongoDBStore.deleteJob(TEST_ID);

        //Test current case
        long currentDate = new Long("9900123456789");//current date
        jobDoc = new Document();
        jobDoc.put("_id", TEST_ID);
        jobDoc.put("resetDate", currentDate);
        MongoDBStore.insertDocument("jobs", jobDoc);
        resetIDs = MongoDBStore.getResetJobs();
        assert !resetIDs.contains(TEST_ID); //assert the current date is NOT present
        MongoDBStore.deleteJob(TEST_ID);

        //Test current case
        long startTime = new Long("1400123456789");//distant past start time
        jobDoc = new Document();
        jobDoc.put("_id", TEST_ID);
        jobDoc.put("startTime", startTime);
        MongoDBStore.insertDocument("jobs", jobDoc);
        resetIDs = MongoDBStore.getResetJobs();
        assert resetIDs.contains(TEST_ID); //assert the date with startTime should be reset
        MongoDBStore.deleteJob(TEST_ID);

        //Test current case, already RESET
        startTime = new Long("1400123456789");//distant past start time
        jobDoc = new Document();
        jobDoc.put("_id", TEST_ID);
        jobDoc.put("startTime", startTime);
        jobDoc.put("status", LGJob.STATUS_RESET);
        MongoDBStore.insertDocument("jobs", jobDoc);
        resetIDs = MongoDBStore.getResetJobs();
        assert !resetIDs.contains(TEST_ID); //assert the date with startTime should be reset
        MongoDBStore.deleteJob(TEST_ID);

        //Test current case
        startTime = new Long("9900123456789");//distant future start time
        jobDoc = new Document();
        jobDoc.put("_id", TEST_ID);
        jobDoc.put("startTime", startTime);
        MongoDBStore.insertDocument("jobs", jobDoc);
        resetIDs = MongoDBStore.getResetJobs();
        assert !resetIDs.contains(TEST_ID); //assert the date with startTime should NOT be reset
        MongoDBStore.deleteJob(TEST_ID);
    }

}
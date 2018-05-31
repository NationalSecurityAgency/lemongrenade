package lemongrenade.core.database.mongo;

import lemongrenade.core.coordinator.JobManager;
import lemongrenade.core.database.lemongraph.LemonGraph;
import lemongrenade.core.models.LGJob;
import org.apache.storm.shade.org.joda.time.DateTime;
import org.apache.storm.shade.org.joda.time.DateTimeZone;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class LGJobDAOImplTest {
    private final Logger log = LoggerFactory.getLogger(getClass());
    public static final String TEST_ID = "00000000-0000-0000-0000-000000000000";

    private static MorphiaService ms  = new MorphiaService();;
    private static LGJobDAOImpl dao  = new LGJobDAOImpl(LGJob.class, ms.getDatastore());;

    HashMap<String,String> jobMap;

    @Before public void setupEachTest() {
        System.out.println("Creating test jobs");
        jobMap = createJobsForDateRangeTesting();
    }

    @After public void cleanup() {
        System.out.println("Cleaning up jobs");
        deleteJobs(jobMap);
    }

    /**
     * Returns the UUID of the job that gets created
     * @param testName sets the name in job_config for future use
     * @param age how many days "old" you want the job to be
     */
    private String createJob(String testName, int age) {
        JobManager jm = new JobManager();
        JSONObject jobConfig = new JSONObject();
        jobConfig.put("testname",testName);
        ArrayList<String> alist = new ArrayList();
        LGJob job = new LGJob(UUID.randomUUID().toString(), alist, jobConfig);

        // if age > 0 , set the create date
        if (age > 0) {
            int newage = age * -1;
            Date today = new Date();
            Calendar cal = new GregorianCalendar();
            cal.setTime(today);
            cal.add(Calendar.DAY_OF_MONTH, newage);
            Date createDate = cal.getTime();
            job.setCreateDate(createDate);
        }
        jm.addJob(job);
        assertTrue(jm.doesJobExist(job));
        return job.getJobId();
    }

    /** */
    private HashMap<String,String> createJobsForDateRangeTesting() {
        HashMap<String,String> jobMap = new HashMap<String,String>();
        // Create jobs at various date ranges
        String job = createJob("one",1);
        jobMap.put("one", job);
        job = createJob("fifteen",15);
        jobMap.put("fifteen", job);
        job = createJob("twentynine",29);
        jobMap.put("twentynine", job);
        job = createJob("thirty",30);
        jobMap.put("thirty", job);
        job = createJob("thirtyone",31);
        jobMap.put("thirtyone", job);
        job = createJob("fiftynine",59);
        jobMap.put("fiftynine", job);
        job = createJob("sixty",60);
        jobMap.put("sixty", job);
        job = createJob("sixtyone",61);
        jobMap.put("sixtyone", job);
        return jobMap;
    }

    private  void deleteJobs(HashMap<String,String>  jobs) {
        for (String jobId : jobs.values()) {
            LGJob job = dao.getByJobId(jobId);
            if (job!= null) {
                dao.delete(job);
            }
        }
    }

    private boolean lookInList(List<LGJob> jobs, String jobId) {
        for (LGJob job : jobs) {
            if (job.getJobId().equals(jobId)) {
                return true;
            }
        }
        return false;
    }

    /** */
    @Test
    public void testDateRangeQuery1() {
        Calendar calStart = GregorianCalendar.getInstance();
        calStart.add(Calendar.DAY_OF_YEAR, -26);
        Calendar calEnd = GregorianCalendar.getInstance();
        calEnd.add(Calendar.DAY_OF_YEAR, -30);
        Date startUtc = new DateTime(calStart.getTimeInMillis(), DateTimeZone.UTC).toDate();
        Date endUtc   = new DateTime(calEnd.getTimeInMillis(), DateTimeZone.UTC).toDate();
        System.out.println("  *** "+startUtc.toString());
        System.out.println("  *** "+endUtc.toString());


        List<LGJob> jobs1 = dao.getAllByDateRange(startUtc, endUtc);
        System.out.println("Number of Jobs returned "+jobs1.size());
        for (LGJob job: jobs1) {
            System.out.println("     :"+job.getCreateDate());
        }
        // This query should contain, "twentynine,thirty,
        LGJob j = dao.getByJobId(jobMap.get("twentynine"));
        assertTrue(lookInList(jobs1, j.getJobId()));

        // Should not contain
        j = dao.getByJobId(jobMap.get("thirty"));
        assertFalse(lookInList(jobs1, j.getJobId()));
        j = dao.getByJobId(jobMap.get("fifteen"));
        assertFalse(lookInList(jobs1, j.getJobId()));
        j = dao.getByJobId(jobMap.get("thirtyone"));
        assertFalse(lookInList(jobs1, j.getJobId()));
    }

    @Test
    public void testDateRangeQuery2() {
        Calendar calStart = GregorianCalendar.getInstance();
        calStart.add(Calendar.DAY_OF_YEAR, -29);
        Calendar calEnd = GregorianCalendar.getInstance();
        calEnd.add(Calendar.DAY_OF_YEAR, -31);
        Date startUtc = new DateTime(calStart.getTimeInMillis(), DateTimeZone.UTC).toDate();
        Date endUtc   = new DateTime(calEnd.getTimeInMillis(), DateTimeZone.UTC).toDate();
        List<LGJob> jobs1 = dao.getAllByDateRange(startUtc,endUtc);
        System.out.println("Number of Jobs returned "+jobs1.size());
        for (LGJob job: jobs1) {
            System.out.println("     :"+job.getCreateDate()+" "+job.getJobConfig());
        }
        // This query should contain, "twentynine,thirty,
        LGJob j = dao.getByJobId(jobMap.get("twentynine"));
        assertTrue(lookInList(jobs1, j.getJobId()));
        j = dao.getByJobId(jobMap.get("thirty"));
        assertTrue(lookInList(jobs1, j.getJobId()));

        // Should not contain
        j = dao.getByJobId(jobMap.get("fifteen"));
        assertFalse(lookInList(jobs1, j.getJobId()));
        j = dao.getByJobId(jobMap.get("thirtyone"));
        assertFalse(lookInList(jobs1, j.getJobId()));
    }


    /** test open ended query  16/0  should give us all jobs older than 15 */
    @Test
    public void testDateRangeQuery3() {
        Calendar calStart = GregorianCalendar.getInstance();
        calStart.add(Calendar.DAY_OF_YEAR, -16);
        Date startUtc = new DateTime(calStart.getTimeInMillis(), DateTimeZone.UTC).toDate();
        List<LGJob> jobs1 = dao.getAllByDateRange(startUtc,null);
        System.out.println("Number of Jobs returned "+jobs1.size());
        for (LGJob job: jobs1) {
            System.out.println("     :"+job.getCreateDate()+" "+job.getJobConfig());
        }

        LGJob j;
        // This query should contain, "twentynine,thirty,
        j= dao.getByJobId(jobMap.get("twentynine"));
        assertTrue(lookInList(jobs1, j.getJobId()));
        j = dao.getByJobId(jobMap.get("thirty"));
        assertTrue(lookInList(jobs1, j.getJobId()));
        j = dao.getByJobId(jobMap.get("thirtyone"));
        assertTrue(lookInList(jobs1, j.getJobId()));

        // Should not contain
        j = dao.getByJobId(jobMap.get("fifteen"));
        assertFalse(lookInList(jobs1, j.getJobId()));
        j = dao.getByJobId(jobMap.get("one"));
        assertFalse(lookInList(jobs1, j.getJobId()));
    }


    /**
     *  Test the date range code. To do this, we'll create a few jobs in certain ranges and see if we can
     *  find it with various queries. (Since we don't know what exists already in the jobs table and we dont
     *  want to delete the entire table first.
     *
     *  We will clean up any jobs we create at the end.
     */
    @Test
    public void testOLDDateRangeQuery1() {
        List<LGJob> jobs1 = dao.getAllByDays(29,30);
        // This query should contain, "twentynine,thirty,
        LGJob j = dao.getByJobId(jobMap.get("twentynine"));
        assertTrue(lookInList(jobs1, j.getJobId()));

        // Should not contain
        j = dao.getByJobId(jobMap.get("thirty"));
        assertFalse(lookInList(jobs1, j.getJobId()));
        j = dao.getByJobId(jobMap.get("fifteen"));
        assertFalse(lookInList(jobs1, j.getJobId()));
        j = dao.getByJobId(jobMap.get("thirtyone"));
        assertFalse(lookInList(jobs1, j.getJobId()));
    }


    @Test
    public void testOLDDateRangeQuery2() {
        List<LGJob> jobs1 = dao.getAllByDays(29,31);
        // This query should contain, "twentynine,thirty,
        LGJob j = dao.getByJobId(jobMap.get("twentynine"));
        assertTrue(lookInList(jobs1, j.getJobId()));
        j = dao.getByJobId(jobMap.get("thirty"));
        assertTrue(lookInList(jobs1, j.getJobId()));

        // Should not contain
        j = dao.getByJobId(jobMap.get("fifteen"));
        assertFalse(lookInList(jobs1, j.getJobId()));
        j = dao.getByJobId(jobMap.get("thirtyone"));
        assertFalse(lookInList(jobs1, j.getJobId()));
    }


    /** test open ended query  16/0  should give us all jobs older than 15 */
    @Test
    public void testOLDDateRangeQuery3() {
        List<LGJob> jobs1 = dao.getAllByDays(16,0);
        LGJob j;
        // This query should contain, "twentynine,thirty,
        j= dao.getByJobId(jobMap.get("twentynine"));
        assertTrue(lookInList(jobs1, j.getJobId()));
        j = dao.getByJobId(jobMap.get("thirty"));
        assertTrue(lookInList(jobs1, j.getJobId()));
        j = dao.getByJobId(jobMap.get("thirtyone"));
        assertTrue(lookInList(jobs1, j.getJobId()));

        // Should not contain
        j = dao.getByJobId(jobMap.get("fifteen"));
        assertFalse(lookInList(jobs1, j.getJobId()));
        j = dao.getByJobId(jobMap.get("one"));
        assertFalse(lookInList(jobs1, j.getJobId()));
    }
}
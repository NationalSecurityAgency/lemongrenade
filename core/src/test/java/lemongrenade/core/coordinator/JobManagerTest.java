package lemongrenade.core.coordinator;

import junit.framework.TestCase;

import lemongrenade.core.database.mongo.LGJobDAOImpl;
import lemongrenade.core.database.mongo.MorphiaService;
import lemongrenade.core.models.LGJob;
import lemongrenade.core.models.LGPayload;
import lemongrenade.core.models.LGTask;
import org.json.JSONObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.UUID;

public class JobManagerTest extends TestCase {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Test public void testJobCreateandDelete() {
        JobManager jm = new JobManager();
        JSONObject jobConfig = new JSONObject();
        ArrayList<String> alist = new ArrayList();
        LGJob job = new LGJob(UUID.randomUUID().toString(), alist, jobConfig);
        jm.addJob(job);
        assertTrue(jm.doesJobExist(job));
        assertTrue(jm.doesJobExist(job.getJobId()));
        log.info("Built test job "+job.toString());
        jm.deleteJob(job);
        assertFalse(jm.doesJobExist(job.getJobId()));
    }

    @Test public void testJobDeleteOnProcessingJob() {
        JobManager jm = new JobManager();
        JSONObject jobConfig = new JSONObject();
        ArrayList<String> alist = new ArrayList();
        LGJob job = new LGJob(UUID.randomUUID().toString(), alist, jobConfig);
        jm.addJob(job);
        assertTrue(jm.doesJobExist(job));
        assertTrue(jm.doesJobExist(job.getJobId()));
        log.info("Built test job "+job.toString());
        jm.setStatus(job,LGJob.STATUS_PROCESSING);


        boolean isDelete = jm.deleteJob(job);
        if(isDelete) {
            fail("Was able to delete a PROCESSING job!");
        }
        assertTrue(jm.doesJobExist(job.getJobId()));

        jm.setStatus(job,LGJob.STATUS_FINISHED);
        isDelete = jm.deleteJob(job);
        if(!isDelete) {
            fail("Was able to delete a FINISHED job!");
        }
        assertFalse(jm.doesJobExist(job.getJobId()));
    }

    @Test public void testJobIsFinishedWhenTaskFails() {
        JobManager jm = new JobManager();
        JSONObject jobConfig = new JSONObject();
        ArrayList<String> alist = new ArrayList();
        LGJob job = new LGJob(UUID.randomUUID().toString(), alist, jobConfig);
        jm.addJob(job);
        assertTrue(jm.doesJobExist(job));

        LGTask lgtask = new LGTask(job,"0001","adpater1",1, "",0,0,0);
        jm.addTaskToJob(job,lgtask);

        boolean isSet = jm.setStatus(job,job.STATUS_PROCESSING);
        assertTrue(isSet);

        // Job shouldn't be finished because task is not completed
        jm.updateJobIfFinished(job);
        job = jm.getJob(job.getJobId());
        assertTrue(job.getStatus() == job.STATUS_PROCESSING);
        assertEquals(job.getActiveTaskCount(),1);
        assertEquals(job.getTaskCount(),1);

        jm.updateJobTaskStatus(job,lgtask.getTaskId(), LGTask.TASK_STATUS_FAILED);
        jm.updateJobIfFinished(job);
        job = jm.getJob(job.getJobId());
        assertFalse(job.getStatus() == job.STATUS_FINISHED);
        assertTrue(job.getStatus() == job.STATUS_FINISHED_WITH_ERRORS);
        assertEquals(job.getActiveTaskCount(),0);
        assertEquals(job.getTaskCount(),1);

        // Now add another task
        LGTask lgtask2 = new LGTask(job,"0001","adapter2",2,"",0,0,0);
        jm.addTaskToJob(job,lgtask2);
        jm.updateJobIfFinished(job);
        job = jm.getJob(job.getJobId());
        System.out.println(job.toString());
        System.out.println("Job Status is now :"+job.getStatusString(job.getStatus()));
        assertTrue(job.getStatus() == job.STATUS_PROCESSING);
        assertFalse(job.getStatus() == job.STATUS_FINISHED_WITH_ERRORS);
        assertFalse(job.getStatus() == job.STATUS_FINISHED);

        // Now complete task2
        jm.updateJobTaskStatus(job,lgtask2.getTaskId(), LGTask.TASK_STATUS_COMPLETE);
        jm.updateJobIfFinished(job);
        job = jm.getJob(job.getJobId());
        assertFalse(job.getStatus() == job.STATUS_FINISHED);
        assertTrue(job.getStatus() == job.STATUS_FINISHED_WITH_ERRORS);
        assertEquals(job.getActiveTaskCount(),0);
        assertEquals(job.getTaskCount(),2);

        System.out.println(job.getJobHistory().toString());

        jm.deleteJob(job);
        assertFalse(jm.doesJobExist(job.getJobId()));
        assertEquals(null,jm.getTask(lgtask.getTaskId()));
    }

    @Test public void testJobIsFinished() {
        JobManager jm = new JobManager();
        JSONObject jobConfig = new JSONObject();
        ArrayList<String> alist = new ArrayList();
        LGJob job = new LGJob(UUID.randomUUID().toString(), alist, jobConfig);
        jm.addJob(job);
        assertTrue(jm.doesJobExist(job));

        LGTask lgtask = new LGTask(job,"0001","adapter1",1,"",0,0,0);
        jm.addTaskToJob(job,lgtask);

        boolean isSet = jm.setStatus(job,job.STATUS_PROCESSING);
        assertTrue(isSet);

        // Job shouldn't be finished becuse task is not ocmpleted
        jm.updateJobIfFinished(job);
        job = jm.getJob(job.getJobId());
        assertTrue(job.getStatus() == job.STATUS_PROCESSING);
        assertEquals(job.getActiveTaskCount(),1);
        assertEquals(job.getTaskCount(),1);

        jm.updateJobTaskStatus(job,lgtask.getTaskId(), LGTask.TASK_STATUS_COMPLETE);
        jm.updateJobIfFinished(job);
        job = jm.getJob(job.getJobId());
        assertTrue(job.getStatus() == job.STATUS_FINISHED);
        assertEquals(job.getActiveTaskCount(),0);
        assertEquals(job.getTaskCount(),1);

        jm.deleteJob(job);
        assertFalse(jm.doesJobExist(job.getJobId()));
        assertEquals(null,jm.getTask(lgtask.getTaskId()));
    }

    @Test public void testAddTaskRemoveTask() {
        JobManager jm = new JobManager();
        JSONObject jobConfig = new JSONObject();
        ArrayList<String> alist = new ArrayList();
        LGJob job = new LGJob(UUID.randomUUID().toString(), alist, jobConfig);
        jm.addJob(job);
        assertTrue(jm.doesJobExist(job));
        boolean isSet = jm.setStatus(job,job.STATUS_PROCESSING);
        assertTrue(isSet);

        LGTask lgt = new LGTask(job,"adapter1","adaptername",1,"",0,0,0);
        jm.addTaskToJob(job,lgt);
        jm.updateJobIfFinished(job);

        boolean checkStatus = jm.setStatus(job,job.STATUS_PROCESSING);
        assertTrue(checkStatus);

        LGTask lg1 = jm.getTask(lgt.getTaskId());
        assertEquals(lg1.getTaskId(), lgt.getTaskId());

        LGJob jt1 = jm.getJob(job.getJobId());
        assertEquals(jt1.getTaskCount(),1);

        // Remove task from job
        jm.removeTaskFromJob(job.getJobId(),lgt.getTaskId());
        // Task count should be 0
        LGJob jremoved = jm.getJob(job.getJobId());
        assertEquals(jremoved.getTaskCount(),0);

        // This should set the status to FINISHED
        jm.setJobFinished(job);

        job = jm.getJob(job.getJobId());
        int status = job.getStatus();
        assertTrue(status == job.STATUS_FINISHED);

        jm.deleteJob(job);
        assertFalse(jm.doesJobExist(job.getJobId()));
    }

    @Test public void testJobConfig() {
        JobManager jm = new JobManager();
        MorphiaService ms = new MorphiaService();
        LGJobDAOImpl dao = new LGJobDAOImpl(LGJob.class, ms.getDatastore());

        String jobId = UUID.randomUUID().toString();
        ArrayList<String> alist = new ArrayList();
        alist.add("HelloWorld");
        alist.add("PlusBang");
        alist.add("HelloWorldPython");
        alist.add("LongRunningTestAdapter");
        alist.add("LongRunningPython");

        JSONObject jobConfig = new JSONObject();
        jobConfig.put("job_id",jobId);
        jobConfig.put("ttl",300); // 5 mins
        jobConfig.put("depth",3);
        jobConfig.put("description","test job");
        LGPayload lgp = new LGPayload(jobConfig);

        // Create test job
        LGJob testJob = new LGJob(jobId, alist, jobConfig);
        dao.save(testJob);

        // Query and test that job_config survives
        LGJob lookup = dao.getByJobId(jobId);
        assertEquals(lookup.getDepth(), 3);
        assertEquals(lookup.getDescription(), "test job");
        assertEquals(lookup.getTTL(),300);

        jm.deleteJob(lookup);

    }

    /** Test job EXPIRED */
    @Test public void testJobTTL() {
        JobManager jm = new JobManager();
        MorphiaService ms = new MorphiaService();
        LGJobDAOImpl dao = new LGJobDAOImpl(LGJob.class, ms.getDatastore());

        String jobId = UUID.randomUUID().toString();
        ArrayList<String> alist = new ArrayList();
        alist.add("HelloWorld");
        alist.add("PlusBang");
        alist.add("HelloWorldPython");
        alist.add("LongRunningTestAdapter");
        alist.add("LongRunningPython");

        JSONObject jobConfig = new JSONObject();
        jobConfig.put("job_id",jobId);
        jobConfig.put("ttl",-300); // trick it into thinking already expired
        jobConfig.put("depth",3);
        jobConfig.put("description","test job for TTL");
        LGPayload lgp = new LGPayload(jobConfig);

        // Create test job
        LGJob testJob = new LGJob(jobId, alist, jobConfig);
        dao.save(testJob);

        // Query and test that job_config survives
        LGJob lookup = dao.getByJobId(jobId);
        // TODO: REDO THIS TEST
        //assertEquals(lookup.getTTL(),-300);
        //assertTrue(jm.hasJobExpired(lookup));
        //assertEquals(lookup.getStatus(), LGJob.STATUS_EXPIRED);

        jm.deleteJob(lookup);

    }

    @Test public void testAddTasks() {
        JobManager jm = new JobManager();
        JSONObject jobConfig = new JSONObject();
        ArrayList<String> alist = new ArrayList();
        LGJob job = new LGJob(UUID.randomUUID().toString(), alist, jobConfig);
        jm.addJob(job);
        assertTrue(jm.doesJobExist(job));
        boolean isSet = jm.setStatus(job,job.STATUS_PROCESSING);
        assertTrue(isSet);

        int tasks = 10;
        for (int i = 1; i<=tasks; i++) {
            LGTask lgt = new LGTask(job, "adapter1","name2",1,"",0,0,0);
            jm.addTaskToJob(job, lgt);
        }

        LGJob jt1 = jm.getJob(job.getJobId());
        assertEquals(jt1.getTaskCount(),tasks);

        // Delete should fail because it's processing
        System.out.println("1");
        jm.deleteJob(job);
        assertTrue(jm.doesJobExist(job.getJobId()));

        isSet = jm.setStatus(job,job.STATUS_FINISHED);
        assertTrue(isSet);
        System.out.println("2");
        // Delete should succeed
        jm.deleteJob(job);
        assertFalse(jm.doesJobExist(job.getJobId()));

    }
}

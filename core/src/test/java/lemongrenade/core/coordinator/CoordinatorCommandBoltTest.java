package lemongrenade.core.coordinator;


import junit.framework.TestCase;
import lemongrenade.core.database.lemongraph.InvalidGraphException;
import lemongrenade.core.database.lemongraph.LemonGraph;
import lemongrenade.core.database.mongo.MongoDBStore;
import lemongrenade.core.models.InvalidJobStateChangeException;
import lemongrenade.core.models.LGCommand;
import lemongrenade.core.models.LGJob;
import lemongrenade.core.models.LGPayload;
import lemongrenade.core.util.LGConstants;
import org.json.JSONObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.UUID;

public class CoordinatorCommandBoltTest extends TestCase {
    private final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * test Cancel Job
     */
    @Test
    public void testJobCancel() {
        JobManager jm = new JobManager();
        JSONObject jobConfig = new JSONObject();
        ArrayList<String> alist = new ArrayList();
        LGJob job = new LGJob(UUID.randomUUID().toString(), alist, jobConfig);
        try {
            job.setStatus(LGJob.STATUS_PROCESSING);
        }
        catch (InvalidJobStateChangeException e) {
            fail();
        }
        jm.addJob(job);
        assertTrue(jm.doesJobExist(job));
        assertTrue(jm.doesJobExist(job.getJobId()));
        log.info("Built test job "+job.toString());



        CoordinatorCommandBolt ccb = new CoordinatorCommandBolt();
        ccb.testSetup();
        LGPayload payload = new LGPayload();
        payload.setJobConfig(jobConfig);

        // Try to cancel finished job
        jm.setStatus(job, LGJob.STATUS_FINISHED);
        LGCommand command = new LGCommand(LGCommand.COMMAND_TYPE_STOP, job.getJobId(), 1,1, null, payload);
        ccb.handleStopCommand(command, null);
        // Check status
        LGJob job2 = jm.getJob(job.getJobId());
        log.info(job2.toString());
        // STATUS Should remain FINISHED!
        assertEquals(job2.getStatus(), LGJob.STATUS_FINISHED);

        // Try to cancel  PROCESSING
        jm.setStatus(job, LGJob.STATUS_PROCESSING);
        ccb.handleStopCommand(command, null);
        // Check status
        job2 = jm.getJob(job.getJobId());
        log.info(job2.toString());
        // STATUS Should remain FINISHED!
        assertEquals(job2.getStatus(), LGJob.STATUS_STOPPED);

        // Delete the job - clean up
        MongoDBStore.deleteJob(job2.getJobId());
        assertFalse(jm.doesJobExist(job2.getJobId()));
    }



    /**
     * Reset leaves the job in database so it can be "RERAN" later on. But the graph data itself
     * is removed from LemonGraph.
     *
     * When reset is given, you can also provide a "REASON" and search on that REASON later.
     */
    @Test
    public void testJobReset() {
        JobManager jm = new JobManager();
        JSONObject jobConfig = new JSONObject();

        String resetReason = "TEST_RESET1";
        jobConfig.put(LGConstants.LG_RESET_REASON,resetReason);
        ArrayList<String> alist = new ArrayList();
        LGJob job = new LGJob(UUID.randomUUID().toString(), alist, jobConfig);
        try {
            job.setStatus(LGJob.STATUS_PROCESSING);
        }
        catch (InvalidJobStateChangeException e) {
            fail();
        }
        jm.addJob(job);
        assertTrue(jm.doesJobExist(job));
        assertTrue(jm.doesJobExist(job.getJobId()));
        log.info("Built test job "+job.toString());

        log.info("Resetting job with reason - should fail becasuse it's PROCESSING state");
        CoordinatorCommandBolt ccb = new CoordinatorCommandBolt();
        ccb.testSetup();
        LGPayload payload = new LGPayload();
        payload.setJobConfig(jobConfig);
        LGCommand command = new LGCommand(LGCommand.COMMAND_TYPE_RESET, job.getJobId(), 1,1, null, payload);
        boolean result = ccb.handleReset(command, null);
        assertFalse(result);

        // Set Job to FINISHED
        jm.setJobFinished(job);

        // Resend reset, should succeed now because job is FINISHED
        result = ccb.handleReset(command, null);
        assertTrue(result);

        // Check status
        LGJob job2 = jm.getJob(job.getJobId());
        assertEquals(job2.getStatus(),LGJob.STATUS_RESET);

        // Check reason
        assertEquals(job2.getReason(),resetReason);

        // Make sure the graph is gone from Lemongraph
        LemonGraph lgGraph = new LemonGraph();
        boolean caughtException = false;
        try {
            JSONObject graph = lgGraph.getGraph(job.getJobId());
        }
        catch (InvalidGraphException e) {
            caughtException = true;
        }
        assertTrue(caughtException);

        // Delete the job - clean up
        MongoDBStore.deleteJob(job.getJobId());
        assertFalse(jm.doesJobExist(job.getJobId()));
    }
}

package lemongrenade.core.coordinator;
import lemongrenade.core.models.LGJob;
import org.junit.Test;

import java.util.HashMap;

public class CoordinatorHeartbeatBoltTest {
    public static final String TEST_ID = "00000000-0000-0000-0000-000000000000";

    @Test public void testResetExpire() {
        CoordinatorHeartbeatBolt test = new CoordinatorHeartbeatBolt();
        test.prepare(new HashMap(), null);
        boolean reset = test.checkForResetExpire();
        assert reset == false;
        LGJob job = new LGJob(TEST_ID);
        job.setExpireDate(10);
        JobManager.addJob(job);//add an expired job to get removed
        test.setLastReset(System.currentTimeMillis()-LGJob.DAY);//set date to >24 hours ago. Time for a reset/expire
        reset = test.checkForResetExpire();
        assert reset == true;
    }

}

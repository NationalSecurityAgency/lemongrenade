package lemongrenade.core.database.mongo;

import org.junit.Test;
import java.util.Set;

public class MongoDBStoreTest {

    //Ensures that the act of pulling these fields doesn't cause an error. Doesn't ensure the DBs actually have relevant jobs.
    @Test public void getExpiredJobsTest() {
        Set<String> jobIds = MongoDBStore.getExpiredJobs();
        int expiredJobs = jobIds.size();
        assert expiredJobs >= 0;
        jobIds = MongoDBStore.getResetJobs();
        int resetJobs = jobIds.size();
        assert resetJobs >= 0;
    }

}

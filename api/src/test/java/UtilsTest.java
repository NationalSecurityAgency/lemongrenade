import lemongrenade.api.services.Utils;
import lemongrenade.core.database.lemongraph.InvalidGraphException;
import lemongrenade.core.database.lemongraph.LemonGraph;
import lemongrenade.core.database.mongo.MongoDBStore;
import lemongrenade.core.models.LGJob;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class UtilsTest {
    private static final Logger log = LoggerFactory.getLogger(UtilsTest.class);
    public static final String TEST_ID = "00000000-0000-0000-0000-000000000000";

    @Test public void multiDeleteTest() throws Exception {
        String id0 = Utils.seedJob();
        String id1 = Utils.seedJob();
        String id2 = Utils.seedJob();
        try {
            JSONArray doArray = new JSONArray()
                    .put("delete")
                    ;
            JSONArray jobIDs = new JSONArray()
                    .put(id0)
                    .put(id1)
                    .put(id2);
            JSONObject params = new JSONObject()
                    .put("do", doArray)
                    .put("ids", jobIDs);
            ArrayList<Document> temp = MongoDBStore.getJob(id0);
            assert temp.size() == 1;
            temp = MongoDBStore.getJob(id1);
            assert temp.size() == 1;
            temp = MongoDBStore.getJob(id2);
            assert temp.size() == 1;
            Utils.doDelete(params);
            temp = MongoDBStore.getJob(id0);
            assert temp.size() == 0;
            temp = MongoDBStore.getJob(id1);
            assert temp.size() == 0;
            temp = MongoDBStore.getJob(id2);
            assert temp.size() == 0;
        }
        catch(AssertionError | Exception e) {
            Utils.deleteHelper(id0);
            Utils.deleteHelper(id1);
            Utils.deleteHelper(id2);
            throw e;
        }
    }

}
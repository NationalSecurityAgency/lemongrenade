package lemongrenade.core.models;

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
import java.util.Map;
import java.util.UUID;

public class LGJobTest extends TestCase {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Test
    public void testInvalidJobConfig() {
        ArrayList<String> alist = new ArrayList<String>();
        alist.add("adapter1");
        alist.add("adapter2");
        JSONObject jobConfig = new JSONObject();
        jobConfig.put("bad.key","somevalue");

    }




}
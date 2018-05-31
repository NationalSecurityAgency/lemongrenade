package lemongrenade.core.LGAdapter;

import lemongrenade.core.templates.LGAdapter;
import org.apache.storm.generated.StormTopology;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class LGAdapterTest {
    private static transient final Logger log = LoggerFactory.getLogger(LGAdapterTest.class);

    @Test public void testInstantiation() {
        log.info("Testing LGAdapter instantiation. This confirms proper reading of LGAdapter.json and "+LGAdapter.DEFAULT_CERTS_FILENAME);
        String id = "00000000-0000-0000-0000-000000000000";

        LGAdapter adapter = new LGAdapter(id) {
            @Override public StormTopology getTopology() {return null;}
            @Override public String getAdapterName() {return "Test";}
            @Override public HashMap<String, String> getRequiredAttributes() {return null;}
            @Override public String getAdapterQuery() {return null;}
        };

        int executors = adapter.getParallelismHint();
        int tasks = adapter.getTaskCount();
        assert executors == 2;
        assert tasks == 3;
    }
}

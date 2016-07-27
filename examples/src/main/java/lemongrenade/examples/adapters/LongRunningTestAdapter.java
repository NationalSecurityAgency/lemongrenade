package lemongrenade.examples.adapters;

import lemongrenade.core.models.LGPayload;
import lemongrenade.core.templates.LGJavaAdapter;
import org.apache.storm.utils.Utils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.List;

/**
 * Used for testing job cancel code
 *
 * This adapter will call itself 5 times and sleep 30 seconds each iteration.
 *
 */
public class LongRunningTestAdapter extends LGJavaAdapter {
    private static int RUNTIMES  = 1000;
    private static int SLEEPTIME = 5000;  // milliseconds

    public LongRunningTestAdapter(String id) {
        super(id);
    }
    private static final Logger log = LoggerFactory.getLogger(LongRunningTestAdapter.class);
    private int count = 1;

    @Override
    public void process(LGPayload input, LGJavaAdapter.LGCallback callback) {
        List<JSONObject> requests = input.getRequestNodes();
        JSONObject request = null;
        count++;
        try {
            for(int i = 0; i < requests.size(); i++) {
                request = requests.get(i);
                int id = request.getInt("ID");
                if (count <= RUNTIMES) {
                    JSONObject newNode = new JSONObject();
                    newNode.put("type", "hello");
                    newNode.put("value", "world" + id);
                    newNode.put("hello", "world" + id);
                    input.addResponseNode(newNode);
                }
            }
            Utils.sleep(SLEEPTIME);
            //throw new Exception();
            callback.emit(input);
        }
        catch (Exception e) {
            e.printStackTrace();
            callback.fail(e);
        }
    }

    @Override
    public String getAdapterName() {
        return "LongRunningTestAdapter";
    }

    @Override
    public HashMap<String, String> getRequiredAttributes() {
        HashMap<String, String> temp = new HashMap<String, String>();
        temp.put("hello", ".*\\d+?$");
        return temp;
    }

    @Override
    public String getAdapterQuery() {
        return "n(hello~/world(\\d+)?$/i)";
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1 ) {
            System.out.println("ERROR: Missing adapter ID");
            System.exit(-1);
        }
        LongRunningTestAdapter adapter = new LongRunningTestAdapter(args[1]);
        adapter.submitTopology(args);
    }

}

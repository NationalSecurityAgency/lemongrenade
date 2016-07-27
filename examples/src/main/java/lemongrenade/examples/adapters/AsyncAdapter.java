package lemongrenade.examples.adapters;

import lemongrenade.core.models.LGPayload;
import lemongrenade.core.templates.LGJavaAdapter;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.List;

public class AsyncAdapter extends LGJavaAdapter {

    public AsyncAdapter(String id) {
        super(id);
    }
    private static final Logger log = LoggerFactory.getLogger(AsyncAdapter.class);

    @Override
    public void process(LGPayload input, LGCallback callback) {
        new Thread() {
            @Override
            public void run() {
                List<JSONObject> requests = input.getRequestNodes();
                JSONObject request = null;
                try {
                    sleep(500);
                    for (int i = 0; i < requests.size(); i++) {
                        request = requests.get(i);
                        request.put("async", "ftw");
                        input.addResponseNode(request);
                        input.addResponseEdge(request,
                                new JSONObject().put("type", "connection").put("value", "foo"),
                                new JSONObject().put("type", "lemongrenade-core-test").put("value", "ignore").put("async", true));
                    }
                    callback.emit(input);
                } catch (Exception e) {
                    e.printStackTrace();
                    callback.fail(e);
                }
            }
        }.start();
    }

    @Override
    public String getAdapterName() {
        return "Async";
    }

    @Override
    public HashMap<String, String> getRequiredAttributes() {
        HashMap<String, String> temp = new HashMap<String, String>();
        temp.put("hello", ".*");
        return temp;
    }

    @Override
    public String getAdapterQuery() {
        return "n(hello~/.*/i)";
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1 ) {
            System.out.println("ERROR: Missing adapter ID");
            System.exit(-1);
        }
        AsyncAdapter adapter = new AsyncAdapter(args[1]);
        adapter.submitTopology(args);
    }

}

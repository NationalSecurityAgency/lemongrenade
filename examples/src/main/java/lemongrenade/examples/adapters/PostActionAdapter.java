package lemongrenade.examples.adapters;
import lemongrenade.core.models.LGPayload;
import lemongrenade.core.templates.LGJavaAdapter;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class PostActionAdapter extends LGJavaAdapter {

    public PostActionAdapter(String id) {
        super(id);
    }

    private static final Logger log = LoggerFactory.getLogger(PlusBangAdapter.class);

    @Override
    public void process(LGPayload input, LGCallback callback) {
        List<JSONObject> requests = input.getRequestNodes();
        JSONObject request;


        JSONObject config = getJobConfig(input);
        log.info("POSTACTON ADAPTER CONFIG: "+config.toString());

        try {
            for (int i = 0; i < requests.size(); i++) {
                request = requests.get(i);
                request.put("hello_postaction", request.get("hello") + "!");
                input.addResponseNode(request);

                JSONObject edgeMeta = new JSONObject().put("type", "edge").put("value", "connects");
                JSONObject dstMeta = new JSONObject().put("type", "node1").put("value", UUID.randomUUID().toString());
                JSONObject dstMeta2 = new JSONObject().put("type", "node2").put("value", UUID.randomUUID().toString());

                // Add an edge
                input.addResponseEdge(request, edgeMeta, dstMeta);
                input.addResponseEdge(request, edgeMeta, dstMeta2);
            }
            callback.emit(input);
        } catch (Exception e) {
            e.printStackTrace();
            callback.fail(e);
        }
    }

    @Override
    public String getAdapterName() {
        return "PostAction";
    }

    @Override
    public String getAdapterQuery() {
        return "n(hello~/.*/i)";
    }


    @Override
    public HashMap<String, String> getRequiredAttributes() {
        HashMap<String, String> temp = new HashMap<String, String>();
        temp.put("hello", ".*");
        return temp;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("ERROR: Missing adapter ID");
            System.exit(-1);
        }
        PlusBangAdapter adapter = new PlusBangAdapter(args[1]);
        System.out.println("AdapterName:" + adapter.getAdapterName());
        System.out.println("Adapter Id :" + adapter.getAdapterId());
        adapter.submitTopology(args);
    }
}

package lemongrenade.examples.adapters;

import lemongrenade.core.models.LGPayload;
import lemongrenade.core.templates.LGJavaAdapter;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

/***/
public class MyBangAdapter extends LGJavaAdapter {

    public MyBangAdapter(String id) {
        super(id);
    }

private static final Logger log = LoggerFactory.getLogger(MyBangAdapter.class);

    @Override
    public void process(LGPayload input, LGCallback callback) {
        List<JSONObject> requests = input.getRequestNodes();
        JSONObject request = null;
        try {
            for(int i = 0; i < requests.size(); i++) {
                request = requests.get(i);
                request.put("hello_my_bang", request.get("type"));
                input.addResponseNode(request);

                JSONObject edgeMeta = new JSONObject().put("type", "edge").put("value", "connects");
                JSONObject dstMeta = new JSONObject().put("type", "node1").put("value", UUID.randomUUID().toString());
                JSONObject dstMeta2 = new JSONObject().put("type", "node2").put("value", UUID.randomUUID().toString());
                JSONObject dstMeta3 = new JSONObject().put("type", "node3").put("value", UUID.randomUUID().toString());
                JSONObject dstMeta4 = new JSONObject().put("type", "node4").put("value", UUID.randomUUID().toString());
                JSONObject dstMeta5 = new JSONObject().put("type", "node5").put("value", UUID.randomUUID().toString());
                JSONObject dstMeta6 = new JSONObject().put("type", "node6").put("value", UUID.randomUUID().toString());
                JSONObject dstMeta7 = new JSONObject().put("type", "node4").put("value", UUID.randomUUID().toString());
                JSONObject dstMeta8 = new JSONObject().put("type", "node5").put("value", UUID.randomUUID().toString());
                JSONObject dstMeta9 = new JSONObject().put("type", "node6").put("value", UUID.randomUUID().toString());
                // Add an edge
                input.addResponseEdge(request, edgeMeta, dstMeta);
                input.addResponseEdge(request, edgeMeta, dstMeta2);
                input.addResponseEdge(request, edgeMeta, dstMeta3);
                input.addResponseEdge(request, edgeMeta, dstMeta4);
                input.addResponseEdge(request, edgeMeta, dstMeta5);
                input.addResponseEdge(request, edgeMeta, dstMeta6);
                input.addResponseEdge(request, edgeMeta, dstMeta7);
                input.addResponseEdge(request, edgeMeta, dstMeta8);
                input.addResponseEdge(request, edgeMeta, dstMeta9);
            }
            log.info("MyBang Success!");
            callback.emit(input);
        }
        catch (Exception e) {
            e.printStackTrace();
            callback.fail(e);
        }
    }

    @Override
    public String getAdapterName() {
        return "MyBang";
    }

    @Override
    public String getAdapterQuery() {
        return "n(type~/.*/i,value~/.*/i,type!:number)";
    }


    @Override
    public HashMap<String, String> getRequiredAttributes() {
        HashMap<String, String> temp = new HashMap<String, String>();
        temp.put("status", ".*");
        return temp;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1 ) {
            System.out.println("ERROR: Missing adapter ID");
            System.exit(-1);
        }
        MyBangAdapter adapter = new MyBangAdapter(args[1]);
        System.out.println("AdapterName:"+adapter.getAdapterName());
        System.out.println("Adapter Id :"+adapter.getAdapterId());
        adapter.submitTopology(args);
    }
}
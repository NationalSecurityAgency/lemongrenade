package lemongrenade.examples.adapters;

import lemongrenade.core.models.LGPayload;
import lemongrenade.core.templates.LGJavaAdapter;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.List;

public class FailAdapter extends LGJavaAdapter {

    public FailAdapter(String id) {
        super(id);
    }

    private static final Logger log = LoggerFactory.getLogger(FailAdapter.class);

    @Override public void process(LGPayload input, LGJavaAdapter.LGCallback callback) {
        List<JSONObject> requests = input.getRequestNodes();
        JSONObject node;
        try { //We always fail
            for(int i = 0; i < requests.size(); i++) {
                node = requests.get(i);
                node.put("HelloWorldFail", "processed");
                input.addResponseNode(node);
            }
            Exception e = new Exception("Adapter Always fails");
            throw(e);
        }
        catch (Exception e) {
            e.printStackTrace();
            callback.fail(e);
        }
    }

    @Override
    public String getAdapterName() {
        return "Fail";
    }

    @Override
    public HashMap<String, String> getRequiredAttributes() {
        HashMap<String, String> temp = new HashMap<String, String>();
        temp.put("type", ".*");
        return temp;
    }

    @Override
    public String getAdapterQuery() { //Numbers cannot be regexd
        return "n(type~/.*/i,value~/.*/i)";//requires a string (any value), any "type", any "value", case insensitive
    }

    /** */
    public static void main(String[] args) throws Exception {
        if (args.length < 1 ) {
            System.out.println("ERROR: Missing adapter ID");
            System.exit(-1);
        }
        FailAdapter adapter = new FailAdapter(args[1]);
        adapter.submitTopology(args);
        adapter.close();
    }

}

package lemongrenade.examples.adapters;

import lemongrenade.core.models.LGPayload;
import lemongrenade.core.templates.LGJavaAdapter;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.List;

public class HelloWorldFailAdapter extends LGJavaAdapter {

    public HelloWorldFailAdapter(String id) {
        super(id);
    }
    private static final Logger log = LoggerFactory.getLogger(HelloWorldFailAdapter.class);

    @Override
    public void process(LGPayload input, LGJavaAdapter.LGCallback callback) {
        List<JSONObject> requests = input.getRequestNodes();
        JSONObject request = null;
        try {
            //We always fail
            Exception e = new Exception("Adapter Always fails");
            callback.fail(e);
        }
        catch (Exception e) {
            e.printStackTrace();
            callback.fail(e);
        }
    }

    @Override
    public String getAdapterName() {
        return "HelloWorldFail";
    }

    @Override
    public HashMap<String, String> getRequiredAttributes() {
        HashMap<String, String> temp = new HashMap<String, String>();
        temp.put("status", ".*");
        return temp;
    }

    @Override
    public String getAdapterQuery() { //Numbers cannot be regexd
        return "n(status~/.*/i,type~/.*/i,value~/.*/i,status!:number)";//requires a string (any value), any "type", any "value", case insensitive
    }

    /** */
    public static void main(String[] args) throws Exception {
        if (args.length < 1 ) {
            System.out.println("ERROR: Missing adapter ID");
            System.exit(-1);
        }
        HelloWorldFailAdapter adapter = new HelloWorldFailAdapter(args[1]);
        adapter.submitTopology(args);
    }

}

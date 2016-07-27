package lemongrenade.examples.adapters;

import lemongrenade.core.models.LGPayload;
import lemongrenade.core.templates.LGJavaAdapter;
import lemongrenade.core.util.RequestResult;
import lemongrenade.core.util.Requests;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.List;

public class HelloWorldExceptionAdapter extends LGJavaAdapter {


    public HelloWorldExceptionAdapter(String id) {
        super(id);
    }
    private static final Logger log = LoggerFactory.getLogger(HelloWorldExceptionAdapter.class);

    @Override
    public void process(LGPayload input, LGJavaAdapter.LGCallback callback) {
        List<JSONObject> requests = input.getRequestNodes();
        JSONObject request = null;
        Exception e = new Exception("heloworldEXCEPTION ");
        callback.fail(e);
    }

    @Override
    public String getAdapterName() {
        return "HelloWorldException";
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

    public static void test() {
        try {
            Requests.setCerts();
            RequestResult result = Requests.get("http://google.com");
            System.out.println(result.response_msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("All done.");
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1 ) {
            System.out.println("ERROR: Missing adapter ID");
            System.exit(-1);
        }
        HelloWorldAdapter adapter = new HelloWorldAdapter(args[1]);
        adapter.submitTopology(args);
//        test();
    }
}

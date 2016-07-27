package lemongrenade.examples.adapters;

import lemongrenade.core.models.LGPayload;
import lemongrenade.core.templates.LGJavaAdapter;
import lemongrenade.core.util.Requests;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.List;

public class CurlAdapter extends LGJavaAdapter {
    public CurlAdapter (String id) {
        super(id);
    }

    private static final Logger log = LoggerFactory.getLogger(CurlAdapter.class);

    @Override
    public void process(LGPayload input, LGCallback callback) {
        List<JSONObject> requests = input.getRequestNodes();
        JSONObject request = null;
        try {
            String result = Requests.get("http://www.google.com").response_msg;
            final String alphaNumeric = result.replaceAll("[^a-zA-Z0-9]+", "").substring(0, 100);
            for(int i = 0; i < requests.size(); i++) {
                request = requests.get(i);
                request.put("curl-google", alphaNumeric);
                input.addResponseNode(request);
            }
            callback.emit(input);
        }
        catch (Exception e) {
            e.printStackTrace();
            callback.fail(e);
        }
    }

    @Override
    public String getAdapterName() {
        return "Curl";
    }

    @Override
    public HashMap<String, String> getRequiredAttributes() {
        HashMap<String, String> temp = new HashMap<String, String>();
        temp.put("status", "curl");
        return temp;
    }

    @Override
    public String getAdapterQuery() {
        return "n(status~/curl/i)";
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1 ) {
            System.out.println("ERROR: Missing adapter ID");
            System.exit(-1);
        }
        CurlAdapter adapter = new CurlAdapter(args[1]);
        adapter.submitTopology(args);

    }

}
